use crate::bitops::*;
use crate::query::sql_query::SqlQueryRef;
use crate::sql_ast::ComparatorMask;
use crate::table::{cmp::table_row_col_cmp_col, OrderedTables, Table};
use crate::types::*;
use crate::uint::mask::BoolMask;
use crate::uint::mask::SizedMask;
use crate::uint::triangular_matrix::TriangularMatrix;
#[cfg(feature = "parallel")]
use rayon::iter::*;
use std::collections::HashMap;

////////////////////////////////////////////////////////////////////////////////
// IdentOpIdent
////////////////////////////////////////////////////////////////////////////////

pub struct IdentOpIdent<B> {
    binary_op_index: usize,
    query_ref: SqlQueryRef<B>,
    /// Ident(i) AND Ident(j) AND Operator(k)
    matrix: TriangularMatrix<ComparatorMask<B>>,
    select_mask: BoolMask<B>,
}

////////////////////////////////////////////////////////////////////////////////

impl<B> IdentOpIdent<B> {
    pub fn new_empty(binary_op_index: usize, query_ref: SqlQueryRef<B>) -> Self {
        IdentOpIdent {
            binary_op_index,
            query_ref,
            matrix: TriangularMatrix::<ComparatorMask<B>>::new_empty(),
            select_mask: BoolMask::<B>::new_empty(),
        }
    }

    pub fn table_mask(&self) -> &BoolMask<B> {
        &self.query_ref.header().table_mask
    }

    pub fn select_mask(&self) -> &BoolMask<B> {
        &self.select_mask
    }
}

impl<B> IdentOpIdent<B>
where
    B: ThreadSafeBool,
{
    fn par_compute_matrix(&mut self, tables: &OrderedTables) {
        let sql_binary_op = self.query_ref.binary_op_at(self.binary_op_index);

        let left_ident = &sql_binary_op.left_ident_mask;
        let right_ident = &sql_binary_op.right.ident_mask;
        let op_mask = &sql_binary_op.comparator_mask;

        assert_eq!(left_ident.len(), right_ident.len());
        assert_eq!(left_ident.len(), tables.ordered_schemas().max_num_fields());

        // Parallel + Costly
        // Computes a triangular matrix:
        // Tr(i,j) = Left(i) & Right(j) where i <= j < max_num_columns
        let left_right_ident_matrix = left_ident.triangular_matrix(right_ident);

        // Parallel + Costly
        // Tr(i,j) = [k; Left(i) & Right(j) & Op(k)]
        self.matrix = op_mask.triangular_matrix_and(&left_right_ident_matrix);
        assert_eq!(self.matrix.dim(), tables.ordered_schemas().max_num_fields());
    }

    pub fn compute(&mut self, tables: &OrderedTables) {
        self.par_compute_matrix(tables);
        // Costly
        self.select_mask = IdentOpIdentCacheBuilder::<B>::build(tables, self);
    }

    #[inline]
    pub fn _field_i_and_field_j_and_op_k(&self, i: usize, j: usize, k: usize) -> &B {
        assert!(i < self.matrix.dim());
        assert!(j < self.matrix.dim());
        assert!(k < ComparatorMask::<B>::LEN);
        &self.matrix.get(i, j).mask().mask[k]
    }
}

////////////////////////////////////////////////////////////////////////////////
// IdentOpIdentCacheBuilder
////////////////////////////////////////////////////////////////////////////////

struct IdentOpIdentCacheBuilder<B> {
    cache_indices: HashMap<Vec<u8>, usize>,
    cache_eq_gt_lt: Vec<TriangularMatrix<[bool; 3]>>,
    cache_values: Vec<B>,
    table_row_index_to_cache_index: Vec<Vec<usize>>,
}

////////////////////////////////////////////////////////////////////////////////

impl<B> IdentOpIdentCacheBuilder<B>
where
    B: ThreadSafeBool,
{
    fn pre_build(tables: &OrderedTables) -> Self {
        // New empty cache
        let mut builder = IdentOpIdentCacheBuilder::<B> {
            cache_indices: HashMap::<Vec<u8>, usize>::new(),
            cache_eq_gt_lt: vec![],
            cache_values: vec![],
            table_row_index_to_cache_index: vec![],
        };
        // 2 pass computation
        // ==================
        // - first pass: serial, build the list of all the requested EQ,GT,LT boolean triplets
        // - second pass: parallel, compute the corresponsing B value associated to each EQ,GT,LT boolean triplets
        // Serial prepare
        tables
            .tables()
            .iter()
            .enumerate()
            .for_each(|(table_index, table)| {
                builder.pre_insert_table(table, table_index);
            });
        builder
    }

    pub fn build(tables: &OrderedTables, ident_op_ident: &IdentOpIdent<B>) -> BoolMask<B> {
        let mut builder = Self::pre_build(tables);
        builder.compute(tables, ident_op_ident)
    }

    fn compute(&mut self, tables: &OrderedTables, ident_op_ident: &IdentOpIdent<B>) -> BoolMask<B> {
        self.compute_values(ident_op_ident);
        self.compute_select(tables, ident_op_ident)
    }

    #[cfg(feature = "parallel")]
    fn compute_values(&mut self, ident_op_ident: &IdentOpIdent<B>) {
        self.cache_values
            .par_iter_mut()
            .zip(self.cache_eq_gt_lt.par_iter())
            .for_each(|(dst, values_i_j_op)| {
                // Parallel
                let v = ident_op_ident
                    .matrix
                    .par_filter_map_with(values_i_j_op, |field_i_j_op, eq_gt_lt_i_j| {
                        field_i_j_op.or_and_value3(eq_gt_lt_i_j)
                    });
                // Parallel
                *dst = par_bitor_vec::<B>(v).unwrap();
            });
    }

    #[cfg(not(feature = "parallel"))]
    fn compute_values(&mut self, ident_op_ident: &IdentOpIdent<B>) {
        self.cache_values
            .iter_mut()
            .zip(self.cache_eq_gt_lt.iter())
            .for_each(|(dst, values_i_j_op)| {
                // Parallel
                let v = ident_op_ident
                    .matrix
                    .filter_map_with(values_i_j_op, |field_i_j_op, eq_gt_lt_i_j| {
                        field_i_j_op.or_and_value3(eq_gt_lt_i_j)
                    });
                // Parallel
                *dst = par_bitor_vec::<B>(v).unwrap();
            });
    }

    #[cfg(feature = "parallel")]
    // Compute final select, iterate over rows and apply table mask
    fn compute_select(
        &self,
        tables: &OrderedTables,
        ident_op_ident: &IdentOpIdent<B>,
    ) -> BoolMask<B> {
        let table_mask = ident_op_ident.table_mask();
        let mut select_mask = BoolMask::<B>::all_false(tables.max_num_rows());
        select_mask
            .mask
            .par_iter_mut()
            .enumerate()
            .for_each(|(row_index, dst)| *dst = self.compute_tables_row(table_mask, row_index));
        select_mask
    }

    #[cfg(not(feature = "parallel"))]
    // Compute final select, iterate over rows and apply table mask
    fn compute_select(
        &self,
        tables: &OrderedTables,
        ident_op_ident: &IdentOpIdent<B>,
    ) -> BoolMask<B> {
        let table_mask = ident_op_ident.table_mask();
        let mut select_mask = BoolMask::<B>::all_false(tables.max_num_rows());
        select_mask
            .mask
            .iter_mut()
            .enumerate()
            .for_each(|(row_index, dst)| *dst = self.compute_tables_row(table_mask, row_index));
        select_mask
    }

    #[cfg(feature = "parallel")]
    // TableMask AND { OR 0 <= t < num_tables; TableRow(t)}
    fn compute_tables_row(&self, table_mask: &BoolMask<B>, row_index: usize) -> B {
        let buffer = self
            .table_row_index_to_cache_index
            .par_iter()
            .enumerate()
            .filter(|(_, row_index_to_cache_value_index)| {
                row_index < row_index_to_cache_value_index.len()
            })
            .map(|(table_index, row_index_to_cache_value_index)| {
                let i = row_index_to_cache_value_index[row_index];
                self.cache_values[i].refref_bitand(table_mask.get(table_index))
            })
            .collect::<Vec<B>>();

        if buffer.is_empty() {
            B::get_false()
        } else {
            par_bitor_vec(buffer).unwrap()
        }
    }

    #[cfg(not(feature = "parallel"))]
    fn compute_tables_row(&self, table_mask: &BoolMask<B>, row_index: usize) -> B {
        let buffer = self
            .table_row_index_to_cache_index
            .iter()
            .enumerate()
            .filter(|(_, row_index_to_cache_value_index)| {
                row_index < row_index_to_cache_value_index.len()
            })
            .map(|(table_index, row_index_to_cache_value_index)| {
                let i = row_index_to_cache_value_index[row_index];
                self.cache_values[i].refref_bitand(table_mask.get(table_index))
            })
            .collect::<Vec<B>>();

        if buffer.len() == 0 {
            B::get_false()
        } else {
            par_bitor_vec(buffer).unwrap()
        }
    }

    // Serial
    fn pre_insert_table(&mut self, table: &Table, table_index: usize) {
        assert!(table_index == self.table_row_index_to_cache_index.len());
        self.table_row_index_to_cache_index.push(vec![]);
        let n = table.batch().num_rows();
        for i in 0..n {
            self.pre_insert_row(table, table_index, i);
        }
    }

    // Serial: we are manipulating clear data on a single row
    fn pre_insert_row(&mut self, table: &Table, table_index: usize, row_index: usize) -> usize {
        assert!(row_index == self.table_row_index_to_cache_index[table_index].len());

        // col_cmp_col_matrix is a triangular matrix :
        // (Col(0),Col(0)) (Col(0),Col(1)) (Col(0),Col(2)) ...   (Col(0),Col(p-1))
        //                 (Col(1),Col(1)) (Col(1),Col(2)) ...   (Col(1),Col(p-1))
        //                                                 ...
        //                                 (Col(p-2),Col(p-2)) (Col(p-2),Col(p-1))
        //                                                     (Col(p-1),Col(p-1))
        // Each element is a [bool;3] value:
        // value[i,j][0 = EQ] == Col(i) = Col(j)
        // value[i,j][1 = GT] == Col(i) > Col(j)
        // value[i,j][2 = LT] == Col(i) < Col(j)
        let col_cmp_col_matrix = table_row_col_cmp_col(table, row_index);
        assert_eq!(col_cmp_col_matrix.dim(), table.num_columns());
        // Build the corresponding key value
        let key: Vec<u8> = Vec::<u8>::from(&col_cmp_col_matrix);

        if let Some(existing_value_index) = self.cache_indices.get(&key) {
            self.table_row_index_to_cache_index[table_index].push(*existing_value_index);
            assert_eq!(
                self.table_row_index_to_cache_index[table_index][row_index],
                *existing_value_index
            );

            return *existing_value_index;
        }

        let next_value_index = self.cache_values.len();

        assert_eq!(self.cache_eq_gt_lt.len(), self.cache_values.len());
        self.cache_eq_gt_lt.push(col_cmp_col_matrix);
        self.cache_values.push(B::get_false());

        self.cache_indices.insert(key, next_value_index);
        self.table_row_index_to_cache_index[table_index].push(next_value_index);
        assert_eq!(
            self.table_row_index_to_cache_index[table_index][row_index],
            next_value_index
        );

        next_value_index
    }
}
