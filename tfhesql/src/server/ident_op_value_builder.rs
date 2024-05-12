use super::ident_op_value::IdentOpValue;
use crate::default_into::DefaultInto;
use crate::sql_ast::ComparatorMask;
use crate::table::eq_gt_lt_type_cache::EqGtLtTypedTableValueCache;
use crate::uint::mask::BoolMask;
use crate::bitops::*;
use crate::types::*;
use crate::OrderedTables;
use crate::Table;

#[cfg(feature = "parallel")]
use rayon::iter::*;

////////////////////////////////////////////////////////////////////////////////
// IdentOpValueCacheBuilder
////////////////////////////////////////////////////////////////////////////////

pub(super) struct IdentOpValueCacheBuilder<B> {
    eq_gt_lt_table_cache: EqGtLtTypedTableValueCache<B>,
}

////////////////////////////////////////////////////////////////////////////////

impl<B> Default for IdentOpValueCacheBuilder<B>
where
    B: BooleanType,
{
    fn default() -> Self {
        Self {
            eq_gt_lt_table_cache: Default::default(),
        }
    }
}

impl<B> IdentOpValueCacheBuilder<B>
where
    B: ThreadSafeBool + DefaultInto<B>,
{
    fn pre_build(tables: &OrderedTables) -> Self {
        let mut builder = IdentOpValueCacheBuilder::<B>::default();
        builder
            .eq_gt_lt_table_cache
            .default_into_from_ordered_tables(tables);
        builder
    }

    pub fn build(tables: &OrderedTables, ident_op_value: &IdentOpValue<B>) -> BoolMask<B> {
        let mut builder = Self::pre_build(tables);
        builder.compute(tables, ident_op_value)
    }

    fn compute(&mut self, tables: &OrderedTables, ident_op_value: &IdentOpValue<B>) -> BoolMask<B> {
        // Compute all EQ, GT, LT bools for all the cached values
        self.eq_gt_lt_table_cache.fill_from_eq_gt(
            ident_op_value.right_strictly_negative(),
            ident_op_value.eq_gt_cache(),
            ident_op_value.ascii_cache(),
        );
        // Apply Column Mask to every cached value
        self.eq_gt_lt_table_cache
            .bitand_col_value(ident_op_value.left_ident_mask());

        self.compute_select(tables, ident_op_value)
    }

    #[cfg(feature = "parallel")]
    fn compute_select(
        &self,
        tables: &OrderedTables,
        ident_op_value: &IdentOpValue<B>,
    ) -> BoolMask<B> {
        let table_mask = ident_op_value.table_mask();
        let right_is_value = ident_op_value.right_is_value();
        let comparator_mask = ident_op_value.comparator_mask();
        let mut select_mask = BoolMask::<B>::all_false(tables.max_num_rows());
        select_mask
            .mask
            .par_iter_mut()
            .enumerate()
            .for_each(|(row_index, dst)| {
                // Apply the 'is_value' flag
                // is_value = false is the right operand is column name
                *dst = self
                    .compute_tables_row(tables, table_mask, row_index, comparator_mask)
                    .refref_bitand(right_is_value);
            });
        select_mask
    }

    #[cfg(not(feature = "parallel"))]
    fn compute_select(
        &self,
        tables: &OrderedTables,
        ident_op_value: &IdentOpValue<B>,
    ) -> BoolMask<B> {
        let table_mask = ident_op_value.table_mask();
        let right_is_value = ident_op_value.right_is_value();
        let comparator_mask = ident_op_value.comparator_mask();
        let mut select_mask = BoolMask::<B>::all_false(tables.max_num_rows());
        select_mask
            .mask
            .iter_mut()
            .enumerate()
            .for_each(|(row_index, dst)| {
                // Apply the 'is_value' flag
                // is_value = false is the right operand is column name
                *dst = self
                    .compute_tables_row(tables, table_mask, row_index, comparator_mask)
                    .refref_bitand(right_is_value);
            });
        select_mask
    }

    #[cfg(feature = "parallel")]
    fn compute_tables_row(
        &self,
        tables: &OrderedTables,
        table_mask: &BoolMask<B>,
        row_index: usize,
        comparator_mask: &ComparatorMask<B>,
    ) -> B {
        let buffer = tables
            .tables()
            .par_iter()
            .enumerate()
            .filter(|(_, table)| row_index < table.num_rows())
            .map(|(table_index, table)| {
                self.compute_table_row(
                    table,
                    table_mask.get(table_index),
                    row_index,
                    comparator_mask,
                )
            })
            .collect::<Vec<B>>();

        if buffer.is_empty() {
            B::get_false()
        } else {
            par_bitor_vec(buffer).unwrap()
        }
    }

    #[cfg(not(feature = "parallel"))]
    fn compute_tables_row(
        &self,
        tables: &OrderedTables,
        table_mask: &BoolMask<B>,
        row_index: usize,
        comparator_mask: &ComparatorMask<B>,
    ) -> B {
        let buffer = tables
            .tables()
            .iter()
            .enumerate()
            .filter(|(_, table)| row_index < table.num_rows())
            .map(|(table_index, table)| {
                self.compute_table_row(
                    table,
                    table_mask.get(table_index),
                    row_index,
                    comparator_mask,
                )
            })
            .collect::<Vec<B>>();

        if buffer.len() == 0 {
            B::get_false()
        } else {
            par_bitor_vec(buffer).unwrap()
        }
    }

    // Always parallel
    fn compute_table_row(
        &self,
        table: &Table,
        table_mask: &B,
        row_index: usize,
        comparator_mask: &ComparatorMask<B>,
    ) -> B {
        if table.num_rows() <= row_index {
            return B::get_false();
        }

        let or_col_and_eq_gt_lt = self
            .eq_gt_lt_table_cache
            .par_or_row(table, row_index);

        // Apply comparator mask + Invert the result
        // Apply table mask at the very end!
        comparator_mask.or_and_eq_gt_lt(
            &or_col_and_eq_gt_lt.eq,
            &or_col_and_eq_gt_lt.gt,
            &or_col_and_eq_gt_lt.lt,
        ).refref_bitand(table_mask)
    }
}
