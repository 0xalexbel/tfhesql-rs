use crate::uint::mask::BoolMask;
use crate::bitops::*;
use crate::types::*;
use crate::OrderedTables;
use crate::Table;
use rayon::iter::*;

pub(super) fn compute_select_distinct<B>(
    select_mask: &mut BoolMask<B>,
    distinct: &B,
    tables: &OrderedTables,
    table_mask: &BoolMask<B>,
    not_field_mask: &BoolMask<B>,
) where
    B: ThreadSafeUInt + ThreadSafeBool,
{
    if select_mask.mask.len() <= 1 {
        return;
    }

    // Start at 1 since line 0 is invariant
    // Iterative
    (1..select_mask.mask.len()).for_each(|row_index_i| {
        // Parallel
        par_compute_select_distinct_row_i(
            select_mask,
            row_index_i,
            distinct,
            tables,
            table_mask,
            not_field_mask,
        )
    });

    par_unselect_out_of_bounds(select_mask, tables, table_mask);
}

/// Unselect every out of bounds lines
/// Try to minimize the number of boolean ops
/// Maximum number of tables = 64
fn par_unselect_out_of_bounds<B>(
    select_mask: &mut BoolMask<B>,
    tables: &OrderedTables,
    table_mask: &BoolMask<B>,
) where
    B: ThreadSafeUInt + ThreadSafeBool + DebugToString,
{
    assert!(!table_mask.is_empty());
    assert!(table_mask.len() <= 64);
    let min_rows = tables.min_num_rows();
    let max_rows = tables.max_num_rows();

    let mut prev_rows = min_rows;
    let mut prev = tables.encode_predicate_rows_gteq(min_rows);
    assert_eq!(prev, table_mask.max_u64());

    let mut vanished = B::get_false();
    let mut not_vanished = B::get_true();

    ((min_rows + 1)..=max_rows).for_each(|num_rows| {
        let cur = tables.encode_predicate_rows_gteq(num_rows);
        if prev != cur {
            let indices_of_new_vanished_tables = tables.rows_between(prev_rows, num_rows);
            assert!(!indices_of_new_vanished_tables.is_empty());
            let mut new_vanished_mask = table_mask.extract(&indices_of_new_vanished_tables);
            assert_eq!(
                new_vanished_mask.len(),
                indices_of_new_vanished_tables.len()
            );
            new_vanished_mask.push(&vanished);
            vanished = par_bitor_vec_ref(new_vanished_mask).unwrap();
            not_vanished = vanished.ref_not();
            prev = cur;
            prev_rows = num_rows;
        }
        select_mask.mask[num_rows - 1] =
            select_mask.mask[num_rows - 1].refref_bitand(&not_vanished);
    });
}

fn par_compute_select_distinct_row_i<B>(
    select_mask: &mut BoolMask<B>,
    row_index_i: usize,
    distinct: &B,
    tables: &OrderedTables,
    table_mask: &BoolMask<B>,
    not_field_mask: &BoolMask<B>,
) where
    B: ThreadSafeUInt + ThreadSafeBool,
{
    assert!(select_mask.len() == tables.max_num_rows());

    // [j < i; LineVisible(j) AND EqualLine(i, j)]
    let select_j_and_eq_line_i_j: Vec<B> = select_mask
        .mask
        .par_iter()
        .take(row_index_i)
        .enumerate()
        .map(|(row_index_j, select_j)| {
            tables_row_i_eq_row_j(
                row_index_i,
                row_index_j,
                tables,
                table_mask,
                not_field_mask,
            ).map(|eq_line_i_j| select_j.refref_bitand(&eq_line_i_j))
        })
        .filter(|x| x.is_some())
        .map(|x| x.unwrap())
        .collect();

    if select_j_and_eq_line_i_j.is_empty() {
        return;
    }

    // - par_bitor_vec :
    //   OR [j < i; Select(j) AND EqualLine(i, j)]
    //
    // - refref_bitand(distinct) :
    //   OR [j < i; Select(j) AND EqualLine(i, j)] AND distinct
    //
    // - ref_not() :
    //   !{ OR [j < i; Select(j) AND EqualLine(i, j)] AND distinct }
    //
    // - refref_bitand(&select_mask.mask[row_index_i]) :
    //   Select(i) AND !{ OR [j < i; Select(j) AND EqualLine(i, j)] AND distinct }
    //
    let new_select_i = par_bitor_vec(select_j_and_eq_line_i_j)
        .unwrap()
        .refref_bitand(distinct)
        .ref_not()
        .refref_bitand(&select_mask.mask[row_index_i]);

    select_mask.mask[row_index_i] = new_select_i;
}

/// Cost:
/// -----
/// - 1 x Bit And x (N(NotEq Cells) + 1)
/// - zero if All cells are equal or different
///
/// Formula:
/// --------
/// - Line(t, i) == Line(t, j) <=> AND { 0 <= k < N(Columns) so that Value(t,i,k) != Value(t,j,k); !Visible(k) }
///
/// Return:
/// -------
/// - IsTable(t) AND Line(t, i) == Line(t, j)
fn table_row_i_eq_row_j<B>(
    row_index_i: usize,
    row_index_j: usize,
    table: &Table,
    table_index_mask: &B,
    not_field_mask: &BoolMask<B>,
) -> Option<B>
where
    B: ThreadSafeUInt + ThreadSafeBool,
{
    assert!(table.num_columns() <= not_field_mask.len());
    let mut cell_eq_buffer = vec![false; table.num_columns()];

    let num_eq =
        table.compute_line_equality_vector(row_index_i, row_index_j, false, &mut cell_eq_buffer);
    // all cells are different
    if num_eq == 0 {
        return None;
    }
    // all cells are equal, return IsTable(t)
    if num_eq == table.num_columns() {
        return Some(table_index_mask.clone());
    }

    // Keep refs since B can be larger than a bool (FheBool)
    let mut v: Vec<&B> = vec![table_index_mask];
    cell_eq_buffer
        .iter()
        .enumerate()
        .for_each(|(column_index, cell_i_eq_cell_j)| {
            if !*cell_i_eq_cell_j {
                v.push(&not_field_mask.mask[column_index])
            }
        });

    par_bitand_vec_ref(v)
}

/// Cost:
/// -----
/// - 1 x Bit And x N(Tables) x (N(NotEq Cells)) + 1)
///
/// Formula:
/// --------
/// - EqualLine(i, j) <=> OR { t; IsTable(t) AND Line(t, i) == Line(t, j) }
fn tables_row_i_eq_row_j<B>(
    row_index_i: usize,
    row_index_j: usize,
    tables: &OrderedTables,
    table_mask: &BoolMask<B>,
    not_field_mask: &BoolMask<B>,
) -> Option<B>
where
    B: ThreadSafeUInt + ThreadSafeBool,
{
    let a: Vec<B> = tables
        .tables()
        .par_iter()
        .zip(table_mask.mask.par_iter())
        .map(|(table, table_index_mask)| {
            table_row_i_eq_row_j(
                row_index_i,
                row_index_j,
                table,
                table_index_mask,
                not_field_mask,
            )
        })
        .filter(|x| x.is_some())
        .map(|x| x.unwrap())
        .collect();

    par_bitor_vec(a)
}
