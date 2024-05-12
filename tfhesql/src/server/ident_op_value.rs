use crate::hi_lo_tree::EqNe;
use crate::hi_lo_tree::U64EqGtTree;
use crate::sql_ast::ComparatorMask;
use crate::uint::iter::*;
use crate::uint::mask::BoolMask;
use crate::{
    default_into::DefaultInto,
    query::sql_query::SqlQueryRef,
    table::{ascii_cache::AsciiCache, OrderedTables},
};
use crate::types::*;
use super::IdentOpValueCacheBuilder;

#[cfg(all(feature = "stats", not(feature = "parallel")))]
use crate::stats::PerfStats;

////////////////////////////////////////////////////////////////////////////////
// IdentOpValue
////////////////////////////////////////////////////////////////////////////////

pub(super) struct IdentOpValue<B> {
    binary_op_index: usize,
    query_ref: SqlQueryRef<B>,
    eq_gt_cache: U64EqGtTree<B>,
    select_mask: BoolMask<B>,
    ascii_cache: AsciiCache<B>,
}

////////////////////////////////////////////////////////////////////////////////

impl<B> IdentOpValue<B> {
    pub fn eq_gt_cache(&self) -> &U64EqGtTree<B> {
        &self.eq_gt_cache
    }

    pub fn ascii_cache(&self) -> &AsciiCache<B> {
        &self.ascii_cache
    }

    pub fn select_mask(&self) -> &BoolMask<B> {
        &self.select_mask
    }

    pub fn comparator_mask(&self) -> &ComparatorMask<B> {
        &self
            .query_ref
            .binary_op_at(self.binary_op_index)
            .comparator_mask
    }

    pub fn left_ident_mask(&self) -> &BoolMask<B> {
        &self
            .query_ref
            .binary_op_at(self.binary_op_index)
            .left_ident_mask
    }

    pub fn table_mask(&self) -> &BoolMask<B> {
        &self.query_ref.header().table_mask
    }

    pub fn right_strictly_negative(&self) -> &EqNe<B> {
        &self
            .query_ref
            .binary_op_at(self.binary_op_index)
            .right
            .is_strictly_negative
    }

    pub fn right_is_value(&self) -> &B {
        &self
            .query_ref
            .binary_op_at(self.binary_op_index)
            .right
            .is_value
    }
}

impl<B> IdentOpValue<B>
where
    B: Clone,
{
    pub fn new_empty(binary_op_index: usize, query_ref: SqlQueryRef<B>) -> Self {
        let a = query_ref.binary_op_at(binary_op_index);

        IdentOpValue {
            binary_op_index,
            query_ref: query_ref.clone(),
            eq_gt_cache: U64EqGtTree::<B>::new(a.right.bytes_256.word_0_eq_gt.clone()),
            select_mask: BoolMask::<B>::new_empty(),
            ascii_cache: AsciiCache::<B>::new(&a.right.bytes_256),
        }
    }
}

impl<B> IdentOpValue<B>
where
    B: ThreadSafeBool + DefaultInto<B>,
{
    fn fill(&mut self, tables: &OrderedTables, chunck_size: usize) {
        #[cfg(all(feature = "stats", not(feature = "parallel")))]
        let mut stats = PerfStats::new("Fill Number");

        self.eq_gt_cache
            .fill_with_iter(tables.iter_le_u16(), chunck_size)
            .fill16();
        self.eq_gt_cache
            .fill_with_iter(tables.iter_le_u32(), chunck_size)
            .fill32();
        self.eq_gt_cache
            .fill_with_iter(tables.iter_le_u64(), chunck_size)
            .fill64();

        #[cfg(all(feature = "stats", not(feature = "parallel")))]
        {
            stats.close();
            stats.print();
        }

        #[cfg(all(feature = "stats", not(feature = "parallel")))]
        let mut stats = PerfStats::new("Fill Ascii");

        // Use u64_tree pre-computed values
        self.ascii_cache
            .fill(tables, chunck_size, Some(&self.eq_gt_cache));

        #[cfg(all(feature = "stats", not(feature = "parallel")))]
        {
            stats.close();
            stats.print();
        }
    }

    pub fn compute(&mut self, tables: &OrderedTables, chunck_size: usize) {
        self.fill(tables, chunck_size);
        // Costly
        let select_mask = IdentOpValueCacheBuilder::<B>::build(tables, self);
        self.select_mask = select_mask;
    }
}
