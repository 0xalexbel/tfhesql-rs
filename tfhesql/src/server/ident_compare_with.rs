use super::ident_op_ident::IdentOpIdent;
use super::ident_op_value::IdentOpValue;
use crate::default_into::{DefaultInto, ValueFrom};
use crate::query::optional_bool_tree::OptionalBool;
use crate::query::sql_query::SqlQueryRef;
use crate::query::sql_query_tree::SqlQueryTree;
use crate::uint::mask::BoolMask;
use crate::bitops::*;
use crate::types::*;
use crate::OrderedTables;

#[cfg(feature = "parallel")]
use rayon::iter::*;

////////////////////////////////////////////////////////////////////////////////
// IdentCompareWith
////////////////////////////////////////////////////////////////////////////////

struct IdentCompareWith<B> {
    ident: IdentOpIdent<B>,
    value: IdentOpValue<B>,
    select_mask: BoolMask<B>,
}

impl<B> IdentCompareWith<B> {
    pub fn select_mask(&self) -> &BoolMask<B> {
        &self.select_mask
    }
}

impl<B> IdentCompareWith<B>
where
    B: ThreadSafeBool + DefaultInto<B> + ValueFrom<B>,
{
    pub fn compute_select(&mut self) {
        assert_eq!(
            self.ident.select_mask().len(),
            self.value.select_mask().len()
        );
        self.select_mask = RefBitOr::<BoolMask<B>>::refref_bitor(
            self.ident.select_mask(),
            self.value.select_mask(),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////
// IdentCompareWithArray
////////////////////////////////////////////////////////////////////////////////

pub(super) struct IdentCompareWithArray<B> {
    array: Vec<IdentCompareWith<B>>,
}

impl<B> IdentCompareWithArray<B>
where
    B: ThreadSafeBool,
{
    #[cfg(feature = "parallel")]
    // Always parallel (for now)
    pub fn tree_compute_select_in_place(
        &self,
        query_ref: &SqlQueryRef<B>,
        select_mask: &mut BoolMask<B>,
    ) {
        select_mask
            .mask
            .par_iter_mut()
            .enumerate()
            .for_each(|(row_index, dst)| {
                let tree_arg = self.compute_select_row_arg(query_ref, row_index);
                *dst = query_ref.where_tree().tree_compute(tree_arg);
            });
    }

    #[cfg(not(feature = "parallel"))]
    // Always parallel (for now)
    pub fn tree_compute_select_in_place(
        &self,
        query_ref: &SqlQueryRef<B>,
        select_mask: &mut BoolMask<B>,
    ) {
        select_mask
            .mask
            .iter_mut()
            .enumerate()
            .for_each(|(row_index, dst)| {
                let tree_arg = self.compute_select_row_arg(query_ref, row_index);
                *dst = query_ref.where_tree().tree_compute(tree_arg);
            });
    }

    #[cfg(feature = "parallel")]
    fn compute_select_row_arg(
        &self,
        query_ref: &SqlQueryRef<B>,
        row_index: usize,
    ) -> Vec<OptionalBool<B>> {
        let sql_query_tree = query_ref.where_tree();
        if sql_query_tree.num_dummy_ops() == 0 {
            // Serial, no mask computation needed (could be parallel)
            sql_query_tree
                .dummy_mask()
                .iter()
                .enumerate()
                .map(|(tree_index, _)| {
                    // when no dummies, tree_index == op_index
                    OptionalBool::<B>::from_value(self.get_select_at(tree_index, row_index).clone())
                })
                .collect::<Vec<OptionalBool<B>>>()
        } else {
            // Parallel
            sql_query_tree
                .dummy_mask()
                .par_iter()
                .enumerate()
                .map(|(tree_index, dummy_not_dummy)| {
                    let value = self.par_compute_select_at(tree_index, sql_query_tree, row_index);
                    OptionalBool::<B>::from_optional_value(value, dummy_not_dummy.clone())
                })
                .collect::<Vec<OptionalBool<B>>>()
        }
    }

    #[cfg(not(feature = "parallel"))]
    fn compute_select_row_arg(
        &self,
        query_ref: &SqlQueryRef<B>,
        row_index: usize,
    ) -> Vec<OptionalBool<B>> {
        let sql_query_tree = query_ref.where_tree();
        if sql_query_tree.num_dummy_ops() == 0 {
            // Serial, no mask computation needed (could be parallel)
            sql_query_tree
                .dummy_mask()
                .iter()
                .enumerate()
                .map(|(tree_index, _)| {
                    // when no dummies, tree_index == op_index
                    OptionalBool::<B>::from_value(self.get_select_at(tree_index, row_index).clone())
                })
                .collect::<Vec<OptionalBool<B>>>()
        } else {
            // Parallel
            sql_query_tree
                .dummy_mask()
                .iter()
                .enumerate()
                .map(|(tree_index, dummy_not_dummy)| {
                    let value = self.par_compute_select_at(tree_index, sql_query_tree, row_index);
                    OptionalBool::<B>::from_optional_value(value, dummy_not_dummy.clone())
                })
                .collect::<Vec<OptionalBool<B>>>()
        }
    }

    #[inline]
    fn get_select_at(&self, op_index: usize, row_index: usize) -> &B {
        self.array[op_index].select_mask().get(row_index)
    }

    // Always parallel (for now)
    #[cfg(feature = "parallel")]
    fn par_compute_select_at(
        &self,
        tree_index: usize,
        sql_query_tree: &SqlQueryTree<B>,
        row_index: usize,
    ) -> B {
        // inclusive: op_index is in [min_op_index, max_op_index] interval
        let (min_op_index, max_op_index) = sql_query_tree.ops_at(tree_index);
        assert!(min_op_index <= max_op_index);

        if min_op_index == max_op_index {
            if min_op_index == 0 {
                assert_eq!(tree_index, 0);
                self.get_select_at(0, row_index).clone()
            } else {
                // if there is only one single position, it cannot be dummy!
                self.get_select_at(min_op_index, row_index).clone()
            }
        } else {
            let mut buffer = vec![B::get_false(); max_op_index - min_op_index + 1];
            assert!(buffer.len() > 1);
            // OR {all OpFlag AND OpSelect(row) }
            buffer
                .par_iter_mut()
                .enumerate()
                .for_each(|(buffer_index, dst)| {
                    // buffer_index range is [0, max_op_index - min_op_index]
                    let op_index = buffer_index + min_op_index;
                    let f = sql_query_tree.op_flag_at(tree_index, op_index);
                    let s = self.get_select_at(op_index, row_index);
                    *dst = f.refref_bitand(s);
                });
            par_bitor_vec(buffer).unwrap()
        }
    }

    #[cfg(not(feature = "parallel"))]
    fn par_compute_select_at(
        &self,
        tree_index: usize,
        sql_query_tree: &SqlQueryTree<B>,
        row_index: usize,
    ) -> B {
        // inclusive: op_index is in [min_op_index, max_op_index] interval
        let (min_op_index, max_op_index) = sql_query_tree.ops_at(tree_index);
        assert!(min_op_index <= max_op_index);

        if min_op_index == max_op_index {
            if min_op_index == 0 {
                assert_eq!(tree_index, 0);
                self.get_select_at(0, row_index).clone()
            } else {
                // if there is only one single position, it cannot be dummy!
                self.get_select_at(min_op_index, row_index).clone()
            }
        } else {
            let mut buffer = vec![B::get_false(); max_op_index - min_op_index + 1];
            assert!(buffer.len() > 1);
            // OR {all OpFlag AND OpSelect(row) }
            buffer
                .iter_mut()
                .enumerate()
                .for_each(|(buffer_index, dst)| {
                    // buffer_index range is [0, max_op_index - min_op_index]
                    let op_index = buffer_index + min_op_index;
                    let f = sql_query_tree.op_flag_at(tree_index, op_index);
                    let s = self.get_select_at(op_index, row_index);
                    *dst = f.refref_bitand(s);
                });
            par_bitor_vec(buffer).unwrap()
        }
    }
}

impl<B> IdentCompareWithArray<B>
where
    B: Clone,
{
    pub fn len(&self) -> usize {
        self.array.len()
    }

    pub(super) fn new_empty(query_ref: &SqlQueryRef<B>) -> Self {
        let mut arr = IdentCompareWithArray::<B> { array: vec![] };
        let n = query_ref.num_binary_ops();
        for i in 0..n {
            arr.array.push(IdentCompareWith {
                ident: IdentOpIdent::new_empty(i, query_ref.clone()),
                value: IdentOpValue::new_empty(i, query_ref.clone()),
                select_mask: BoolMask::<B>::new_empty(),
            })
        }
        arr
    }
}

impl<B> IdentCompareWithArray<B>
where
    B: ThreadSafeBool + ThreadSafeUInt + DefaultInto<B> + ValueFrom<B>,
{
    #[cfg(feature = "parallel")]
    fn pre_compute(&mut self, tables: &OrderedTables, chunck_size: usize) {
        self.array.par_iter_mut().for_each(|x| {
            rayon::join(
                || x.ident.compute(tables),
                || x.value.compute(tables, chunck_size),
            );
            x.compute_select();
        })
    }

    #[cfg(not(feature = "parallel"))]
    fn pre_compute(&mut self, tables: &OrderedTables, chunck_size: usize) {
        self.array.iter_mut().for_each(|x| {
            x.ident.compute(tables);
            x.value.compute(tables, chunck_size);
            x.compute_select();
        })
    }

    pub fn compute_select(
        &mut self,
        query_ref: &SqlQueryRef<B>,
        tables: &OrderedTables,
        chunck_size: usize,
    ) -> BoolMask<B> {
        self.pre_compute(tables, chunck_size);
        
        assert_eq!(self.len(), query_ref.num_binary_ops());

        if query_ref.num_binary_ops() == 1 {
            // Without WHERE Tree
            self.array[0].select_mask().clone()
        } else {
            // With WHERE Tree
            assert!(query_ref.num_binary_ops() > 1);
            let mut select_mask = BoolMask::<B>::none(tables.max_num_rows());
            self.tree_compute_select_in_place(query_ref, &mut select_mask);
            select_mask            
        }
    }
}
