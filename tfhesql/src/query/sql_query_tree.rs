use super::optional_bool_tree::*;
use super::sql_query_binops::SqlQueryBinOpArray;
use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::error::FheSqlError;
use crate::hi_lo_tree::EqNe;
use crate::query::sql_query_binops::ClearSqlQueryBinOpArray;
use crate::sql_ast::and_or_ast::AstTreeResult;
use crate::uint::two_pow_n;
use crate::types::ThreadSafeBool;

////////////////////////////////////////////////////////////////////////////////
// SqlQueryTree
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct SqlQueryTree<B> {
    tree: OptionalBoolTree<B>,
    pub(super) dummy_mask: Vec<EqNe<B>>,
    pub(super) compare_ops: SqlQueryBinOpArray<B>,
}

pub type ClearSqlQueryTree = SqlQueryTree<bool>;

derive3_encrypt_decrypt! { SqlQueryTree<B> {tree: OptionalBoolTree<B>, dummy_mask: Vec<EqNe<B>>, compare_ops: SqlQueryBinOpArray<B> } }

////////////////////////////////////////////////////////////////////////////////

impl ClearSqlQueryTree {
    pub fn build(ast_tree_result: AstTreeResult) -> Result<Self, FheSqlError> {
        match ast_tree_result {
            AstTreeResult::Boolean(_) => Ok(ClearSqlQueryTree::new_empty()),
            AstTreeResult::Tree(ast_tree) => {
                let tree_vec = ast_tree
                    .bool_ops_tree()
                    .iter()
                    .map(ClearBoolBinOpMask::from)
                    .collect::<Vec<ClearBoolBinOpMask>>();

                assert_eq!(tree_vec.len(), ast_tree.bool_ops_tree().len());
                assert_eq!(
                    tree_vec.len(),
                    (two_pow_n(ast_tree.bool_ops_tree_levels() as u8) - 1) as usize
                );

                let and_or_tree = ClearOptionalBoolTree::from_binary_tree_vec(
                    tree_vec,
                    ast_tree.bool_ops_tree_levels() as usize,
                )?;

                let dummy_mask = ast_tree.dummy_not_dummy();
                assert!(
                    (dummy_mask.len() == and_or_tree.num_leaves() * 2)
                        || and_or_tree.num_leaves() == 0
                );

                let compare_ops = ClearSqlQueryBinOpArray::build(ast_tree.num_ops());
                assert!(dummy_mask.len() >= compare_ops.len());

                Ok(ClearSqlQueryTree {
                    tree: and_or_tree,
                    compare_ops,
                    dummy_mask,
                })
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

impl<B> SqlQueryTree<B>
where
    B: ThreadSafeBool,
{
    pub fn tree_compute(&self, arg: Vec<OptionalBool<B>>) -> B {
        self.tree.par_compute(&arg).value
    }
}

////////////////////////////////////////////////////////////////////////////////

impl<B> SqlQueryTree<B> {
    pub fn new_empty() -> Self {
        SqlQueryTree {
            tree: OptionalBoolTree::<B>::new_empty(),
            compare_ops: SqlQueryBinOpArray::<B>::new_empty(),
            dummy_mask: vec![],
        }
    }
    pub fn is_empty(&self) -> bool {
        self.compare_ops.is_empty()
    }

    // #[inline]
    // pub fn is_dummy_op(&self, tree_index: usize) -> &B {
    //     &self.dummy_mask[tree_index].eq
    // }

    // #[inline]
    // pub fn is_not_dummy_op(&self, tree_index: usize) -> &B {
    //     &self.dummy_mask[tree_index].ne
    // }

    #[inline]
    pub fn dummy_mask(&self) -> &Vec<EqNe<B>> {
        &self.dummy_mask
    }

    #[inline]
    pub fn num_dummy_ops(&self) -> usize {
        self.max_num_ops() - self.num_ops()
    }

    #[inline]
    pub fn max_num_ops(&self) -> usize {
        // tree is made of AND/OR binary operators
        // for each tree leaf we need 2 compare ops
        assert_eq!(self.dummy_mask.len(), self.tree.num_leaves() * 2);
        self.tree.num_leaves() * 2
    }

    #[inline]
    pub fn num_ops(&self) -> usize {
        self.compare_ops.len()
    }

    /// Interval is  inclusive
    #[inline]
    pub fn ops_at(&self, tree_index: usize) -> (usize, usize) {
        assert!(self.num_ops() > 0);
        assert!(self.max_num_ops() > tree_index);
        // op_interval inverse formula.
        // D = dummies
        // n = num ops
        // N = n + D
        // i > 0 & i < N E(i) = { Op(k), k in [Max(1, i-D); Min(n-1,i)]}
        // Op(0) = {0}
        if tree_index == 0 {
            (0, 0)
        } else {
            let d = self.num_dummy_ops();
            let n = self.num_ops();
            let max = if tree_index <= d { 1 } else { tree_index - d };
            let min = (n - 1).min(tree_index);
            assert!(max <= min);
            assert!(max >= 1);
            assert!(min < n);
            (max, min)
        }
    }

    #[inline]
    pub fn op_flag_at(&self, tree_index: usize, op_index: usize) -> &B {
        assert!(tree_index >= op_index);
        assert!(tree_index - op_index < self.num_dummy_ops());
        assert!(self.num_ops() > op_index);
        assert!(self.max_num_ops() > tree_index);
        if tree_index == 0 || op_index == 0 {
            // Op(0) = {0}
            assert_eq!(tree_index, 0);
            assert_eq!(op_index, 0);
            self.compare_ops.array[0].position_mask.get(0)
        } else {
            self.compare_ops.array[op_index]
                .position_mask
                .get(tree_index - op_index)
        }
    }
}
