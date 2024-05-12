use crate::sql_ast::and_or_ast::AstNumBinaryOp;
use crate::{sql_ast::ComparatorMask, uint::mask::BoolMask};

use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;

use super::sql_query_value::{ClearSqlQueryValue, SqlQueryRightOperand};

////////////////////////////////////////////////////////////////////////////////
// SqlQueryBinOpArray
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct SqlQueryBinOpArray<B> {
    pub(super) array: Vec<SqlQueryBinaryOp<B>>,
}

derive1_encrypt_decrypt! { SqlQueryBinOpArray<B> {array: Vec<SqlQueryBinaryOp<B>>} }

pub type ClearSqlQueryBinOpArray = SqlQueryBinOpArray<bool>;

impl ClearSqlQueryBinOpArray {
    pub fn build(num_bin_ops: &[AstNumBinaryOp]) -> Self {
        if num_bin_ops.is_empty() {
            return SqlQueryBinOpArray { array: vec![] };
        }
        let array = num_bin_ops
            .iter()
            .filter(|x| !x.is_dummy)
            .map(|x| ClearSqlQueryBinaryOp::build(x).unwrap())
            .collect::<Vec<ClearSqlQueryBinaryOp>>();
        ClearSqlQueryBinOpArray { array }
    }
}

impl<B> SqlQueryBinOpArray<B> {
    #[inline]
    pub fn new_empty() -> Self {
        SqlQueryBinOpArray::<B> { array: vec![] }
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.array.is_empty()
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.array.len()
    }
    #[inline]
    pub fn get(&self, index: usize) -> &SqlQueryBinaryOp<B> {
        &self.array[index]
    }
}

////////////////////////////////////////////////////////////////////////////////
// SqlQueryBinaryOp
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct SqlQueryBinaryOp<B> {
    pub position_mask: BoolMask<B>,
    pub comparator_mask: ComparatorMask<B>,
    pub left_ident_mask: BoolMask<B>,
    pub right: SqlQueryRightOperand<B>,
}

pub type ClearSqlQueryBinaryOp = SqlQueryBinaryOp<bool>;

impl ClearSqlQueryBinaryOp {
    pub(super) fn build(value: &AstNumBinaryOp) -> Option<Self> {
        if value.is_dummy {
            return None;
        }
        Some(ClearSqlQueryBinaryOp {
            position_mask: value.pos_mask.clone(),
            comparator_mask: value.op_mask.clone(),
            left_ident_mask: value.left_ident_mask.clone(),
            right: ClearSqlQueryValue::build(&value.right_ident_mask, &value.right_value, value.right_minus_sign),
        })
    }
}

derive4_encrypt_decrypt! { SqlQueryBinaryOp<B> {position_mask: BoolMask<B>, comparator_mask: ComparatorMask<B>, left_ident_mask: BoolMask<B>, right: SqlQueryRightOperand<B>} }
