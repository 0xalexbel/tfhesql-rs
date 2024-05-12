use rayon::iter::*;
use sqlparser::ast::BinaryOperator;

use crate::error::FheSqlError;
use crate::query::optional_bool_tree::BoolBinOpMask;
use crate::uint::mask::{BoolMask, SizedMask};
use crate::types::UIntType;
use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;

////////////////////////////////////////////////////////////////////////////////
// BitOpMask
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct BitOpMask<B>(BoolMask<B>);

derive_wrapper_encrypt_decrypt!(BitOpMask<B>(BoolMask<B>));

pub type ClearBitOpMask = BitOpMask<bool>;

////////////////////////////////////////////////////////////////////////////////
pub trait SizedBitOpMask<B>: SizedMask {
    const NOOP: usize = 0;
    const AND: usize = 1;
    const OR: usize = 2;

    fn is_and(&self) -> &B;
    fn is_or(&self) -> &B;
    fn is_noop(&self) -> &B;
}

impl<B> SizedMask for BitOpMask<B> {
    const LEN: usize = 3;
}

impl<B> SizedBitOpMask<B> for BitOpMask<B> {
    fn is_and(&self) -> &B {
        &self.0.mask[Self::AND]
    }

    fn is_or(&self) -> &B {
        &self.0.mask[Self::OR]
    }

    fn is_noop(&self) -> &B {
        &self.0.mask[Self::NOOP]
    }
}

impl<B> From<&BitOpMask<B>> for BoolBinOpMask<B>
where
    B: Clone,
{
    fn from(value: &BitOpMask<B>) -> Self {
        BoolBinOpMask::<B> {
            is_and: value.is_and().clone(),
            is_or: value.is_or().clone(),
        }
    }
}

impl<'data, B: Sync + 'data> IntoParallelIterator for &'data BitOpMask<B> {
    type Item = &'data B;
    type Iter = rayon::slice::Iter<'data, B>;

    fn into_par_iter(self) -> Self::Iter {
        <&[B]>::into_par_iter(&self.0.mask)
    }
}

impl<'data, B: Send + 'data> IntoParallelIterator for &'data mut BitOpMask<B> {
    type Item = &'data mut B;
    type Iter = rayon::slice::IterMut<'data, B>;

    fn into_par_iter(self) -> Self::Iter {
        <&mut [B]>::into_par_iter(&mut self.0.mask)
    }
}

impl<B> BitOpMask<B>
where
    B: Clone + UIntType,
{
    pub fn new_noop() -> Self {
        let mut bm = BitOpMask::<B>(BoolMask::<B>::none(Self::LEN));
        bm.0.set(Self::NOOP);
        bm
    }
    pub fn set(&mut self, op: &BinaryOperator) -> Result<(), FheSqlError> {
        match op {
            BinaryOperator::And => {
                self.0.unset_all();
                self.0.set(Self::AND);
                Ok(())
            }
            BinaryOperator::Or => {
                self.0.unset_all();
                self.0.set(Self::OR);
                Ok(())
            }
            _ => Err(FheSqlError::unsupported_binary_op(op)),
        }
    }
}

impl std::fmt::Display for ClearBitOpMask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_set(Self::AND) {
            f.write_str("AND")
        } else if self.0.is_set(Self::OR) {
            f.write_str("OR")
        } else if self.0.is_set(Self::NOOP) {
            f.write_str("NOOP")
        } else {
            f.write_str("error")
        }
    }
}
