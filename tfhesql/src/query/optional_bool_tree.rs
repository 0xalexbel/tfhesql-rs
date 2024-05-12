use rayon::iter::*;
use rayon::slice::ParallelSlice;

use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::error::FheSqlError;
use crate::hi_lo_tree::EqNe;
use crate::uint::next_power_of_two;
use crate::uint::two_pow_n;
use crate::types::ThreadSafeBool;

////////////////////////////////////////////////////////////////////////////////
// OptionalBool
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct OptionalBool<B> {
    pub value: B,
    pub none_some: Option<EqNe<B>>,
}

impl<B> OptionalBool<B> {
    pub fn is_always_some(&self) -> bool {
        self.none_some.is_none()
    }
    pub fn eq_ne(&self) -> &EqNe<B> {
        self.none_some.as_ref().unwrap()
    }
    pub fn some(&self) -> &B {
        &self.eq_ne().ne
    }
    pub fn none(&self) -> &B {
        &self.eq_ne().eq
    }

    pub fn from_value(value: B) -> Self {
        Self {
            value,
            none_some: None,
        }
    }
    pub fn from_optional_value(value: B, none_some: EqNe<B>) -> Self {
        Self {
            value,
            none_some: Some(none_some),
        }
    }
}

impl<B> OptionalBool<B>
where
    B: ThreadSafeBool,
{
    fn bin_op(&self, op: &BoolBinOpMask<B>, rhs: &OptionalBool<B>) -> Self {
        if rhs.is_always_some() {
            OptionalBool::<B> {
                value: self.value_bin_op(op, rhs),
                none_some: None,
            }
        } else {
            let (a, b) = rayon::join(
                || {
                    // [Dummy(B) & Value(A)]
                    rhs.none().refref_bitand(&self.value)
                },
                || {
                    // { NOT Dummy[B] } AND { [Value(A) AND Value(B)] AND Op(and) || [Value(A) OR Value(B)] AND Op(or) }
                    rhs.some().ref_bitand(self.value_bin_op(op, rhs))
                },
            );

            OptionalBool::<B> {
                value: a.ref_bitor(b),
                none_some: self.none_some.clone(),
            }
        }
    }

    fn value_bin_op(&self, op: &BoolBinOpMask<B>, rhs: &OptionalBool<B>) -> B {
        let (a, b) = rayon::join(
            || {
                // [Value(A) AND Value(B)] AND Op(and)
                op.is_and.ref_bitand(self.value.refref_bitand(&rhs.value))
            },
            || {
                // [Value(A) OR Value(B)] AND Op(or)
                op.is_or.ref_bitand(self.value.refref_bitor(&rhs.value))
            },
        );
        a.ref_bitor(b)
    }
}

////////////////////////////////////////////////////////////////////////////////
// BoolBinOpMask
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct BoolBinOpMask<B> {
    pub is_and: B,
    pub is_or: B,
}

derive2_encrypt_decrypt! { BoolBinOpMask<B> {is_and:B, is_or:B} }

pub type ClearBoolBinOpMask = BoolBinOpMask<bool>;
//pub type FheBoolBinOpMask = BoolBinOpMask<FheBool>;

////////////////////////////////////////////////////////////////////////////////
// OptionalBoolTree
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct OptionalBoolTree<B> {
    // tree.len = 2^levels - 1
    tree: Vec<BoolBinOpMask<B>>,
}

derive1_encrypt_decrypt! { OptionalBoolTree<B> {tree: Vec<BoolBinOpMask<B>>} }

pub type ClearOptionalBoolTree = OptionalBoolTree<bool>;

////////////////////////////////////////////////////////////////////////////////

impl<B> OptionalBoolTree<B> {
    pub fn num_nodes(&self) -> usize {
        self.tree.len()
    }
    pub fn num_leaves(&self) -> usize {
        (self.tree.len() + 1) / 2
    }
    pub fn levels(&self) -> usize {
        let levels = next_power_of_two((self.tree.len() + 1) as u64);
        assert_eq!(self.tree.len(), (two_pow_n(levels) - 1) as usize);
        levels as usize
    }
    pub fn new_empty() -> Self {
        OptionalBoolTree::<B> { tree: vec![] }
    }
    pub fn from_binary_tree_vec(
        tree: Vec<BoolBinOpMask<B>>,
        levels: usize,
    ) -> Result<Self, FheSqlError> {
        if tree.is_empty() {
            return Ok(OptionalBoolTree::new_empty());
        }

        // tree.len must be = 2^k - 1
        assert!((tree.len() + 1).is_power_of_two());
        assert_eq!(tree.len(), (two_pow_n(levels as u8) - 1) as usize);

        let min_depth = next_power_of_two((tree.len() + 1) as u64) - 1;
        let expected_len = (two_pow_n(min_depth + 1) - 1) as usize;
        if tree.len() != expected_len {
            return Err(FheSqlError::InternalError(
                "Invalid binary tree".to_string(),
            ));
        }
        Ok(OptionalBoolTree::<B> { tree })
    }

    fn par_compute_pairs(ops: &[BoolBinOpMask<B>], v: &[OptionalBool<B>]) -> Vec<OptionalBool<B>>
    where
        B: ThreadSafeBool,
    {
        assert_eq!(v.len(), ops.len() * 2);
        assert_eq!(v.len() % 2, 0);

        v.par_chunks(2)
            .zip(ops.par_iter())
            .map(|(ab, op)| ab[0].bin_op(op, &ab[1]))
            .collect()
    }

    pub fn par_compute(&self, slice: &[OptionalBool<B>]) -> OptionalBool<B>
    where
        B: ThreadSafeBool,
    {
        if slice.len() == 1 {
            return slice[0].clone();
        }

        assert!((self.tree.len() + 1).is_power_of_two());
        assert!(slice.len().is_power_of_two());
        assert_eq!(slice.len(), self.num_leaves() * 2);
        assert_eq!(slice.len(), two_pow_n(self.levels() as u8) as usize);

        let mut max_op: usize = self.num_nodes();
        let mut min_op: usize = max_op - slice.len() / 2;
        let mut v = Self::par_compute_pairs(&self.tree[min_op..max_op], slice);
        assert!(v.len() == slice.len() / 2);
        if v.len() == 1 {
            return v[0].clone();
        }

        loop {
            max_op = min_op;
            min_op = max_op - v.len() / 2;
            assert!(max_op - min_op >= 1);
            let w = Self::par_compute_pairs(&self.tree[min_op..max_op], &v);
            assert!(v.len() / 2 == w.len());
            v = w;
            if v.len() == 1 {
                return v[0].clone();
            }
        }
    }
}
