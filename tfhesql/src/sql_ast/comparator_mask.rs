use rayon::iter::*;
use sqlparser::ast::BinaryOperator;

use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;

use crate::uint::mask::BoolMask;
use crate::uint::mask::Mask;
use crate::uint::mask::SizedMask;
use crate::uint::triangular_matrix::TriangularMatrix;
use crate::utils::rayon::rayon_join3;
use crate::bitops::par_bitor_8;
use crate::bitops::par_bitor_10_ref;
use crate::bitops::{RefNot, RefBitAnd, RefBitOr};
use crate::types::UIntType;

////////////////////////////////////////////////////////////////////////////////
// ComparatorMask
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct ComparatorMask<B>(BoolMask<B>);

derive_wrapper_encrypt_decrypt!(ComparatorMask<B>(BoolMask<B>));

pub type ClearComparatorMask = ComparatorMask<bool>;

////////////////////////////////////////////////////////////////////////////////

pub trait SizedComparatorMask: SizedMask {
    const EQ: usize = 0;
    const GT: usize = 1;
    const LT: usize = 2;
    const GTEQ: usize = 3;
    const LTEQ: usize = 4;
    const NOTEQ: usize = 5;
}

impl<B> SizedMask for ComparatorMask<B> {
    const LEN: usize = 6;
}

impl<B> SizedComparatorMask for ComparatorMask<B> {}

impl<B> ComparatorMask<B> {
    pub fn mask(&self) -> &BoolMask<B> {
        &self.0
    }
}

impl<B> ComparatorMask<B>
where
    B: Send + Sync + RefBitAnd<Output = B> + RefBitOr<Output = B> + Clone + RefNot,
{
    /// triangular_matrix_and(i, j) = OperatorMask[k; Op(k) AND Tr(i, j)]
    pub fn triangular_matrix_and(&self, matrix: &TriangularMatrix<B>) -> TriangularMatrix<Self> {
        let elements = matrix
            .elements()
            .par_iter()
            .map(|e| ComparatorMask(self.0.refref_bitand(e)))
            .collect();
        TriangularMatrix::<Self>::from_vec(elements, matrix.dim())
    }

    /// Cost:
    /// -----
    /// - 2 x Bit Not
    /// - 1 x Bit And
    /// - 1 x Bit Or
    ///
    /// Formula:
    /// --------
    /// - noteq = !eq
    /// - lteq  = !gt
    /// - gteq  = gt OR eq
    /// - lt    = lteq AND noteq
    fn set_eq_gt(&mut self, eq: &B, gt: &B) {
        let (noteq, lteq, gteq) =
            rayon_join3(|| eq.ref_not(), || gt.ref_not(), || gt.refref_bitor(eq));
        let lt = lteq.refref_bitand(&noteq);
        self.0.mask[Self::EQ] = eq.clone();
        self.0.mask[Self::GT] = gt.clone();
        self.0.mask[Self::LT] = lt;
        self.0.mask[Self::GTEQ] = gteq;
        self.0.mask[Self::LTEQ] = lteq;
        self.0.mask[Self::NOTEQ] = noteq;
    }

    /// Cost:
    /// -----
    /// - 2 x Bit Not
    /// - 1 x Bit And
    /// - 1 x Bit Or
    ///
    /// Formula:
    /// --------
    /// - noteq = !eq
    /// - gteq  = !lt
    /// - lteq  = lt OR eq
    /// - gt    = gteq AND noteq
    fn set_eq_lt(&mut self, eq: &B, lt: &B) {
        let (noteq, gteq, lteq) =
            rayon_join3(|| eq.ref_not(), || lt.ref_not(), || lt.refref_bitor(eq));
        let gt = gteq.refref_bitand(&noteq);
        self.0.mask[Self::EQ] = eq.clone();
        self.0.mask[Self::GT] = gt;
        self.0.mask[Self::LT] = lt.clone();
        self.0.mask[Self::GTEQ] = gteq;
        self.0.mask[Self::LTEQ] = lteq;
        self.0.mask[Self::NOTEQ] = noteq;
    }

    /// Cost:
    /// -----
    /// - 3 x Bit Or
    ///
    /// Formula:
    /// --------
    /// - noteq = !eq
    /// - gteq  = !lt
    /// - lteq  = !gt
    fn set_eq_gt_lt(&mut self, eq: &B, gt: &B, lt: &B) {
        let (noteq, gteq, lteq) = rayon_join3(|| eq.ref_not(), || lt.ref_not(), || gt.ref_not());
        self.0.mask[Self::EQ] = eq.clone();
        self.0.mask[Self::GT] = gt.clone();
        self.0.mask[Self::LT] = lt.clone();
        self.0.mask[Self::GTEQ] = gteq;
        self.0.mask[Self::LTEQ] = lteq;
        self.0.mask[Self::NOTEQ] = noteq;
    }

    fn invert(&mut self) {
        self.0.mask.swap(Self::GT, Self::LT);
        self.0.mask.swap(Self::GTEQ, Self::LTEQ);
    }

    pub fn or_and_eq_ne(&self, eq: &B, ne: &B) -> B
    where
        B: DefaultInto<B>,
    {
        let (eq, ne) = rayon::join(
            || self.mask().mask[Self::EQ].refref_bitand(eq),
            || self.mask().mask[Self::NOTEQ].refref_bitand(ne),
        );
        eq.refref_bitor(&ne)
    }

    /// Cost:
    /// -----
    /// - 2 x Bit Not
    /// - 7 x Bit And
    /// - 6 x Bit Or
    pub fn or_and_eq_gt(&self, eq: &B, gt: &B) -> B
    where
        B: DefaultInto<B>,
    {
        let mut c1 = self.clone();
        // Compute the complete comparator results from EQ and GT (Cost: 2xNOT 1xOR 1xAND)
        c1.set_eq_gt(eq, gt);
        // EqGt represents X == C(i) and X > C(i)
        // We must reverse X and C(i) to get C(i) == X and C(i) < X
        c1.invert();
        // Compute MASK(i) & RESULT(i) (Cost: 6xAND)
        let and_mask = RefBitAnd::<Mask<B>>::refref_bitand(self.mask(), c1.mask());
        // Compute full OR (Cost: 5xOR)
        par_bitor_8(&and_mask.mask).unwrap()
    }

    /// Cost:
    /// -----
    /// - 2 x Bit Not
    /// - 7 x Bit And
    /// - 6 x Bit Or
    pub fn or_and_eq_lt(&self, eq: &B, lt: &B) -> B
    where
        B: DefaultInto<B>,
    {
        let mut c1 = self.clone();
        // Compute the complete comparator results from EQ and LT (Cost: 2xNOT 1xOR 1xAND)
        c1.set_eq_lt(eq, lt);
        // EqGt represents X == C(i) and X > C(i)
        // We must reverse X and C(i) to get C(i) == X and C(i) < X
        c1.invert();
        // Compute MASK(i) & RESULT(i) (Cost: 6xAND)
        let and_mask = RefBitAnd::<Mask<B>>::refref_bitand(self.mask(), c1.mask());
        // Compute full OR (Cost: 5xOR)
        par_bitor_8(&and_mask.mask).unwrap()
    }

    /// Cost:
    /// -----
    /// - 3 x Bit Not
    /// - 6 x Bit And
    /// - 5 x Bit Or
    pub fn or_and_eq_gt_lt(&self, eq: &B, gt: &B, lt: &B) -> B
    where
        B: DefaultInto<B>,
    {
        let mut c1 = self.clone();
        // Compute the complete comparator results from EQ, GT and LT (Cost: 3xNOT)
        c1.set_eq_gt_lt(eq, gt, lt);
        // EqGt represents X == C(i) and X > C(i)
        // We must reverse X and C(i) to get C(i) == X and C(i) < X
        c1.invert();
        // Compute MASK(i) & RESULT(i) (Cost: 6xAND)
        let and_mask = RefBitAnd::<Mask<B>>::refref_bitand(self.mask(), c1.mask());
        // Compute full OR (Cost: 5xOR)
        par_bitor_8(&and_mask.mask).unwrap()
    }

    /// OR { Self AND [bool;3] }
    pub fn or_and_value3(&self, eq_gt_lt: &[bool; 3]) -> Option<B> {
        let mut vec = vec![];
        // 0: Eq '>'
        if eq_gt_lt[Self::EQ] {
            vec.push(&self.0.mask[Self::EQ]);
        }
        // 1: Gt '>'
        if eq_gt_lt[Self::GT] {
            vec.push(&self.0.mask[Self::GT]);
        }
        // 2: Lt '<'
        if eq_gt_lt[Self::LT] {
            vec.push(&self.0.mask[Self::LT]);
        }
        // 3: GtEq '>='
        if eq_gt_lt[Self::GT] || eq_gt_lt[Self::EQ] {
            vec.push(&self.0.mask[Self::GTEQ]);
        }
        // 4: LtEq '<='
        if eq_gt_lt[Self::LT] || eq_gt_lt[Self::EQ] {
            vec.push(&self.0.mask[Self::LTEQ]);
        }
        // 5: NotEq '!='
        if !eq_gt_lt[Self::EQ] {
            vec.push(&self.0.mask[Self::NOTEQ]);
        }
        assert!(vec.len() == 3);
        par_bitor_10_ref(&vec)
    }
}

impl<'data, B: Sync + 'data> IntoParallelIterator for &'data ComparatorMask<B> {
    type Item = &'data B;
    type Iter = rayon::slice::Iter<'data, B>;

    fn into_par_iter(self) -> Self::Iter {
        <&[B]>::into_par_iter(&self.0.mask)
    }
}

impl<'data, B: Send + 'data> IntoParallelIterator for &'data mut ComparatorMask<B> {
    type Item = &'data mut B;
    type Iter = rayon::slice::IterMut<'data, B>;

    fn into_par_iter(self) -> Self::Iter {
        <&mut [B]>::into_par_iter(&mut self.0.mask)
    }
}

impl<B> ComparatorMask<B>
where
    B: Clone + UIntType,
{
    pub fn none() -> Self {
        ComparatorMask::<B>(BoolMask::<B>::none(Self::LEN))
    }
    pub fn set(&mut self, op: &BinaryOperator) {
        match op {
            BinaryOperator::Gt => self.0.set(Self::GT),
            BinaryOperator::Lt => self.0.set(Self::LT),
            BinaryOperator::GtEq => self.0.set(Self::GTEQ),
            BinaryOperator::LtEq => self.0.set(Self::LTEQ),
            BinaryOperator::Eq => self.0.set(Self::EQ),
            BinaryOperator::NotEq => self.0.set(Self::NOTEQ),
            _ => panic!("Unsupported binary operator"),
        };
    }
}

impl ClearComparatorMask {
    pub fn set_u64(&mut self, left: u64, right: u64) {
        self.0.mask[Self::EQ] = left == right;
        self.0.mask[Self::GT] = left > right;
        self.0.mask[Self::LT] = left < right;
        self.0.mask[Self::GTEQ] = left >= right;
        self.0.mask[Self::LTEQ] = left <= right;
        self.0.mask[Self::NOTEQ] = left != right;
    }
}

impl std::fmt::Display for ClearComparatorMask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_set(Self::GT) {
            f.write_str(">")
        } else if self.0.is_set(Self::GTEQ) {
            f.write_str(">=")
        } else if self.0.is_set(Self::LT) {
            f.write_str("<")
        } else if self.0.is_set(Self::LTEQ) {
            f.write_str("<=")
        } else if self.0.is_set(Self::EQ) {
            f.write_str("=")
        } else if self.0.is_set(Self::NOTEQ) {
            f.write_str("<>")
        } else {
            f.write_str("__")
        }
    }
}
