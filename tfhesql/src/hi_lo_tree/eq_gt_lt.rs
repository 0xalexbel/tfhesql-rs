use crate::default_into::*;
use crate::encrypt::derive3_encrypt_decrypt;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::bitops::*;
use crate::types::*;
use crate::utils::rayon::rayon_join3;

////////////////////////////////////////////////////////////////////////////////
// EqGtLt
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct EqGtLt<B> {
    pub eq: B,
    pub gt: B,
    pub lt: B,
}

derive3_encrypt_decrypt! { EqGtLt<B> {eq:B, gt:B, lt:B} }

////////////////////////////////////////////////////////////////////////////////

impl<B> Default for EqGtLt<B>
where
    B: BooleanType,
{
    fn default() -> Self {
        Self {
            eq: B::get_false(),
            gt: B::get_false(),
            lt: B::get_false(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

impl<B> std::fmt::Display for EqGtLt<B>
where
    B: DebugToString,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_fmt(format_args!(
            "[eq:{}, gt:{}, lt:{}]",
            self.eq.debug_to_string(),
            self.gt.debug_to_string(),
            self.lt.debug_to_string()
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////

impl<B> EqGtLt<B>
where
    B: ThreadSafeBool,
{
    pub fn from_eq_gt(eq: &B, gt: &B) -> Self {
        let (noteq, lteq) = rayon::join(|| eq.ref_not(), || gt.ref_not());
        let lt = lteq.refref_bitand(&noteq);
        EqGtLt {
            eq: eq.clone(),
            gt: gt.clone(),
            lt,
        }
    }

    pub fn from_eq_lt(eq: &B, lt: &B) -> Self {
        let (noteq, gteq) = rayon::join(|| eq.ref_not(), || lt.ref_not());
        let gt = gteq.refref_bitand(&noteq);
        EqGtLt {
            eq: eq.clone(),
            gt,
            lt: lt.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

impl<B> RefBitAnd<B> for EqGtLt<B>
where
    B: ThreadSafeBool,
{
    type Output = Self;

    fn ref_bitand(&self, rhs: B) -> Self::Output {
        let rhs1 = rhs.clone();
        let rhs2 = rhs.clone();
        let rhs3 = rhs;
        let (eq, gt, lt) = rayon_join3(
            || self.eq.ref_bitand(rhs1),
            || self.gt.ref_bitand(rhs2),
            || self.lt.ref_bitand(rhs3),
        );
        EqGtLt { eq, gt, lt }
    }

    fn refref_bitand(&self, rhs: &B) -> Self::Output {
        let (eq, gt, lt) = rayon_join3(
            || self.eq.refref_bitand(rhs),
            || self.gt.refref_bitand(rhs),
            || self.lt.refref_bitand(rhs),
        );
        EqGtLt { eq, gt, lt }
    }
}

////////////////////////////////////////////////////////////////////////////////

impl<B> RefBitOr for EqGtLt<B>
where
    B: ThreadSafeBool,
{
    type Output = Self;

    fn ref_bitor(&self, rhs: EqGtLt<B>) -> Self::Output {
        let (eq, gt, lt) = rayon_join3(
            || self.eq.ref_bitor(rhs.eq),
            || self.gt.ref_bitor(rhs.gt),
            || self.lt.ref_bitor(rhs.lt),
        );
        EqGtLt { eq, gt, lt }
    }

    fn refref_bitor(&self, rhs: &EqGtLt<B>) -> Self::Output {
        let (eq, gt, lt) = rayon_join3(
            || self.eq.refref_bitor(&rhs.eq),
            || self.gt.refref_bitor(&rhs.gt),
            || self.lt.refref_bitor(&rhs.lt),
        );
        EqGtLt { eq, gt, lt }
    }
}
