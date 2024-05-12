use std::ops::Neg;

use super::signed_u64::{SignedU64, PLUS_INFINITY_SIGNED_U64, MINUS_INFINITY_SIGNED_U64};

pub const MINUS_PLUS_INFINITY_SIGNED_U64_RANGE: ClosedInterval<SignedU64> = ClosedInterval::<SignedU64> {
    empty: false,
    min: MINUS_INFINITY_SIGNED_U64,
    max: PLUS_INFINITY_SIGNED_U64,
};

#[derive(Debug, Clone, Copy)]
pub struct ClosedInterval<T> {
    empty: bool,
    min: T,
    max: T,
}

impl<T> Default for ClosedInterval<T>
where
    T: Default,
{
    fn default() -> Self {
        Self {
            empty: true,
            min: Default::default(),
            max: Default::default(),
        }
    }
}

impl<T> Neg for ClosedInterval<T>
where
    T: Neg<Output = T>,
{
    type Output = ClosedInterval<T>;

    fn neg(self) -> Self::Output {
        ClosedInterval::<T> {
            min: -(self.max),
            max: -self.min,
            empty: self.empty
        }
    }
}

impl<T> ClosedInterval<T>
where
    T: Ord + Default + Copy,
{
    #[inline]
    pub fn new(a: T, b: T) -> Self {
        if a > b {
            ClosedInterval::default()
        } else {
            ClosedInterval {
                empty: false,
                min: a,
                max: b,
            }
        }
    }
    #[inline]
    pub fn min(&self) -> T {
        self.min
    }
    #[inline]
    pub fn max(&self) -> T {
        self.max
    }
    #[inline]
    pub fn contains(&self, other: &T) -> bool {
        other >= &self.min && other <= &self.max
    }
}

