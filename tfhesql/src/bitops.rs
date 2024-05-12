use rayon::iter::*;
use tfhe::{FheBool, FheUint8};

use crate::default_into::DefaultInto;
use crate::utils::rayon::rayon_join3;
use crate::utils::rayon::rayon_join4;
use crate::utils::rayon::rayon_join5;

#[cfg(feature = "stats")]
use crate::stats::*;

////////////////////////////////////////////////////////////////////////////////
// RefBitAnd
////////////////////////////////////////////////////////////////////////////////

pub trait RefBitAnd<Rhs = Self, Output = Self>: Sized {
    type Output;
    fn ref_bitand(&self, rhs: Rhs) -> Self::Output;
    fn refref_bitand(&self, rhs: &Rhs) -> Self::Output;
}

impl RefBitAnd for bool {
    type Output = Self;

    #[inline(always)]
    fn ref_bitand(&self, rhs: bool) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_bool_and();
        self & rhs
    }
    #[inline(always)]
    fn refref_bitand(&self, rhs: &bool) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_bool_and();
        self & rhs
    }
}

impl RefBitAnd for u8 {
    type Output = Self;

    #[inline(always)]
    fn ref_bitand(&self, rhs: u8) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_u8_and();
        self & rhs
    }
    #[inline(always)]
    fn refref_bitand(&self, rhs: &u8) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_u8_and();
        self & rhs
    }
}

impl RefBitAnd for FheBool {
    type Output = Self;

    #[inline(always)]
    fn ref_bitand(&self, rhs: FheBool) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_bool_and();
        std::ops::BitAnd::<FheBool>::bitand(self, rhs)
    }
    #[inline(always)]
    fn refref_bitand(&self, rhs: &FheBool) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_bool_and();
        std::ops::BitAnd::<&FheBool>::bitand(self, rhs)
    }
}

impl RefBitAnd for FheUint8 {
    type Output = Self;

    #[inline(always)]
    fn ref_bitand(&self, rhs: FheUint8) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_u8_and();
        std::ops::BitAnd::<FheUint8>::bitand(self, rhs)
    }
    #[inline(always)]
    fn refref_bitand(&self, rhs: &FheUint8) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_u8_and();
        std::ops::BitAnd::<&FheUint8>::bitand(self, rhs)
    }
}

impl<T, const N: usize> RefBitAnd for [T; N]
where
    T: RefBitAnd<Output = T> + Send + Sync + Clone + DefaultInto<T>,
{
    type Output = Self;

    fn ref_bitand(&self, rhs: Self) -> Self::Output {
        let mut a = <[T; N]>::default_into();
        a.par_iter_mut()
            .zip(self.par_iter().zip(rhs.par_iter()))
            .for_each(|(dst, (left, right))| {
                *dst = left.refref_bitand(right);
            });
        a
    }

    fn refref_bitand(&self, rhs: &Self) -> Self::Output {
        let mut a = <[T; N]>::default_into();
        a.par_iter_mut()
            .zip(self.par_iter().zip(rhs.par_iter()))
            .for_each(|(dst, (left, right))| {
                *dst = left.refref_bitand(right);
            });
        a
    }
}

impl<T> RefBitAnd for Vec<T>
where
    T: RefBitAnd<Output = T> + Send + Sync + Clone + DefaultInto<T>,
{
    type Output = Self;

    fn ref_bitand(&self, rhs: Self) -> Self::Output {
        let mut a = vec![T::default_into(); self.len()];
        a.par_iter_mut()
            .zip(self.par_iter().zip(rhs.par_iter()))
            .for_each(|(dst, (left, right))| {
                *dst = left.refref_bitand(right);
            });
        a
    }

    fn refref_bitand(&self, rhs: &Self) -> Self::Output {
        let mut a = vec![T::default_into(); self.len()];
        a.par_iter_mut()
            .zip(self.par_iter().zip(rhs.par_iter()))
            .for_each(|(dst, (left, right))| {
                *dst = left.refref_bitand(right);
            });
        a
    }
}

////////////////////////////////////////////////////////////////////////////////
// RefBitOr
////////////////////////////////////////////////////////////////////////////////

pub trait RefBitOr<Rhs = Self>: Sized {
    type Output;
    fn ref_bitor(&self, rhs: Rhs) -> Self::Output;
    fn refref_bitor(&self, rhs: &Rhs) -> Self::Output;
}

impl RefBitOr for bool {
    type Output = Self;

    #[inline(always)]
    fn ref_bitor(&self, rhs: bool) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_bool_or();
        self | rhs
    }
    #[inline(always)]
    fn refref_bitor(&self, rhs: &bool) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_bool_or();
        self | rhs
    }
}

impl RefBitOr for u8 {
    type Output = Self;

    #[inline(always)]
    fn ref_bitor(&self, rhs: u8) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_u8_or();
        self | rhs
    }
    #[inline(always)]
    fn refref_bitor(&self, rhs: &u8) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_u8_or();
        self | rhs
    }
}

impl RefBitOr for FheBool {
    type Output = Self;

    #[inline(always)]
    fn ref_bitor(&self, rhs: FheBool) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_bool_or();
        std::ops::BitOr::<FheBool>::bitor(self, rhs)
    }
    #[inline(always)]
    fn refref_bitor(&self, rhs: &FheBool) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_bool_or();
        std::ops::BitOr::<&FheBool>::bitor(self, rhs)
    }
}

impl RefBitOr for FheUint8 {
    type Output = Self;

    #[inline(always)]
    fn ref_bitor(&self, rhs: FheUint8) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_u8_or();
        std::ops::BitOr::<FheUint8>::bitor(self, rhs)
    }
    #[inline(always)]
    fn refref_bitor(&self, rhs: &FheUint8) -> Self::Output {
        #[cfg(feature = "stats")]
        inc_u8_or();
        std::ops::BitOr::<&FheUint8>::bitor(self, rhs)
    }
}

impl<T, const N: usize> RefBitOr for [T; N]
where
    T: RefBitOr<Output = T> + Send + Sync + Clone + DefaultInto<T>,
{
    type Output = Self;

    fn ref_bitor(&self, rhs: Self) -> Self::Output {
        let mut a = <[T; N]>::default_into();
        a.par_iter_mut()
            .zip(self.par_iter().zip(rhs.par_iter()))
            .for_each(|(dst, (left, right))| {
                *dst = left.refref_bitor(right);
            });
        a
    }

    fn refref_bitor(&self, rhs: &Self) -> Self::Output {
        let mut a = <[T; N]>::default_into();
        a.par_iter_mut()
            .zip(self.par_iter().zip(rhs.par_iter()))
            .for_each(|(dst, (left, right))| {
                *dst = left.refref_bitor(right);
            });
        a
    }
}

impl<T> RefBitOr for Vec<T>
where
    T: RefBitOr<Output = T> + Send + Sync + Clone + DefaultInto<T>,
{
    type Output = Self;

    fn ref_bitor(&self, rhs: Self) -> Self::Output {
        let mut a = vec![T::default_into(); self.len()];
        a.par_iter_mut()
            .zip(self.par_iter().zip(rhs.par_iter()))
            .for_each(|(dst, (left, right))| {
                *dst = left.refref_bitor(right);
            });
        a
    }

    fn refref_bitor(&self, rhs: &Self) -> Self::Output {
        let mut a = vec![T::default_into(); self.len()];
        a.par_iter_mut()
            .zip(self.par_iter().zip(rhs.par_iter()))
            .for_each(|(dst, (left, right))| {
                *dst = left.refref_bitor(right);
            });
        a
    }
}

////////////////////////////////////////////////////////////////////////////////
// RefNot
////////////////////////////////////////////////////////////////////////////////

pub trait RefNot {
    fn ref_not(&self) -> Self;
}

impl<T, const N: usize> RefNot for [T; N]
where
    T: RefNot + Send + Sync + Clone + DefaultInto<T>,
{
    fn ref_not(&self) -> Self {
        let mut a = <[T; N]>::default_into();
        a.par_iter_mut()
            .zip(self.par_iter())
            .for_each(|(dst, left)| {
                *dst = left.ref_not();
            });
        a
    }
}

impl<T> RefNot for Vec<T>
where
    T: RefNot + Send + Sync + Clone + DefaultInto<T>,
{
    fn ref_not(&self) -> Self {
        let mut a = vec![T::default_into(); self.len()];
        a.par_iter_mut()
            .zip(self.par_iter())
            .for_each(|(dst, left)| {
                *dst = left.ref_not();
            });
        a
    }
}

impl RefNot for FheBool {
    #[inline(always)]
    fn ref_not(&self) -> Self {
        #[cfg(feature = "stats")]
        inc_bool_not();
        std::ops::Not::not(self)
    }
}

impl RefNot for bool {
    #[inline(always)]
    fn ref_not(&self) -> Self {
        #[cfg(feature = "stats")]
        inc_bool_not();
        std::ops::Not::not(self)
    }
}

impl RefNot for FheUint8 {
    #[inline(always)]
    fn ref_not(&self) -> Self {
        #[cfg(feature = "stats")]
        inc_u8_not();
        std::ops::Not::not(self)
    }
}

impl RefNot for u8 {
    #[inline(always)]
    fn ref_not(&self) -> Self {
        #[cfg(feature = "stats")]
        inc_u8_not();
        std::ops::Not::not(self)
    }
}

pub fn par_bitor_vec<T>(v: Vec<T>) -> Option<T>
where
    T: Send + Sync + RefBitOr<Output = T> + Clone,
{
    use rayon::slice::ParallelSlice;
    if v.len() <= 8 {
        return par_bitor_8(&v);
    }

    let mut y: Vec<T> = v
        .par_chunks(2)
        .map(|x| {
            if x.len() == 1 {
                x[0].clone()
            } else {
                x[0].refref_bitor(&x[1])
            }
        })
        .collect();
    if y.len() == 1 {
        return Some(y[0].clone());
    }
    assert!(y.len() < v.len());
    loop {
        let z: Vec<T> = y
            .par_chunks(2)
            .map(|x| {
                if x.len() == 1 {
                    x[0].clone()
                } else {
                    x[0].refref_bitor(&x[1])
                }
            })
            .collect();
        if z.len() == 1 {
            return Some(z[0].clone());
        }
        assert!(z.len() < y.len());
        y = z;
    }
}

pub fn par_bitor_8<T>(v: &[T]) -> Option<T>
where
    T: Send + Sync + RefBitOr<Output = T> + Clone,
{
    assert!(v.len() <= 8);

    if v.is_empty() {
        None
    } else if v.len() == 1 {
        Some(v[0].clone())
    } else if v.len() == 2 {
        Some(v[0].refref_bitor(&v[1]))
    } else if v.len() == 3 {
        Some(v[0].ref_bitor(v[1].refref_bitor(&v[2])))
    } else if v.len() == 4 {
        let (a, b) = rayon::join(|| v[0].refref_bitor(&v[1]), || v[2].refref_bitor(&v[3]));
        Some(a.ref_bitor(b))
    } else if v.len() == 5 {
        let (a, b) = rayon::join(|| v[0].refref_bitor(&v[1]), || v[2].refref_bitor(&v[3]));
        Some(v[4].ref_bitor(a.ref_bitor(b)))
    } else if v.len() == 6 {
        let (a, b, c) = rayon_join3(
            || v[0].refref_bitor(&v[1]),
            || v[2].refref_bitor(&v[3]),
            || v[4].refref_bitor(&v[5]),
        );
        Some(a.ref_bitor(b.ref_bitor(c)))
    } else if v.len() == 7 {
        let (or_01, or_23, or_45) = rayon_join3(
            || v[0].refref_bitor(&v[1]),
            || v[2].refref_bitor(&v[3]),
            || v[4].refref_bitor(&v[5]),
        );
        let (or_0123, or_456) = rayon::join(|| or_01.ref_bitor(or_23), || v[6].ref_bitor(or_45));
        Some(or_0123.ref_bitor(or_456))
    } else {
        let (or_01, or_23, or_45, or_67) = rayon_join4(
            || v[0].refref_bitor(&v[1]),
            || v[2].refref_bitor(&v[3]),
            || v[4].refref_bitor(&v[5]),
            || v[6].refref_bitor(&v[7]),
        );
        let (or_0123, or_4567) = rayon::join(|| or_01.ref_bitor(or_23), || or_45.ref_bitor(or_67));
        Some(or_0123.ref_bitor(or_4567))
    }
}

pub fn par_bitor_10_ref<T>(v: &[&T]) -> Option<T>
where
    T: Send + Sync + RefBitOr<Output = T> + Clone,
{
    assert!(v.len() <= 10);

    if v.is_empty() {
        None
    } else if v.len() == 1 {
        Some(v[0].clone())
    } else if v.len() == 2 {
        Some(v[0].refref_bitor(v[1]))
    } else if v.len() == 3 {
        Some(v[0].ref_bitor(v[1].refref_bitor(v[2])))
    } else if v.len() == 4 {
        let (a, b) = rayon::join(|| v[0].refref_bitor(v[1]), || v[2].refref_bitor(v[3]));
        Some(a.ref_bitor(b))
    } else if v.len() == 5 {
        let (a, b) = rayon::join(|| v[0].refref_bitor(v[1]), || v[2].refref_bitor(v[3]));
        Some(v[4].ref_bitor(a.ref_bitor(b)))
    } else if v.len() == 6 {
        let (a, b, c) = rayon_join3(
            || v[0].refref_bitor(v[1]),
            || v[2].refref_bitor(v[3]),
            || v[4].refref_bitor(v[5]),
        );
        Some(a.ref_bitor(b.ref_bitor(c)))
    } else if v.len() == 7 {
        let (or_01, or_23, or_45) = rayon_join3(
            || v[0].refref_bitor(v[1]),
            || v[2].refref_bitor(v[3]),
            || v[4].refref_bitor(v[5]),
        );
        let (or_0123, or_456) = rayon::join(|| or_01.ref_bitor(or_23), || v[6].ref_bitor(or_45));
        Some(or_0123.ref_bitor(or_456))
    } else if v.len() == 8 {
        let (or_01, or_23, or_45, or_67) = rayon_join4(
            || v[0].refref_bitor(v[1]),
            || v[2].refref_bitor(v[3]),
            || v[4].refref_bitor(v[5]),
            || v[6].refref_bitor(v[7]),
        );
        let (or_0123, or_4567) = rayon::join(|| or_01.ref_bitor(or_23), || or_45.ref_bitor(or_67));
        Some(or_0123.ref_bitor(or_4567))
    } else if v.len() == 9 {
        let (or_01, or_23, or_45, or_67) = rayon_join4(
            || v[0].refref_bitor(v[1]),
            || v[2].refref_bitor(v[3]),
            || v[4].refref_bitor(v[5]),
            || v[6].refref_bitor(v[7]),
        );
        let (or_0123, or_4567) = rayon::join(|| or_01.ref_bitor(or_23), || or_45.ref_bitor(or_67));
        Some(or_0123.ref_bitor(or_4567).refref_bitor(v[8]))
    } else {
        let (or_01, or_23, or_45, or_67, or_89) = rayon_join5(
            || v[0].refref_bitor(v[1]),
            || v[2].refref_bitor(v[3]),
            || v[4].refref_bitor(v[5]),
            || v[6].refref_bitor(v[7]),
            || v[8].refref_bitor(v[9]),
        );
        let (or_0123, or_4567) = rayon::join(|| or_01.ref_bitor(or_23), || or_45.ref_bitor(or_67));
        Some(or_0123.ref_bitor(or_4567).ref_bitor(or_89))
    }
}

#[allow(dead_code)]
pub fn par_bitand_10<T>(v: &[T]) -> Option<T>
where
    T: Send + Sync + RefBitAnd<Output = T> + Clone,
{
    assert!(v.len() <= 10);

    if v.is_empty() {
        None
    } else if v.len() == 1 {
        Some(v[0].clone())
    } else if v.len() == 2 {
        Some(v[0].refref_bitand(&v[1]))
    } else if v.len() == 3 {
        Some(v[0].ref_bitand(v[1].refref_bitand(&v[2])))
    } else if v.len() == 4 {
        let (a, b) = rayon::join(|| v[0].refref_bitand(&v[1]), || v[2].refref_bitand(&v[3]));
        Some(a.ref_bitand(b))
    } else if v.len() == 5 {
        let (a, b) = rayon::join(|| v[0].refref_bitand(&v[1]), || v[2].refref_bitand(&v[3]));
        Some(v[4].ref_bitand(a.ref_bitand(b)))
    } else if v.len() == 6 {
        let (a, b, c) = rayon_join3(
            || v[0].refref_bitand(&v[1]),
            || v[2].refref_bitand(&v[3]),
            || v[4].refref_bitand(&v[5]),
        );
        Some(a.ref_bitand(b.ref_bitand(c)))
    } else if v.len() == 7 {
        let (or_01, or_23, or_45) = rayon_join3(
            || v[0].refref_bitand(&v[1]),
            || v[2].refref_bitand(&v[3]),
            || v[4].refref_bitand(&v[5]),
        );
        let (or_0123, or_456) = rayon::join(|| or_01.ref_bitand(or_23), || v[6].ref_bitand(or_45));
        Some(or_0123.ref_bitand(or_456))
    } else if v.len() == 8 {
        let (or_01, or_23, or_45, or_67) = rayon_join4(
            || v[0].refref_bitand(&v[1]),
            || v[2].refref_bitand(&v[3]),
            || v[4].refref_bitand(&v[5]),
            || v[6].refref_bitand(&v[7]),
        );
        let (or_0123, or_4567) =
            rayon::join(|| or_01.ref_bitand(or_23), || or_45.ref_bitand(or_67));
        Some(or_0123.ref_bitand(or_4567))
    } else if v.len() == 9 {
        let (or_01, or_23, or_45, or_67) = rayon_join4(
            || v[0].refref_bitand(&v[1]),
            || v[2].refref_bitand(&v[3]),
            || v[4].refref_bitand(&v[5]),
            || v[6].refref_bitand(&v[7]),
        );
        let (or_0123, or_4567) =
            rayon::join(|| or_01.ref_bitand(or_23), || or_45.ref_bitand(or_67));
        Some(or_0123.ref_bitand(or_4567).refref_bitand(&v[8]))
    } else {
        let (or_01, or_23, or_45, or_67, or_89) = rayon_join5(
            || v[0].refref_bitand(&v[1]),
            || v[2].refref_bitand(&v[3]),
            || v[4].refref_bitand(&v[5]),
            || v[6].refref_bitand(&v[7]),
            || v[8].refref_bitand(&v[9]),
        );
        let (or_0123, or_4567) =
            rayon::join(|| or_01.ref_bitand(or_23), || or_45.ref_bitand(or_67));
        Some(or_0123.ref_bitand(or_4567).ref_bitand(or_89))
    }
}

#[allow(dead_code)]
pub fn par_bitand_10_ref<T>(v: &[&T]) -> Option<T>
where
    T: Send + Sync + RefBitAnd<Output = T> + Clone,
{
    assert!(v.len() <= 10);

    if v.is_empty() {
        None
    } else if v.len() == 1 {
        Some(v[0].clone())
    } else if v.len() == 2 {
        Some(v[0].refref_bitand(v[1]))
    } else if v.len() == 3 {
        Some(v[0].ref_bitand(v[1].refref_bitand(v[2])))
    } else if v.len() == 4 {
        let (a, b) = rayon::join(|| v[0].refref_bitand(v[1]), || v[2].refref_bitand(v[3]));
        Some(a.ref_bitand(b))
    } else if v.len() == 5 {
        let (a, b) = rayon::join(|| v[0].refref_bitand(v[1]), || v[2].refref_bitand(v[3]));
        Some(v[4].ref_bitand(a.ref_bitand(b)))
    } else if v.len() == 6 {
        let (a, b, c) = rayon_join3(
            || v[0].refref_bitand(v[1]),
            || v[2].refref_bitand(v[3]),
            || v[4].refref_bitand(v[5]),
        );
        Some(a.ref_bitand(b.ref_bitand(c)))
    } else if v.len() == 7 {
        let (or_01, or_23, or_45) = rayon_join3(
            || v[0].refref_bitand(v[1]),
            || v[2].refref_bitand(v[3]),
            || v[4].refref_bitand(v[5]),
        );
        let (or_0123, or_456) = rayon::join(|| or_01.ref_bitand(or_23), || v[6].ref_bitand(or_45));
        Some(or_0123.ref_bitand(or_456))
    } else if v.len() == 8 {
        let (or_01, or_23, or_45, or_67) = rayon_join4(
            || v[0].refref_bitand(v[1]),
            || v[2].refref_bitand(v[3]),
            || v[4].refref_bitand(v[5]),
            || v[6].refref_bitand(v[7]),
        );
        let (or_0123, or_4567) =
            rayon::join(|| or_01.ref_bitand(or_23), || or_45.ref_bitand(or_67));
        Some(or_0123.ref_bitand(or_4567))
    } else if v.len() == 9 {
        let (or_01, or_23, or_45, or_67) = rayon_join4(
            || v[0].refref_bitand(v[1]),
            || v[2].refref_bitand(v[3]),
            || v[4].refref_bitand(v[5]),
            || v[6].refref_bitand(v[7]),
        );
        let (or_0123, or_4567) =
            rayon::join(|| or_01.ref_bitand(or_23), || or_45.ref_bitand(or_67));
        Some(or_0123.ref_bitand(or_4567).refref_bitand(v[8]))
    } else {
        let (or_01, or_23, or_45, or_67, or_89) = rayon_join5(
            || v[0].refref_bitand(v[1]),
            || v[2].refref_bitand(v[3]),
            || v[4].refref_bitand(v[5]),
            || v[6].refref_bitand(v[7]),
            || v[8].refref_bitand(v[9]),
        );
        let (or_0123, or_4567) =
            rayon::join(|| or_01.ref_bitand(or_23), || or_45.ref_bitand(or_67));
        Some(or_0123.ref_bitand(or_4567).ref_bitand(or_89))
    }
}

pub fn par_bitor_vec_ref<T>(v: Vec<&T>) -> Option<T>
where
    T: Send + Sync + RefBitOr<Output = T> + Clone,
{
    use rayon::slice::ParallelSlice;
    if v.len() <= 10 {
        return par_bitor_10_ref(&v);
    }

    let mut y: Vec<T> = v
        .par_chunks(2)
        .map(|x| {
            if x.len() == 1 {
                x[0].clone()
            } else {
                x[0].refref_bitor(x[1])
            }
        })
        .collect();
    if y.len() == 1 {
        return Some(y[0].clone());
    }
    assert!(y.len() < v.len());
    loop {
        let z: Vec<T> = y
            .par_chunks(2)
            .map(|x| {
                if x.len() == 1 {
                    x[0].clone()
                } else {
                    x[0].refref_bitor(&x[1])
                }
            })
            .collect();
        if z.len() == 1 {
            return Some(z[0].clone());
        }
        assert!(z.len() < y.len());
        y = z;
    }
}

#[allow(dead_code)]
pub fn par_bitand_vec<T>(v: Vec<T>) -> Option<T>
where
    T: Send + Sync + RefBitAnd<Output = T> + Clone,
{
    use rayon::slice::ParallelSlice;
    if v.len() <= 10 {
        return par_bitand_10(&v);
    }

    let mut y: Vec<T> = v
        .par_chunks(2)
        .map(|x| {
            if x.len() == 1 {
                x[0].clone()
            } else {
                x[0].refref_bitand(&x[1])
            }
        })
        .collect();
    if y.len() == 1 {
        return Some(y[0].clone());
    }
    assert!(y.len() < v.len());
    loop {
        let z: Vec<T> = y
            .par_chunks(2)
            .map(|x| {
                if x.len() == 1 {
                    x[0].clone()
                } else {
                    x[0].refref_bitand(&x[1])
                }
            })
            .collect();
        if z.len() == 1 {
            return Some(z[0].clone());
        }
        assert!(z.len() < y.len());
        y = z;
    }
}

pub fn par_bitand_vec_ref<T>(v: Vec<&T>) -> Option<T>
where
    T: Send + Sync + RefBitAnd<Output = T> + Clone,
{
    use rayon::slice::ParallelSlice;
    if v.len() <= 10 {
        return par_bitand_10_ref(&v);
    }

    let mut y: Vec<T> = v
        .par_chunks(2)
        .map(|x| {
            if x.len() == 1 {
                x[0].clone()
            } else {
                x[0].refref_bitand(x[1])
            }
        })
        .collect();
    if y.len() == 1 {
        return Some(y[0].clone());
    }
    assert!(y.len() < v.len());
    loop {
        let z: Vec<T> = y
            .par_chunks(2)
            .map(|x| {
                if x.len() == 1 {
                    x[0].clone()
                } else {
                    x[0].refref_bitand(&x[1])
                }
            })
            .collect();
        if z.len() == 1 {
            return Some(z[0].clone());
        }
        assert!(z.len() < y.len());
        y = z;
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_util::broadcast_set_server_key;
    use crate::{test_util::try_load_or_gen_test_keys, types::*};
    use tfhe::{prelude::FheTrivialEncrypt, set_server_key};

    //fn is_thread_safe_bool<'a, T>(_: T) where T: ThreadSafeBool<'a> {}
    fn is_thread_safe_bool<T>(_: T)
    where
        T: ThreadSafeBool,
    {
    }
    fn is_thread_safe_uint<T>(_: T)
    where
        T: ThreadSafeUInt,
    {
    }

    #[test]
    fn test_thread_safe_bool() {
        let (_ck, sk) = try_load_or_gen_test_keys(false);

        broadcast_set_server_key(&sk);
        set_server_key(sk);

        // Must compile
        is_thread_safe_bool(true);

        // Must compile
        is_thread_safe_bool(FheBool::encrypt_trivial(true));

        let one = 1_u8;
        let zero = 0_u8;
        assert_eq!(one.refref_bitand(&zero), 0_u8);

        // Must compile
        is_thread_safe_uint(one);

        // Must compile
        is_thread_safe_uint(FheUint8::encrypt_trivial(one));
    }
}
