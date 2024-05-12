use crate::ascii::ascii_to_le_u64x4;
use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::types::*;
use crate::uint::maps::U8BlockMap;
use crate::uint::maps::UIntLeBlocksPow2Array;
use crate::uint::FromN;

use super::eq_gt::Bytes256EqGt;
use super::eq_gt::Bytes64EqGt;
use super::eq_gt::EqGt;
use super::eq_gt::U8EqGtMap;
use super::hi_lo_logic_op::HiLoLogicOp;
use super::zero_max::ZeroMax;
use super::zero_max::ZeroMaxPow2Array;

////////////////////////////////////////////////////////////////////////////////
// EqNe
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct EqNe<B> {
    pub eq: B,
    pub ne: B,
}

pub type ClearEqNe = EqNe<bool>;
pub type U8EqNeMap<B> = U8BlockMap<EqNe<B>>;

// impl<T> EqNe<T>
// where
//     T: BooleanType,
// {
//     pub fn new_false() -> Self {
//         EqNe::<T> {
//             eq: T::get_false(),
//             ne: T::get_true(),
//         }
//     }
// }

impl<T> MemoryCastInto<EqGt<T>> for EqNe<T> {
    fn mem_cast_into(self) -> EqGt<T> {
        EqGt::<T> {
            eq: self.eq,
            gt: self.ne,
        }
    }
}

impl<B> std::fmt::Display for EqNe<B>
where
    B: DebugToString,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_fmt(format_args!(
            "[eq:{}, ne:{}]",
            self.eq.debug_to_string(),
            self.ne.debug_to_string()
        ))
    }
}

impl<T> EqNe<T> {
    pub fn eq(&self) -> &T {
        &self.eq
    }
    pub fn ne(&self) -> &T {
        &self.ne
    }
}

impl<T> From<(T, T)> for ClearEqNe
where
    T: tfhe::core_crypto::commons::numeric::Numeric,
{
    #[inline]
    fn from((lhs, rhs): (T, T)) -> Self {
        EqNe {
            eq: lhs == rhs,
            ne: lhs != rhs,
        }
    }
}

impl<T, const N: usize> FromN<(T, T), N> for ClearEqNe
where
    T: tfhe::core_crypto::commons::numeric::Numeric,
{
    #[inline]
    fn from_n(n_values: &[(T, T); N]) -> Self {
        let eq = n_values.iter().all(|value| (value.0 == value.1));
        //let eq= n_values.iter().fold(true, |eq, value| eq && (value.0 == value.1));
        // let mut eq = true;
        // for i in 0..N {
        //     eq = eq && (n_values[i].0 == n_values[i].1);
        // }
        EqNe { eq, ne: !eq }
    }
}

derive2_encrypt_decrypt! { EqNe<T> {eq:T, ne:T} }

impl<B> HiLoLogicOp for EqNe<B>
where
    B: ThreadSafeBool,
{
    type BooleanType = B;

    /// Cost:
    /// -----
    /// - 1 x Bit And
    /// - 1 x Bit Or
    ///
    /// Optimisation:
    /// -------------
    /// None
    ///
    /// Formula:
    /// --------
    /// - eq = hi.eq & lo.eq
    /// - gt = hi.ne | lo.ne
    fn from_hi_any_lo_any(hi: &Self, lo: &Self) -> Self {
        let (eq, ne) = rayon::join(
            || hi.eq.refref_bitand(&lo.eq),
            || hi.ne.refref_bitor(&lo.ne),
        );
        EqNe::<B> { eq, ne }
    }

    /// Cost:
    /// -----
    /// - 1 x Bit And
    /// - 1 x Bit Or
    ///
    /// Optimisation:
    /// -------------
    /// None
    ///
    /// Formula:
    /// --------
    /// - eq = hi.eq & (lo == 0)
    /// - ne = hi.ne | (lo != 0)
    fn from_hi_any_lo_zero(hi: &EqNe<B>, lo_z: &ZeroMax<B>) -> Self {
        let (eq, ne) = rayon::join(
            || hi.eq.refref_bitand(&lo_z.is_zero.eq),
            || hi.ne.refref_bitor(&lo_z.is_zero.ne),
        );
        EqNe::<B> { eq, ne }
    }

    /// Cost:
    /// -----
    /// - 1 x Bit And
    /// - 1 x Bit Or
    ///
    /// Optimisation:
    /// -------------
    /// None
    ///
    /// Formula:
    /// --------
    /// - eq = hi.eq & (lo == MAX)
    /// - ne = hi.ne | (lo != MAX)
    fn from_hi_any_lo_max(hi: &Self, lo_mx: &ZeroMax<Self::BooleanType>) -> Self {
        let (eq, ne) = rayon::join(
            || hi.eq.refref_bitand(&lo_mx.is_max.eq),
            || hi.ne.refref_bitor(&lo_mx.is_max.ne),
        );
        EqNe::<B> { eq, ne }
    }

    /// Cost:
    /// -----
    /// - 1 x Bit And
    /// - 1 x Bit Or
    ///
    /// Optimisation:
    /// -------------
    /// None
    ///
    /// Formula:
    /// --------
    /// - eq = (hi == 0) & lo.eq
    /// - ne = (hi != 0) | lo.ne
    fn from_hi_zero_lo_any(hi_z: &ZeroMax<B>, lo: &EqNe<B>) -> Self {
        let (eq, ne) = rayon::join(
            || hi_z.is_zero.eq.refref_bitand(&lo.eq),
            || hi_z.is_zero.ne.refref_bitor(&lo.ne),
        );
        EqNe::<B> { eq, ne }
    }

    /// Cost:
    /// -----
    /// - 2 x Bit And
    ///
    /// Optimisation:
    /// -------------
    /// None
    ///
    /// Formula:
    /// --------
    /// - eq = (hi == MAX) & lo.eq
    /// - ne = (hi != MAX) | lo.ne
    fn from_hi_max_lo_any(hi_mx: &ZeroMax<B>, lo: &EqNe<B>) -> Self {
        let (eq, ne) = rayon::join(
            || hi_mx.is_max.eq.refref_bitand(&lo.eq),
            || hi_mx.is_max.ne.refref_bitor(&lo.ne),
        );
        EqNe::<B> { eq, ne }
    }

    /// Cost:
    /// -----
    /// - 1 x Bit And
    ///
    /// Optimisation:
    /// -------------
    /// None
    ///
    /// Formula:
    /// --------
    /// - eq = hi.eq & lo.eq
    /// - ne = hi.ne | lo.ne
    fn from_hi_zero_lo_max(hi_z: &ZeroMax<B>, lo_mx: &ZeroMax<B>) -> Self {
        let (eq, ne) = rayon::join(
            || hi_z.is_zero.eq.refref_bitand(&lo_mx.is_max.eq),
            || hi_z.is_zero.ne.refref_bitor(&lo_mx.is_max.ne),
        );
        EqNe::<B> { eq, ne }
    }

    /// Cost:
    /// -----
    /// - No cost
    ///
    /// Optimisation:
    /// -------------
    /// - hi == 0 + lo == 0 => num == 0
    ///
    /// Formula:
    /// --------
    /// - eq = num.is_zero.eq
    /// - nt = num.is_zero.ne
    fn from_hi_zero_lo_zero(z: &ZeroMax<Self::BooleanType>) -> Self {
        EqNe::<B> {
            eq: z.is_zero.eq.clone(),
            ne: z.is_zero.ne.clone(),
        }
    }

    /// Cost:
    /// -----
    /// - No cost
    ///
    /// Optimisation:
    /// -------------
    /// - hi == MAX + lo == MAX => num == MAX
    ///
    /// Formula:
    /// --------
    /// - eq = num.is_max.eq
    /// - ne = num.is_max.ne
    fn from_hi_max_lo_max(mx: &ZeroMax<B>) -> Self {
        EqNe::<B> {
            eq: mx.is_max.eq.clone(),
            ne: mx.is_max.ne.clone(),
        }
    }

    /// Cost:
    /// -----
    /// - 1 x Bit And
    /// - 1 x Bit Or
    ///
    /// Optimisation:
    /// -------------
    /// None
    ///
    /// Formula:
    /// --------
    /// - eq = (hi == MAX) & (lo == 0)
    /// - ne = (hi != MAX) | (lo != 0)
    fn from_hi_max_lo_zero(hi_mx: &ZeroMax<B>, lo_z: &ZeroMax<B>) -> Self {
        let (eq, ne) = rayon::join(
            || hi_mx.is_max.eq.refref_bitand(&lo_z.is_zero.eq),
            || hi_mx.is_max.ne.refref_bitor(&lo_z.is_zero.ne),
        );
        EqNe::<B> { eq, ne }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Bytes64EqNe
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct Bytes64EqNe<B> {
    // len = 8, in Little Endian Order
    pub le_bytes: U8EqNeMap<B>,
    // (8 x u8) + (4 x u16) + (2 x u32) + (1 x u64)
    pub zero_max: ZeroMaxPow2Array<B>,
}

derive2_encrypt_decrypt! { Bytes64EqNe<B> {le_bytes:U8BlockMap<EqNe<B>>, zero_max: UIntLeBlocksPow2Array<ZeroMax<B>>} }

pub type ClearBytes64EqNe = Bytes64EqNe<bool>;

////////////////////////////////////////////////////////////////////////////////

impl From<u64> for ClearBytes64EqNe {
    fn from(value: u64) -> Self {
        let le_bytes = U8EqNeMap::from_uint(value);
        let zero_max = ZeroMaxPow2Array::from(value);

        assert_eq!(le_bytes.count_blocks() as u32, u64::BITS / u8::BITS);
        assert_eq!(le_bytes.count_blocks() as usize, zero_max.bytes_len());

        Bytes64EqNe { le_bytes, zero_max }
    }
}

impl<B> MemoryCastInto<Bytes64EqGt<B>> for Bytes64EqNe<B> {
    fn mem_cast_into(self) -> Bytes64EqGt<B> {
        Bytes64EqGt::<B> {
            le_bytes: MemoryCastInto::<U8EqGtMap<B>>::mem_cast_into(self.le_bytes),
            zero_max: self.zero_max,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Bytes256EqNe
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq)]
pub struct Bytes256EqNe<B> {
    pub word0: Bytes64EqNe<B>,
    pub word1: Bytes64EqNe<B>,
    pub word2: Bytes64EqNe<B>,
    pub word3: Bytes64EqNe<B>,
}

impl From<&str> for ClearBytes256EqNe {
    fn from(value: &str) -> Self {
        let four_u64 = ascii_to_le_u64x4(value);
        Self::from(&four_u64)
    }
}

impl From<&[u64; 4]> for ClearBytes256EqNe {
    fn from(value: &[u64; 4]) -> Self {
        ClearBytes256EqNe {
            word0: Bytes64EqNe::from(value[0]),
            word1: Bytes64EqNe::from(value[1]),
            word2: Bytes64EqNe::from(value[2]),
            word3: Bytes64EqNe::from(value[3]),
        }
    }
}

derive4_encrypt_decrypt! { Bytes256EqNe<B> {word0: Bytes64EqNe<B>, word1: Bytes64EqNe<B>, word2: Bytes64EqNe<B>, word3: Bytes64EqNe<B>} }

pub type ClearBytes256EqNe = Bytes256EqNe<bool>;

impl<B> MemoryCastInto<Bytes256EqGt<B>> for Bytes256EqNe<B> {
    fn mem_cast_into(self) -> Bytes256EqGt<B> {
        Bytes256EqGt::<B> {
            word0: MemoryCastInto::<Bytes64EqGt<B>>::mem_cast_into(self.word0),
            word1: MemoryCastInto::<Bytes64EqGt<B>>::mem_cast_into(self.word1),
            word2: MemoryCastInto::<Bytes64EqGt<B>>::mem_cast_into(self.word2),
            word3: MemoryCastInto::<Bytes64EqGt<B>>::mem_cast_into(self.word3),
        }
    }
}
