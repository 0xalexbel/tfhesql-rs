use std::fmt::Debug;

use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::maps::U8Map;
use crate::uint::maps::U8BlockMap;
use crate::types::*;

use super::eq_gt::Bytes64EqGt;
use super::eq_gt::EqGt;
use super::eq_gt::U8EqGtMap;
use super::eq_ne::Bytes64EqNe;
use super::eq_ne::EqNe;
use super::eq_ne::U8EqNeMap;
use super::hi_lo_logic_op::HiLoLogicOp;
use super::zero_max::ZeroMax;
use super::zero_max::ZeroMaxPow2Array;

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Equ<B> {
    eq: B,
}

impl<B> Equ<B> {
    #[inline]
    pub fn eq(&self) -> &B {
        &self.eq
    }
}

impl<B> Default for Equ<B>
where
    B: Default,
{
    fn default() -> Self {
        Self {
            eq: Default::default(),
        }
    }
}

impl<B> std::fmt::Display for Equ<B>
where
    B: DebugToString,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_fmt(format_args!("[eq:{}]", self.eq.debug_to_string()))
    }
}

pub type ClearEqu = Equ<bool>;

pub type U8EquMap<B> = U8BlockMap<Equ<B>>;

impl<T> From<(T, T)> for ClearEqu
where
    T: tfhe::core_crypto::commons::numeric::Numeric,
{
    #[inline]
    fn from((lhs, rhs): (T, T)) -> Self {
        Equ { eq: lhs == rhs }
    }
}

impl<B> From<EqGt<B>> for Equ<B> {
    #[inline]
    fn from(value: EqGt<B>) -> Self {
        Equ { eq: value.eq }
    }
}

impl<B> From<&EqGt<B>> for Equ<B>
where
    B: Clone,
{
    #[inline]
    fn from(value: &EqGt<B>) -> Self {
        Equ {
            eq: value.eq.clone(),
        }
    }
}

derive1_encrypt_decrypt! { Equ<B> {eq:B} }

impl<B> HiLoLogicOp for Equ<B>
where
    B: ThreadSafeBool,
{
    type BooleanType = B;
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
    fn from_hi_any_lo_any(hi: &Equ<B>, lo: &Equ<B>) -> Self {
        Equ::<B> {
            eq: hi.eq.refref_bitand(&lo.eq),
        }
    }

    /// Cost:
    /// -----
    /// - 1 x Bit And
    ///
    /// Optimisation:
    /// -------------
    /// - lo == 0 => lo.gt = (lo != 0)
    ///
    /// Formula:
    /// --------
    /// - eq = hi.eq & (lo == 0)
    fn from_hi_any_lo_zero(hi: &Equ<B>, lo_z: &ZeroMax<B>) -> Self {
        Equ::<B> {
            eq: hi.eq.refref_bitand(&lo_z.is_zero.eq),
        }
    }

    /// Cost:
    /// -----
    /// - 1 x Bit And
    ///
    /// Optimisation:
    /// -------------
    /// - lo == MAX => lo.gt = FALSE
    ///
    /// Formula:
    /// --------
    /// - eq = hi.eq & (lo == MAX)
    fn from_hi_any_lo_max(hi: &Equ<B>, lo_mx: &ZeroMax<B>) -> Self {
        Equ::<B> {
            eq: hi.eq.refref_bitand(&lo_mx.is_max.eq),
        }
    }

    /// Cost:
    /// -----
    /// - 1 x Bit And
    ///
    /// Optimisation:
    /// -------------
    /// - hi == 0 => hi.gt = (hi != 0)
    ///
    /// Formula:
    /// --------
    /// - eq = (hi == 0) & lo.eq
    fn from_hi_zero_lo_any(hi_z: &ZeroMax<B>, lo: &Equ<B>) -> Self {
        Equ::<B> {
            eq: hi_z.is_zero.eq.refref_bitand(&lo.eq),
        }
    }

    /// Cost:
    /// -----
    /// - 1 x Bit And
    ///
    /// Optimisation:
    /// -------------
    /// - hi == MAX => hi.gt = FALSE
    ///
    /// Formula:
    /// --------
    /// - eq = (hi == MAX) & lo.eq
    fn from_hi_max_lo_any(hi_mx: &ZeroMax<B>, lo: &Equ<B>) -> Self {
        Equ::<B> {
            eq: hi_mx.is_max.eq.refref_bitand(&lo.eq),
        }
    }

    /// Cost:
    /// -----
    /// - 1 x Bit And
    ///
    /// Optimisation:
    /// -------------
    /// - hi == 0 => hi.gt = (hi != 0)
    /// - lo == MAX => lo.gt == FALSE
    ///
    /// Formula:
    /// --------
    /// - eq = hi.eq & lo.eq
    fn from_hi_zero_lo_max(hi_z: &ZeroMax<B>, lo_mx: &ZeroMax<B>) -> Self {
        Equ::<B> {
            eq: hi_z.is_zero.eq.refref_bitand(&lo_mx.is_max.eq),
        }
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
    fn from_hi_zero_lo_zero(z: &ZeroMax<B>) -> Self {
        Equ::<B> {
            eq: z.is_zero.eq.clone(),
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
    fn from_hi_max_lo_max(mx: &ZeroMax<B>) -> Self {
        Equ::<B> {
            eq: mx.is_max.eq.clone(),
        }
    }

    /// Cost:
    /// -----
    /// - 1 x Bit And
    ///
    /// Optimisation:
    /// -------------
    /// - hi == MAX => hi.gt = FALSE
    ///
    /// Formula:
    /// --------
    /// - eq = (hi == MAX) & (lo == 0)
    fn from_hi_max_lo_zero(hi_mx: &ZeroMax<B>, lo_z: &ZeroMax<B>) -> Self {
        Equ::<B> {
            eq: hi_mx.is_max.eq.refref_bitand(&lo_z.is_zero.eq),
        }
    }
}

impl<B> From<&U8Map<EqGt<B>>> for U8Map<Equ<B>>
where
    B: Clone,
{
    fn from(value: &U8Map<EqGt<B>>) -> Self {
        U8Map::<Equ<B>>::from_values(
            value
                .values()
                .iter()
                .map(|x| Equ { eq: x.eq.clone() })
                .collect(),
        )
    }
}

impl<B> From<&U8Map<EqNe<B>>> for U8Map<Equ<B>>
where
    B: Clone,
{
    fn from(value: &U8Map<EqNe<B>>) -> Self {
        U8Map::<Equ<B>>::from_values(
            value
                .values()
                .iter()
                .map(|x| Equ { eq: x.eq.clone() })
                .collect(),
        )
    }
}

impl<B> From<&U8EqGtMap<B>> for U8EquMap<B>
where
    B: Clone,
{
    fn from(value: &U8EqGtMap<B>) -> Self {
        U8EquMap::<B>::from_le_blocks(value.iter().map(U8Map::<Equ<B>>::from).collect())
    }
}

impl<B> From<&U8EqNeMap<B>> for U8EquMap<B>
where
    B: Clone,
{
    fn from(value: &U8EqNeMap<B>) -> Self {
        U8EquMap::<B>::from_le_blocks(value.iter().map(U8Map::<Equ<B>>::from).collect())
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Bytes64Equ<B> {
    // len = 8, in Little Endian Order
    pub le_bytes: U8EquMap<B>,
    // (8 x u8) + (4 x u16) + (2 x u32) + (1 x u64)
    pub zero_max: ZeroMaxPow2Array<B>,
}

impl<B> From<&Bytes64EqGt<B>> for Bytes64Equ<B>
where
    B: Clone,
{
    fn from(value: &Bytes64EqGt<B>) -> Self {
        Bytes64Equ::<B> {
            le_bytes: U8EquMap::<B>::from(&value.le_bytes),
            zero_max: value.zero_max.clone(),
        }
    }
}

impl<B> From<&Bytes64EqNe<B>> for Bytes64Equ<B>
where
    B: Clone,
{
    fn from(value: &Bytes64EqNe<B>) -> Self {
        Bytes64Equ::<B> {
            le_bytes: U8EquMap::<B>::from(&value.le_bytes),
            zero_max: value.zero_max.clone(),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct Bytes256Equ<B> {
    pub word0: Bytes64Equ<B>,
    pub word1: Bytes64Equ<B>,
    pub word2: Bytes64Equ<B>,
    pub word3: Bytes64Equ<B>,
}
