use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::uint::maps::U8BlockMap;
use crate::uint::maps::UIntLeBlocksPow2Array;
use crate::types::*;
use std::fmt::Debug;

use super::eq_ne::{Bytes256EqNe, Bytes64EqNe, EqNe, U8EqNeMap};
use super::hi_lo_logic_op::HiLoEq;
use super::hi_lo_logic_op::HiLoLogicOp;
use super::zero_max::ZeroMax;
use super::zero_max::ZeroMaxPow2Array;

////////////////////////////////////////////////////////////////////////////////
// EqGt
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct EqGt<B> {
    pub eq: B,
    pub gt: B,
}

derive2_encrypt_decrypt! { EqGt<B> {eq:B, gt:B} }

pub type ClearEqGt = EqGt<bool>;

////////////////////////////////////////////////////////////////////////////////

pub type U8EqGtMap<B> = U8BlockMap<EqGt<B>>;
#[cfg(test)]
pub type ClearU8EqGtMap = U8EqGtMap<bool>;

////////////////////////////////////////////////////////////////////////////////

impl<B> std::fmt::Display for EqGt<B>
where
    B: DebugToString,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_fmt(format_args!(
            "[eq:{}, gt:{}]",
            self.eq.debug_to_string(),
            self.gt.debug_to_string()
        ))
    }
}

impl<T> MemoryCastInto<EqNe<T>> for EqGt<T> {
    fn mem_cast_into(self) -> EqNe<T> {
        EqNe::<T> {
            eq: self.eq,
            ne: self.gt,
        }
    }
}

impl<N> From<(N, N)> for ClearEqGt
where
    N: tfhe::core_crypto::commons::numeric::Numeric,
{
    #[inline]
    fn from((lhs, rhs): (N, N)) -> Self {
        EqGt {
            eq: lhs == rhs,
            gt: lhs > rhs,
        }
    }
}

impl<B> HiLoLogicOp for EqGt<B>
where
    B: ThreadSafeBool,
{
    type BooleanType = B;
    /// Cost:
    /// -----
    /// - 2 x Bit And
    /// - 1 x Bit Or
    ///
    /// Optimisation:
    /// -------------
    /// None
    ///
    /// Formula:
    /// --------
    /// - eq = hi.eq & lo.eq
    /// - gt = hi.gt | (hi.eq & lo.gt)
    fn from_hi_any_lo_any(hi: &EqGt<B>, lo: &EqGt<B>) -> Self {
        let (eq, gt) = rayon::join(
            || hi.eq.refref_bitand(&lo.eq),
            || hi.gt.ref_bitor(hi.eq.refref_bitand(&lo.gt)),
        );
        EqGt::<B> { eq, gt }
    }

    /// Cost:
    /// -----
    /// - 2 x Bit And
    /// - 1 x Bit Or
    ///
    /// Optimisation:
    /// -------------
    /// - lo == 0 => lo.gt = (lo != 0)
    ///
    /// Formula:
    /// --------
    /// - eq = hi.eq & (lo == 0)
    /// - gt = hi.gt | (hi.eq & (lo != 0))
    fn from_hi_any_lo_zero(hi: &EqGt<B>, lo_z: &ZeroMax<B>) -> Self {
        let (eq, gt) = rayon::join(
            || hi.eq.refref_bitand(&lo_z.is_zero.eq),
            || hi.gt.ref_bitor(hi.eq.refref_bitand(&lo_z.is_zero.ne)),
        );
        EqGt::<B> { eq, gt }
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
    /// - gt = hi.gt
    fn from_hi_any_lo_max(hi: &EqGt<B>, lo_mx: &ZeroMax<B>) -> Self {
        // Formula :
        // eq = hi.eq & lo.eq
        // gt = hi.gt | (hi.eq & FALSE) = hi.gt
        EqGt::<B> {
            eq: hi.eq.refref_bitand(&lo_mx.is_max.eq),
            gt: hi.gt.clone(),
        }
    }

    /// Cost:
    /// -----
    /// - 1 x Bit And
    /// - 1 x Bit Or
    ///
    /// Optimisation:
    /// -------------
    /// - hi == 0 => hi.gt = (hi != 0)
    ///
    /// Formula:
    /// --------
    /// - eq = (hi == 0) & lo.eq
    /// - gt = (hi != 0) | ((hi == 0) & lo.gt) => (hi != 0) | lo.gt
    fn from_hi_zero_lo_any(hi_z: &ZeroMax<B>, lo: &EqGt<B>) -> Self {
        let (eq, gt) = rayon::join(
            || hi_z.is_zero.eq.refref_bitand(&lo.eq),
            || hi_z.is_zero.ne.refref_bitor(&lo.gt),
        );
        EqGt::<B> { eq, gt }
    }

    /// Cost:
    /// -----
    /// - 2 x Bit And
    ///
    /// Optimisation:
    /// -------------
    /// - hi == MAX => hi.gt = FALSE
    ///
    /// Formula:
    /// --------
    /// - eq = (hi == MAX) & lo.eq
    /// - gt = (hi == MAX) & lo.gt
    fn from_hi_max_lo_any(hi_mx: &ZeroMax<B>, lo: &EqGt<B>) -> Self {
        let (eq, gt) = rayon::join(
            || hi_mx.is_max.eq.refref_bitand(&lo.eq),
            || hi_mx.is_max.eq.refref_bitand(&lo.gt),
        );
        EqGt::<B> { eq, gt }
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
    /// - gt = hi.gt
    fn from_hi_zero_lo_max(hi_z: &ZeroMax<B>, lo_mx: &ZeroMax<B>) -> Self {
        EqGt::<B> {
            eq: hi_z.is_zero.eq.refref_bitand(&lo_mx.is_max.eq),
            gt: hi_z.is_zero.ne.clone(),
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
    /// - gt = num.is_zero.ne
    fn from_hi_zero_lo_zero(z: &ZeroMax<B>) -> Self {
        EqGt::<B> {
            eq: z.is_zero.eq.clone(),
            gt: z.is_zero.ne.clone(),
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
    /// - gt = FALSE (there is no uint greater than MAX)
    fn from_hi_max_lo_max(mx: &ZeroMax<B>) -> Self {
        EqGt::<B> {
            eq: mx.is_max.eq.clone(),
            gt: B::get_false(),
        }
    }

    /// Cost:
    /// -----
    /// - 2 x Bit And
    ///
    /// Optimisation:
    /// -------------
    /// - hi == MAX => hi.gt = FALSE
    /// - lo == 0 => lo.gt = (lo != 0)
    ///
    /// Formula:
    /// --------
    /// - eq = (hi == MAX) & (lo == 0)
    /// - gt = (hi == MAX) & (lo != 0)
    fn from_hi_max_lo_zero(hi_mx: &ZeroMax<B>, lo_z: &ZeroMax<B>) -> Self {
        let (eq, gt) = rayon::join(
            || hi_mx.is_max.eq.refref_bitand(&lo_z.is_zero.eq),
            || hi_mx.is_max.eq.refref_bitand(&lo_z.is_zero.ne),
        );
        EqGt::<B> { eq, gt }
    }
}

impl<B> HiLoEq for EqGt<B>
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
    fn eq_hi_any_lo_any(hi: &EqGt<B>, lo: &EqGt<B>) -> Self {
        EqGt::<B> {
            eq: hi.eq.refref_bitand(&lo.eq),
            gt: B::get_false(),
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
    fn eq_hi_any_lo_zero(hi: &EqGt<B>, lo_z: &ZeroMax<B>) -> Self {
        EqGt::<B> {
            eq: hi.eq.refref_bitand(&lo_z.is_zero.eq),
            gt: B::get_false(),
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
    fn eq_hi_any_lo_max(hi: &EqGt<B>, lo_mx: &ZeroMax<B>) -> Self {
        EqGt::<B> {
            eq: hi.eq.refref_bitand(&lo_mx.is_max.eq),
            gt: B::get_false(),
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
    fn eq_hi_zero_lo_any(hi_z: &ZeroMax<B>, lo: &EqGt<B>) -> Self {
        EqGt::<B> {
            eq: hi_z.is_zero.eq.refref_bitand(&lo.eq),
            gt: B::get_false(),
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
    fn eq_hi_max_lo_any(hi_mx: &ZeroMax<B>, lo: &EqGt<B>) -> Self {
        EqGt::<B> {
            eq: hi_mx.is_max.eq.refref_bitand(&lo.eq),
            gt: B::get_false(),
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
    fn eq_hi_zero_lo_max(hi_z: &ZeroMax<B>, lo_mx: &ZeroMax<B>) -> Self {
        EqGt::<B> {
            eq: hi_z.is_zero.eq.refref_bitand(&lo_mx.is_max.eq),
            gt: B::get_false(),
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
    fn eq_hi_zero_lo_zero(z: &ZeroMax<B>) -> Self {
        EqGt::<B> {
            eq: z.is_zero.eq.clone(),
            gt: B::get_false(),
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
    fn eq_hi_max_lo_max(mx: &ZeroMax<B>) -> Self {
        EqGt::<B> {
            eq: mx.is_max.eq.clone(),
            gt: B::get_false(),
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
    fn eq_hi_max_lo_zero(hi_mx: &ZeroMax<B>, lo_z: &ZeroMax<B>) -> Self {
        EqGt::<B> {
            eq: hi_mx.is_max.eq.refref_bitand(&lo_z.is_zero.eq),
            gt: B::get_false(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Bytes64EqGt
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct Bytes64EqGt<B> {
    // len = 8, in Little Endian Order
    pub le_bytes: U8EqGtMap<B>,
    // (8 x u8) + (4 x u16) + (2 x u32) + (1 x u64)
    pub zero_max: ZeroMaxPow2Array<B>,
}

derive2_encrypt_decrypt! { Bytes64EqGt<B> {le_bytes:U8BlockMap<EqGt<B>>, zero_max: UIntLeBlocksPow2Array<ZeroMax<B>>} }

pub type ClearBytes64EqGt = Bytes64EqGt<bool>;

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
use {crate::uint::block::UIntBlockIndex, tfhe::core_crypto::commons::traits::Numeric};

#[cfg(test)]
impl<B> Bytes64EqGt<B> {
    #[inline]
    pub fn count_blocks<UInt: Numeric>(&self) -> UIntBlockIndex {
        ((self.le_bytes.count_blocks() as usize) / (UInt::BITS / 8)) as UIntBlockIndex
    }
}

////////////////////////////////////////////////////////////////////////////////

impl From<u64> for ClearBytes64EqGt {
    fn from(value: u64) -> Self {
        let le_bytes = U8EqGtMap::from_uint(value);
        let zero_max = ZeroMaxPow2Array::from(value);

        assert_eq!(le_bytes.count_blocks() as u32, u64::BITS / u8::BITS);
        assert_eq!(le_bytes.count_blocks() as usize, zero_max.bytes_len());

        Bytes64EqGt { le_bytes, zero_max }
    }
}

impl<B> MemoryCastInto<Bytes64EqNe<B>> for Bytes64EqGt<B> {
    fn mem_cast_into(self) -> Bytes64EqNe<B> {
        Bytes64EqNe::<B> {
            le_bytes: MemoryCastInto::<U8EqNeMap<B>>::mem_cast_into(self.le_bytes),
            zero_max: self.zero_max,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Bytes256EqGt
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq)]
pub struct Bytes256EqGt<B> {
    pub word0: Bytes64EqGt<B>,
    pub word1: Bytes64EqGt<B>,
    pub word2: Bytes64EqGt<B>,
    pub word3: Bytes64EqGt<B>,
}

derive4_encrypt_decrypt! { Bytes256EqGt<B> {word0: Bytes64EqGt<B>, word1: Bytes64EqGt<B>, word2: Bytes64EqGt<B>, word3: Bytes64EqGt<B>} }

pub type ClearBytes256EqGt = Bytes256EqGt<bool>;

////////////////////////////////////////////////////////////////////////////////

impl From<u8> for ClearBytes256EqGt {
    fn from(value: u8) -> Self {
        Self::from(&[value as u64, 0u64, 0u64, 0u64])
    }
}

impl From<u16> for ClearBytes256EqGt {
    fn from(value: u16) -> Self {
        Self::from(&[value as u64, 0u64, 0u64, 0u64])
    }
}

impl From<u32> for ClearBytes256EqGt {
    fn from(value: u32) -> Self {
        Self::from(&[value as u64, 0u64, 0u64, 0u64])
    }
}

impl From<u64> for ClearBytes256EqGt {
    fn from(value: u64) -> Self {
        Self::from(&[value, 0u64, 0u64, 0u64])
    }
}

impl From<&[u64; 4]> for ClearBytes256EqGt {
    fn from(value: &[u64; 4]) -> Self {
        ClearBytes256EqGt {
            word0: ClearBytes64EqGt::from(value[0]),
            word1: ClearBytes64EqGt::from(value[1]),
            word2: ClearBytes64EqGt::from(value[2]),
            word3: ClearBytes64EqGt::from(value[3]),
        }
    }
}

impl<B> MemoryCastInto<Bytes256EqNe<B>> for Bytes256EqGt<B> {
    fn mem_cast_into(self) -> Bytes256EqNe<B> {
        Bytes256EqNe::<B> {
            word0: MemoryCastInto::<Bytes64EqNe<B>>::mem_cast_into(self.word0),
            word1: MemoryCastInto::<Bytes64EqNe<B>>::mem_cast_into(self.word1),
            word2: MemoryCastInto::<Bytes64EqNe<B>>::mem_cast_into(self.word2),
            word3: MemoryCastInto::<Bytes64EqNe<B>>::mem_cast_into(self.word3),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Test
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
impl ClearU8EqGtMap {
    pub fn assert_valid_u8(&self, secret_u8: u8) {
        use crate::maps::UIntMap;
        self.iter().enumerate().for_each(|(block_index, u8_map)| {
            assert_eq!(block_index, 0);
            u8_map.key_value_iter().for_each(|(i, eq_gt)| {
                assert_eq!(eq_gt.eq, secret_u8 == i);
                assert_eq!(eq_gt.gt, secret_u8 > i);
            });
        });
    }

    pub fn assert_valid_u16(&self, secret_u16: u16) {
        use crate::{maps::UIntMap, uint::traits::ToU8};
        self.iter().enumerate().for_each(|(block_index, u8_map)| {
            assert!(block_index < 2);
            let secret_u8 = secret_u16.le_u8_at(block_index as u32);
            u8_map.key_value_iter().for_each(|(i, eq_gt)| {
                assert_eq!(eq_gt.eq, secret_u8 == i);
                assert_eq!(eq_gt.gt, secret_u8 > i);
            });
        });
    }

    pub fn assert_valid_u32(&self, secret_u32: u32) {
        use crate::{maps::UIntMap, uint::traits::ToU8};
        self.iter().enumerate().for_each(|(block_index, u8_map)| {
            assert!(block_index < 4);
            let secret_u8 = secret_u32.le_u8_at(block_index as u32);
            u8_map.key_value_iter().for_each(|(i, eq_gt)| {
                assert_eq!(eq_gt.eq, secret_u8 == i);
                assert_eq!(eq_gt.gt, secret_u8 > i);
            });
        });
    }

    pub fn assert_valid_u64(&self, secret_u64: u64) {
        use crate::{maps::UIntMap, uint::traits::ToU8};
        self.iter().enumerate().for_each(|(block_index, u8_map)| {
            assert!(block_index < 8);
            let secret_u8 = secret_u64.le_u8_at(block_index as u32);
            u8_map.key_value_iter().for_each(|(i, eq_gt)| {
                assert_eq!(eq_gt.eq, secret_u8 == i);
                assert_eq!(eq_gt.gt, secret_u8 > i);
            });
        });
    }
}

#[cfg(test)]
impl ClearBytes64EqGt {
    pub fn assert_valid_as_u64(&self, secret_u64: u64) {
        assert!(self.count_blocks::<u64>() == 1);
        assert!(self.zero_max.count_blocks(8) == 1);
        self.le_bytes.assert_valid_u64(secret_u64);
    }
}

#[cfg(test)]
impl ClearBytes256EqGt {
    fn assert_valid_as_u64(&self, secret_u64: u64) {
        self.word0.assert_valid_as_u64(secret_u64);
        self.word1.assert_valid_as_u64(0u64);
        self.word2.assert_valid_as_u64(0u64);
        self.word3.assert_valid_as_u64(0u64);
    }

    fn assert_valid_as_str(&self, secret_str: &str) {
        use crate::ascii::ascii_to_le_u64x4;
        let four_u64 = ascii_to_le_u64x4(secret_str);
        self.word0.assert_valid_as_u64(four_u64[0]);
        self.word1.assert_valid_as_u64(four_u64[1]);
        self.word2.assert_valid_as_u64(four_u64[2]);
        self.word3.assert_valid_as_u64(four_u64[3]);
    }
}

#[cfg(test)] 
mod test {
    use crate::ascii::{ascii_to_le_u64x4, rand_gen_ascii_32};

    use super::{ClearBytes256EqGt, ClearBytes64EqGt, ClearU8EqGtMap};
    use rand::Rng;

    #[test]
    fn test_clear_u8_eq_gt_map() {
        for secret_u8 in u8::MIN..u8::MAX {
            let b = ClearU8EqGtMap::from_uint::<u8, 1>(secret_u8);
            b.assert_valid_u8(secret_u8);
        }
        for secret_u16 in u16::MIN..u16::MAX {
            let b = ClearU8EqGtMap::from_uint::<u16, 2>(secret_u16);
            b.assert_valid_u16(secret_u16);
        }
        let mut rng = rand::thread_rng();
        for _ in 0..u16::MAX/2 {
            let rn_u32: u32 = rng.gen();
            let b = ClearU8EqGtMap::from_uint::<u32, 4>(rn_u32);
            b.assert_valid_u32(rn_u32);
        }
        for _ in 0..u16::MAX/2 {
            let rn_u64: u64 = rng.gen();
            let b = ClearU8EqGtMap::from_uint::<u64, 8>(rn_u64);
            b.assert_valid_u64(rn_u64);
        }
    }

    #[test]
    fn test_clear_bytes64_eq_gt_map() {
        let mut rng = rand::thread_rng();
        (0..u16::MAX/2).for_each(|_|{
            let rn_u64: u64 = rng.gen();
            let b = ClearBytes64EqGt::from(rn_u64);
            b.assert_valid_as_u64(rn_u64);
        })
    }

    #[test]
    fn test_clear_bytes256_eq_gt_map() {
        let mut rng = rand::thread_rng();
        (0..u16::MAX/2).for_each(|_|{
            let rn_u64: u64 = rng.gen();
            let b = ClearBytes256EqGt::from(rn_u64);
            b.assert_valid_as_u64(rn_u64);
        })
    }

    #[test]
    fn test_clear_bytes256_eq_gt_map_str() {
        let mut rng = rand::thread_rng();
        (0..u16::MAX/2).for_each(|_|{
            let rn_str = rand_gen_ascii_32(&mut rng);
            let b = ClearBytes256EqGt::from(&ascii_to_le_u64x4(&rn_str));
            b.assert_valid_as_str(&rn_str);
        })
    }
}