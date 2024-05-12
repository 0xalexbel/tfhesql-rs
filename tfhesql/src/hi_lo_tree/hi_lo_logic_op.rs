use crate::maps::UIntMap;
use crate::maps::UIntMapMut;
use crate::types::DebugToString;
use crate::types::ThreadSafeBool;
use crate::uint::block::blk_index;
use crate::uint::block::blk_value;
use crate::uint::block::UIntBlock;
use crate::uint::maps::UIntBlockMap;
use crate::uint::to_hilo::ToHiLo;
#[cfg(feature = "parallel")]
use rayon::iter::*;
use tfhe::core_crypto::commons::traits::Numeric;

use super::zero_max::{ZeroMax, ZeroMaxPow2Array};

////////////////////////////////////////////////////////////////////////////////
// HiLoLogicOp
////////////////////////////////////////////////////////////////////////////////

pub trait HiLoLogicOp: Sized {
    type BooleanType: DebugToString;

    fn from_hi_any_lo_any(hi: &Self, lo: &Self) -> Self;
    fn from_hi_any_lo_zero(hi: &Self, lo_z: &ZeroMax<Self::BooleanType>) -> Self;
    fn from_hi_any_lo_max(hi: &Self, lo_mx: &ZeroMax<Self::BooleanType>) -> Self;
    fn from_hi_zero_lo_any(hi_z: &ZeroMax<Self::BooleanType>, lo: &Self) -> Self;
    fn from_hi_max_lo_any(hi_mx: &ZeroMax<Self::BooleanType>, lo: &Self) -> Self;
    fn from_hi_zero_lo_max(
        hi_z: &ZeroMax<Self::BooleanType>,
        lo_mx: &ZeroMax<Self::BooleanType>,
    ) -> Self;
    fn from_hi_zero_lo_zero(z: &ZeroMax<Self::BooleanType>) -> Self;
    fn from_hi_max_lo_max(mx: &ZeroMax<Self::BooleanType>) -> Self;
    fn from_hi_max_lo_zero(
        hi_mx: &ZeroMax<Self::BooleanType>,
        lo_z: &ZeroMax<Self::BooleanType>,
    ) -> Self;

    fn from_hi_lo<UInt: Numeric>(
        [hi, lo]: [UInt; 2],
        zm: &ZeroMax<Self::BooleanType>,
        (hi_zm, lo_zm): (&ZeroMax<Self::BooleanType>, &ZeroMax<Self::BooleanType>),
        (hi_op, lo_op): (Option<&Self>, Option<&Self>),
    ) -> Self {
        let hi = match hi_op {
            Some(_) => hi,
            None => UInt::ZERO,
        };
        let lo = match lo_op {
            Some(_) => lo,
            None => UInt::ZERO,
        };
        if hi == UInt::ZERO {
            if lo == UInt::ZERO {
                // (hi, lo) == (0, 0) <=> value = 0
                Self::from_hi_zero_lo_zero(zm)
            } else if lo == UInt::MAX {
                // (hi, lo) == (0, MAX)
                Self::from_hi_zero_lo_max(hi_zm, lo_zm)
            } else {
                // (hi, lo) == (0, lo)
                Self::from_hi_zero_lo_any(hi_zm, lo_op.unwrap())
            }
        } else if hi == UInt::MAX {
            if lo == UInt::ZERO {
                // (hi, lo) == (MAX, 0)
                Self::from_hi_max_lo_zero(hi_zm, lo_zm)
            } else if lo == UInt::MAX {
                // (hi, lo) == (MAX, MAX) <=> value = MAX
                Self::from_hi_max_lo_max(zm)
            } else {
                // (hi, lo) == (MAX, lo)
                Self::from_hi_max_lo_any(hi_zm, lo_op.unwrap())
            }
        } else if lo == UInt::ZERO {
            // (hi, lo) == (hi, 0)
            Self::from_hi_any_lo_zero(hi_op.unwrap(), lo_zm)
        } else if lo == UInt::MAX {
            // (hi, lo) == (hi, MAX)
            Self::from_hi_any_lo_max(hi_op.unwrap(), lo_zm)
        } else {
            // (hi, lo) == (hi, lo)
            Self::from_hi_any_lo_any(hi_op.unwrap(), lo_op.unwrap())
        }
    }

    fn from_uint_block<UInt, M>(
        uint_block: UIntBlock<UInt>,
        hi_lo_block_map: &UIntBlockMap<M>,
        zero_max: &ZeroMaxPow2Array<Self::BooleanType>,
    ) -> Self
    where
        UInt: ToHiLo + Numeric,
        <UInt as ToHiLo>::HiLoType: Numeric,
        M: UIntMap<UIntType = <UInt as ToHiLo>::HiLoType, ValueType = Self>,
    {
        let hi_lo_uints = blk_value!(uint_block).to_hi_lo();
        let zm = zero_max.le_uint_at(blk_index!(uint_block), (UInt::BITS / 8) as u8);
        let hi_lo_zm = zero_max.le_hi_lo_at(blk_index!(uint_block), (UInt::BITS / 8) as u8);
        let hi_lo_blocks = hi_lo_block_map.get_hi_lo_at(uint_block);
        Self::from_hi_lo(hi_lo_uints, zm, hi_lo_zm, hi_lo_blocks)
    }

    #[cfg(feature = "parallel")]
    fn compute_and_insert_into<UInt, UIntM, UIntM2, HiLoM>(
        uint_block_vec: &Vec<UIntBlock<UInt>>,
        uint_block_map: &mut UIntBlockMap<UIntM>,
        hi_lo_map: &UIntBlockMap<HiLoM>,
        zero_max: &ZeroMaxPow2Array<Self::BooleanType>,
        buffer: &mut Vec<Self>,
        other_uint_block_map: Option<&UIntBlockMap<UIntM2>>,
    ) where
        Self: Clone + Send,
        Self::BooleanType: ThreadSafeBool,
        UInt: ToHiLo + Numeric,
        <UInt as ToHiLo>::HiLoType: Numeric,
        UIntM: UIntMapMut<UIntType = UInt, ValueType = Self>,
        UIntM2: UIntMap<UIntType = UInt> + Sync,
        <UIntM2 as UIntMap>::ValueType: Clone,
        Self: From<<UIntM2 as UIntMap>::ValueType>,
        HiLoM: UIntMap<UIntType = <UInt as ToHiLo>::HiLoType, ValueType = Self> + Sync,
    {
        assert!(buffer.len() >= uint_block_vec.len());

        // Parallel compute blocks using Hi-Lo blocks
        buffer
            .par_iter_mut()
            .zip(uint_block_vec.par_iter())
            .for_each(|(dst, uint_block)| {
                // Maybe the block is already computed in the other tree ??
                match other_uint_block_map {
                    Some(m) => {
                        match m.get_at(*uint_block) {
                            Some(b) => {
                                // Convert + Clone
                                *dst = Self::from(b.clone());
                            }
                            None => {
                                // Costly computation
                                *dst = Self::from_uint_block(*uint_block, hi_lo_map, zero_max);
                            }
                        }
                    }
                    None => {
                        // Costly computation
                        *dst = Self::from_uint_block(*uint_block, hi_lo_map, zero_max);
                    }
                }
            });

        // In the main thread
        // fill the destination map with the newly computed blocks
        buffer
            .iter()
            .zip(uint_block_vec.iter())
            .for_each(|(value, uint_block)| {
                uint_block_map.insert_at(*uint_block, value);
            });
    }

    #[cfg(not(feature = "parallel"))]
    fn compute_and_insert_into<UInt, UIntM, UIntM2, HiLoM>(
        uint_block_vec: &Vec<UIntBlock<UInt>>,
        uint_block_map: &mut UIntBlockMap<UIntM>,
        hi_lo_map: &UIntBlockMap<HiLoM>,
        zero_max: &ZeroMaxPow2Array<Self::BooleanType>,
        buffer: &mut Vec<Self>,
        other_uint_block_map: Option<&UIntBlockMap<UIntM2>>,
    ) where
        Self: Clone + Send,
        Self::BooleanType: ThreadSafeBool,
        UInt: ToHiLo + Numeric,
        <UInt as ToHiLo>::HiLoType: Numeric,
        UIntM: UIntMapMut<UIntType = UInt, ValueType = Self>,
        UIntM2: UIntMap<UIntType = UInt> + Sync,
        <UIntM2 as UIntMap>::ValueType: Clone,
        Self: From<<UIntM2 as UIntMap>::ValueType>,
        HiLoM: UIntMap<UIntType = <UInt as ToHiLo>::HiLoType, ValueType = Self> + Sync,
    {
        assert!(buffer.len() >= uint_block_vec.len());

        // Parallel compute blocks using Hi-Lo blocks
        buffer
            .iter_mut()
            .zip(uint_block_vec.iter())
            .for_each(|(dst, uint_block)| {
                // Maybe the block is already computed in the other tree ??
                match other_uint_block_map {
                    Some(m) => {
                        match m.get_at(*uint_block) {
                            Some(b) => {
                                // Convert + Clone
                                *dst = Self::from(b.clone());
                            }
                            None => {
                                // Costly computation
                                *dst = Self::from_uint_block(*uint_block, hi_lo_map, zero_max);
                            }
                        }
                    }
                    None => {
                        // Costly computation
                        *dst = Self::from_uint_block(*uint_block, hi_lo_map, zero_max);
                    }
                }
            });

        // In the main thread
        // fill the destination map with the newly computed blocks
        buffer
            .iter()
            .zip(uint_block_vec.iter())
            .for_each(|(value, uint_block)| {
                uint_block_map.insert_at(*uint_block, value);
            });
    }
}

////////////////////////////////////////////////////////////////////////////////
// HiLoEq
////////////////////////////////////////////////////////////////////////////////

pub trait HiLoEq: Sized {
    type BooleanType: DebugToString;

    fn eq_hi_any_lo_any(hi: &Self, lo: &Self) -> Self;
    fn eq_hi_any_lo_zero(hi: &Self, lo_z: &ZeroMax<Self::BooleanType>) -> Self;
    fn eq_hi_any_lo_max(hi: &Self, lo_mx: &ZeroMax<Self::BooleanType>) -> Self;
    fn eq_hi_zero_lo_any(hi_z: &ZeroMax<Self::BooleanType>, lo: &Self) -> Self;
    fn eq_hi_max_lo_any(hi_mx: &ZeroMax<Self::BooleanType>, lo: &Self) -> Self;
    fn eq_hi_zero_lo_max(
        hi_z: &ZeroMax<Self::BooleanType>,
        lo_mx: &ZeroMax<Self::BooleanType>,
    ) -> Self;
    fn eq_hi_zero_lo_zero(z: &ZeroMax<Self::BooleanType>) -> Self;
    fn eq_hi_max_lo_max(mx: &ZeroMax<Self::BooleanType>) -> Self;
    fn eq_hi_max_lo_zero(
        hi_mx: &ZeroMax<Self::BooleanType>,
        lo_z: &ZeroMax<Self::BooleanType>,
    ) -> Self;

    fn eq_hi_lo<UInt: Numeric>(
        [hi, lo]: [UInt; 2],
        zm: &ZeroMax<Self::BooleanType>,
        (hi_zm, lo_zm): (&ZeroMax<Self::BooleanType>, &ZeroMax<Self::BooleanType>),
        (hi_op, lo_op): (&Self, &Self),
    ) -> Self {
        if hi == UInt::ZERO {
            if lo == UInt::ZERO {
                // (hi, lo) == (0, 0) <=> value = 0
                Self::eq_hi_zero_lo_zero(zm)
            } else if lo == UInt::MAX {
                // (hi, lo) == (0, MAX)
                Self::eq_hi_zero_lo_max(hi_zm, lo_zm)
            } else {
                // (hi, lo) == (0, lo)
                Self::eq_hi_zero_lo_any(hi_zm, lo_op)
            }
        } else if hi == UInt::MAX {
            if lo == UInt::ZERO {
                // (hi, lo) == (MAX, 0)
                Self::eq_hi_max_lo_zero(hi_zm, lo_zm)
            } else if lo == UInt::MAX {
                // (hi, lo) == (MAX, MAX) <=> value = MAX
                Self::eq_hi_max_lo_max(zm)
            } else {
                // (hi, lo) == (MAX, lo)
                Self::eq_hi_max_lo_any(hi_zm, lo_op)
            }
        } else if lo == UInt::ZERO {
            // (hi, lo) == (hi, 0)
            Self::eq_hi_any_lo_zero(hi_op, lo_zm)
        } else if lo == UInt::MAX {
            // (hi, lo) == (hi, MAX)
            Self::eq_hi_any_lo_max(hi_op, lo_zm)
        } else {
            // (hi, lo) == (hi, lo)
            Self::eq_hi_any_lo_any(hi_op, lo_op)
        }
    }
}
