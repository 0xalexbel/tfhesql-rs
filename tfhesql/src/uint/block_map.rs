use std::fmt::Debug;

use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::types::MemoryCastInto;

use super::{
    block::{blk_index, blk_value, U8Block, UIntBlock, UIntBlockIndex},
    to_hilo::ToHiLo,
    to_uint::ToU8,
};
use crate::encrypt::derive1_encrypt_decrypt;
use crate::maps::*;

////////////////////////////////////////////////////////////////////////////////
// UIntBlockMap
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct UIntBlockMap<M> {
    le_blocks: Vec<M>,
}

pub type U8BlockMap<V> = UIntBlockMap<U8Map<V>>;
pub type U16BlockMap<V> = UIntBlockMap<U16Map<V>>;
pub type U32BlockMap<V> = UIntBlockMap<U32Map<V>>;
pub type U64BlockMap<V> = UIntBlockMap<U64Map<V>>;

////////////////////////////////////////////////////////////////////////////////

impl<M> UIntBlockMap<M> {
    pub fn new() -> Self {
        Self { le_blocks: vec![] }
    }

    pub fn from_le_blocks(le_blocks: Vec<M>) -> Self {
        Self { le_blocks }
    }
}

impl<M> UIntBlockMap<M>
where
    M: UIntMap,
{
    pub fn iter(&self) -> impl Iterator<Item = &M> {
        self.le_blocks.iter()
    }

    #[cfg(test)]
    pub fn count_mem(&self) -> usize {
        self.le_blocks.iter().fold(0, |acc, e| acc + e.len())
    }

    #[inline]
    pub fn get_at_unchecked(&self, uint_block: UIntBlock<M::UIntType>) -> &M::ValueType {
        self.le_blocks[blk_index!(uint_block) as usize]
            .get(blk_value!(uint_block))
            .unwrap()
    }

    #[inline]
    pub fn get_at(&self, uint_block: UIntBlock<M::UIntType>) -> Option<&M::ValueType> {
        self.le_blocks.get(blk_index!(uint_block) as usize)?.get(blk_value!(uint_block))
    }

    pub fn get_hi_lo_at<UIntPow2: ToHiLo<HiLoType = M::UIntType>>(
        &self,
        uint_pow2_block: UIntBlock<UIntPow2>,
    ) -> (Option<&M::ValueType>, Option<&M::ValueType>) {
        let [hi_uint_block, lo_uint_block] = uint_pow2_block.to_hi_lo();

        if blk_index!(hi_uint_block) >= self.count_blocks() {
            if blk_index!(lo_uint_block) >= self.count_blocks() {
                (None, None)
            } else {
                (None, Some(self.get_at_unchecked(lo_uint_block)))
            }
        } else {
            assert!(blk_index!(lo_uint_block) < self.count_blocks());
            (
                Some(self.get_at_unchecked(hi_uint_block)),
                Some(self.get_at_unchecked(lo_uint_block)),
            )
        }
    }

    #[inline]
    pub fn count_blocks(&self) -> UIntBlockIndex {
        self.le_blocks.len() as UIntBlockIndex
    }

    #[inline]
    pub fn contains_at(&self, uint_block: UIntBlock<M::UIntType>) -> bool {
        if blk_index!(uint_block) as usize >= self.le_blocks.len() {
            return false;
        }
        self.le_blocks[blk_index!(uint_block) as usize].contains_key(blk_value!(uint_block))
    }

    // pub fn contains_hi_lo_at<UIntPow2: ToHiLo<HiLoType = M::UIntType>>(
    //     &self,
    //     uint_pow2_block: UIntBlock<UIntPow2>,
    // ) -> bool {
    //     let [hi_uint_block, lo_uint_block] = uint_pow2_block.to_hi_lo();
    //     self.contains_at(hi_uint_block) && self.contains_at(lo_uint_block)
    // }
}

impl<M> UIntBlockMap<M>
where
    M: UIntMapMut,
    <M as UIntMap>::ValueType: Clone,
{
    fn alloc_block(&mut self, block_index: UIntBlockIndex) {
        let bi = block_index as usize;
        let len = self.le_blocks.len();
        if len <= bi {
            let missing = bi - len + 1;
            self.le_blocks.extend((0..missing).map(|_| M::new()));
        }
    }

    pub fn insert_at(&mut self, uint_block: UIntBlock<M::UIntType>, value: &M::ValueType) {
        self.alloc_block(blk_index!(uint_block));
        self.le_blocks[blk_index!(uint_block) as usize]
            .insert(blk_value!(uint_block), value.clone());
    }
}

////////////////////////////////////////////////////////////////////////////////
// U8BlockMap
////////////////////////////////////////////////////////////////////////////////

derive1_encrypt_decrypt! { U8BlockMap<V> {le_blocks:Vec<U8Map<V>>} }

impl<V> U8BlockMap<V>
where
    V: From<U8Block> + Clone,
{
    #[inline]
    pub fn from_uint<UInt: ToU8<N>, const N: usize>(value: UInt) -> Self {
        let le_blocks: Vec<U8Map<V>> = value
            .to_le_u8()
            .iter()
            .map(|x| U8Map::<V>::from(*x))
            .collect();
        U8BlockMap::from_le_blocks(le_blocks)
    }
}

impl<V, U> MemoryCastInto<U8BlockMap<U>> for U8BlockMap<V>
where
    V: MemoryCastInto<U>,
{
    fn mem_cast_into(self) -> U8BlockMap<U> {
        U8BlockMap::<U> {
            le_blocks: self
                .le_blocks
                .into_iter()
                .map(MemoryCastInto::<U8Map<U>>::mem_cast_into)
                .collect(),
        }
    }
}

