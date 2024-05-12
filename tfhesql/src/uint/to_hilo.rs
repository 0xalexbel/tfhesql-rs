use super::block::{blk_index, blk_value, le_block_index_to_hi_lo, UIntBlock};

////////////////////////////////////////////////////////////////////////////////
// ToHiLo
////////////////////////////////////////////////////////////////////////////////

pub trait ToHiLo {
    type HiLoType;
    fn to_hi_lo(self) -> [Self::HiLoType; 2];
}

////////////////////////////////////////////////////////////////////////////////

impl ToHiLo for u16 {
    type HiLoType = u8;
    fn to_hi_lo(self) -> [u8; 2] {
        [(self >> u8::BITS) as u8, self as u8]
    }
}
impl ToHiLo for u32 {
    type HiLoType = u16;
    fn to_hi_lo(self) -> [u16; 2] {
        [
            (self >> u16::BITS) as u16,
            self as u16,
        ]
    }
}
impl ToHiLo for u64 {
    type HiLoType = u32;
    fn to_hi_lo(self) -> [u32; 2] {
        [
            (self >> u32::BITS) as u32,
            self as u32,
        ]
    }
}

impl<UInt> ToHiLo for UIntBlock<UInt>
where
    UInt: ToHiLo,
{
    type HiLoType = UIntBlock<<UInt as ToHiLo>::HiLoType>;
    #[inline]
    fn to_hi_lo(self) -> [Self::HiLoType; 2] {
        // Blocks are ordered in little endian order
        let (hi_le_block_index, lo_le_block_index) = le_block_index_to_hi_lo(blk_index!(self));

        let [hi, lo] = blk_value!(self).to_hi_lo();
        [(hi, hi_le_block_index), (lo, lo_le_block_index)]
    }
}
