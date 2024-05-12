////////////////////////////////////////////////////////////////////////////////
// UIntBlock (uxx, u8)
////////////////////////////////////////////////////////////////////////////////

pub type UIntBlockIndex = u8;
pub type UIntBlock<UInt> = (UInt, UIntBlockIndex);
pub type U8Block = UIntBlock<u8>;
pub type U16Block = UIntBlock<u16>;
pub type U32Block = UIntBlock<u32>;
pub type U64Block = UIntBlock<u64>;
pub type U64x4 = [u64;4];

#[inline]
pub fn le_block_index_to_hi_lo(le_block_index: UIntBlockIndex) -> (UIntBlockIndex, UIntBlockIndex) {
    // Blocks are ordered in little endian order
    (2 * le_block_index + 1, 2 * le_block_index)
}

////////////////////////////////////////////////////////////////////////////////
// Macros
////////////////////////////////////////////////////////////////////////////////

#[allow(unused_macros)]
macro_rules! blk_value {
    ($TheBlock:expr) => {
        $TheBlock.0
    };
}
#[allow(unused_imports)]
pub(crate) use blk_value;

#[allow(unused_macros)]
macro_rules! blk_index {
    ($TheBlock:expr) => {
        $TheBlock.1 as u8
    };
}
#[allow(unused_imports)]
pub(crate) use blk_index;

#[allow(unused_macros)]
macro_rules! block0 {
    ($le:tt) => {
        ($le[0], 0u8)
    };
}
#[allow(unused_macros)]
macro_rules! block1 {
    ($le:tt) => {
        ($le[1], 1u8)
    };
}
#[allow(unused_macros)]
macro_rules! block2 {
    ($le:tt) => {
        ($le[2], 2u8)
    };
}
#[allow(unused_macros)]
macro_rules! block3 {
    ($le:tt) => {
        ($le[3], 3u8)
    };
}
#[allow(unused_imports)]
pub(crate) use block0;
#[allow(unused_imports)]
pub(crate) use block1;
#[allow(unused_imports)]
pub(crate) use block2;
#[allow(unused_imports)]
pub(crate) use block3;
