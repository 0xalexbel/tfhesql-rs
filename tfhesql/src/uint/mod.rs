pub mod block;
mod block_iter;
mod block_pow2_array;
mod byte_array;

pub use byte_array::ByteArray;
pub use byte_array::ClearByteArray;
pub use byte_array::ClearByteArrayList;

mod byte_mask;
pub mod mask {
    pub use super::byte_mask::SizedMask;
    pub use super::byte_mask::Mask;
    //pub use super::byte_mask::MaskMatrix;
    pub use super::byte_mask::BoolMask;
    pub use super::byte_mask::ClearBoolMask;
    pub use super::byte_mask::ByteMask;
    pub use super::byte_mask::ByteMaskMatrix;
    #[allow(unused_imports)]
    pub use super::byte_mask::ClearByteMask;
    #[allow(unused_imports)]
    pub use super::byte_mask::ClearByteMaskMatrix;
}

pub mod interval;
pub mod signed_u64;
pub mod to_hilo;
mod to_uint;
pub mod triangular_matrix;
mod block_map;

pub mod iter {
    pub use super::block_iter::LeU16BlockIterator;
    pub use super::block_iter::LeU32BlockIterator;
    pub use super::block_iter::LeU64BlockIterator;
    pub use super::block_iter::LeU64x4Iterator;
    pub use super::block_iter::AsciiU16BlockIterator;
    pub use super::block_iter::AsciiU32BlockIterator;
    pub use super::block_iter::AsciiU64BlockIterator;
}

pub mod traits {
    #[cfg(test)]
    pub use super::to_uint::ToLeU8;
    pub use super::to_uint::ToU16;
    pub use super::to_uint::ToU32;
    pub use super::to_uint::ToU64;
    pub use super::to_uint::ToU8;
}

pub mod maps {
    pub use super::block_map::U16BlockMap;
    pub use super::block_map::U32BlockMap;
    pub use super::block_map::U64BlockMap;
    pub use super::block_map::U8BlockMap;
    pub use super::block_map::UIntBlockMap;
    pub use super::block_pow2_array::UIntLeBlocksPow2Array;
}

mod pow2;
pub use pow2::*;

pub trait FromN<T, const N:usize> {
    fn from_n(n_values: &[T; N]) -> Self;
}
