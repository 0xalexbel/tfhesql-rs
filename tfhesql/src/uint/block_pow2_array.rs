use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::uint::block::le_block_index_to_hi_lo;
use crate::uint::FromN;

use super::to_uint::*;
use super::block::UIntBlockIndex;

////////////////////////////////////////////////////////////////////////////////
// UIntLeBlocksPow2Array
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct UIntLeBlocksPow2Array<T>(pub Vec<T>);

derive_wrapper_encrypt_decrypt! { UIntLeBlocksPow2Array<T> (Vec<T>) }

////////////////////////////////////////////////////////////////////////////////

impl<T> UIntLeBlocksPow2Array<T> {
    pub fn drop(&mut self) {
        self.0 = vec![];
    }
}

impl<T> From<u8> for UIntLeBlocksPow2Array<T>
where
    T: From<u8>,
{
    // Len == 1
    // [T(u8)]
    fn from(value: u8) -> Self {
        UIntLeBlocksPow2Array::<T>(vec![T::from(value)])
    }
}

impl<T> From<u16> for UIntLeBlocksPow2Array<T>
where
    T: From<u16> + From<u8>,
{
    // Len == 2 + 1
    // [T(u8(lo)), T(u8(hi)), T(u16)]
    fn from(value: u16) -> Self {
        let mut v: Vec<T> = vec![];
        // u8 in le order
        for le_u8 in value.to_le_u8() {
            v.push(T::from(le_u8));
        }
        // u16
        v.push(T::from(value));
        UIntLeBlocksPow2Array::<T>(v)
    }
}

impl<T> From<u32> for UIntLeBlocksPow2Array<T>
where
    T: From<u32> + From<u16> + From<u8>,
{
    // Len == 4 + 2 + 1
    // [T(u8(0)), T(u8(1)), T(u8(2)), T(u8(3)), T(u16(0)), T(u16(1)), T(u32)]
    fn from(value: u32) -> Self {
        let mut v: Vec<T> = vec![];
        // u8 in le order
        for le_u8 in value.to_le_u8() {
            v.push(T::from(le_u8));
        }
        // u16 in le order
        for le_u16 in value.to_le_u16() {
            v.push(T::from(le_u16));
        }
        // u32
        v.push(T::from(value));
        UIntLeBlocksPow2Array::<T>(v)
    }
}

impl<T> From<u64> for UIntLeBlocksPow2Array<T>
where
    T: From<u64> + From<u32> + From<u16> + From<u8>,
{
    // Len == 8 + 4 + 2 + 1
    // [T(u8(0)), T(u8(1)), ..., T(u64)]
    fn from(value: u64) -> Self {
        let mut v: Vec<T> = vec![];
        // u8 in le order
        for le_u8 in value.to_le_u8() {
            v.push(T::from(le_u8));
        }
        // u16 in le order
        for le_u16 in value.to_le_u16() {
            v.push(T::from(le_u16));
        }
        // u32 in le order
        for le_u32 in value.to_le_u32() {
            v.push(T::from(le_u32));
        }
        // u64
        v.push(T::from(value));
        UIntLeBlocksPow2Array::<T>(v)
    }
}

impl<T> From<u128> for UIntLeBlocksPow2Array<T>
where
    T: From<u128> + From<u64> + From<u32> + From<u16> + From<u8>,
{
    // Len == 16 + 8 + 4 + 2 + 1
    // [T(u8(0)), T(u8(1)), ..., T(u128)]
    fn from(value: u128) -> Self {
        let mut v: Vec<T> = vec![];
        // u8 in le order
        for le_u8 in value.to_le_u8() {
            v.push(T::from(le_u8));
        }
        // u16 in le order
        for le_u16 in value.to_le_u16() {
            v.push(T::from(le_u16));
        }
        // u32 in le order
        for le_u32 in value.to_le_u32() {
            v.push(T::from(le_u32));
        }
        // u64 in le order
        for le_u64 in value.to_le_u64() {
            v.push(T::from(le_u64));
        }
        v.push(T::from(value));
        UIntLeBlocksPow2Array::<T>(v)
    }
}

// [lo_u128, hi_u128] : in Little endian order
impl<T> FromN<u128, 2> for UIntLeBlocksPow2Array<T>
where
    T: FromN<u128, 2> + From<u128> + From<u64> + From<u32> + From<u16> + From<u8>,
{
    // Len == 32 + 16 + 8 + 4 + 2 + 1
    // [T(u8(0)), T(u8(1)), ..., T(u256)]
    fn from_n(value: &[u128; 2]) -> Self {
        let mut v: Vec<T> = vec![];
        // u8 in le order
        for le_u8 in value[0].to_le_u8() {
            v.push(T::from(le_u8));
        }
        for le_u8 in value[1].to_le_u8() {
            v.push(T::from(le_u8));
        }
        // u16 in le order
        for le_u16 in value[0].to_le_u16() {
            v.push(T::from(le_u16));
        }
        for le_u16 in value[1].to_le_u16() {
            v.push(T::from(le_u16));
        }
        // u32 in le order
        for le_u32 in value[0].to_le_u32() {
            v.push(T::from(le_u32));
        }
        for le_u32 in value[1].to_le_u32() {
            v.push(T::from(le_u32));
        }
        // u64 in le order
        for le_u64 in value[0].to_le_u64() {
            v.push(T::from(le_u64));
        }
        for le_u64 in value[1].to_le_u64() {
            v.push(T::from(le_u64));
        }
        // u128 in le order
        v.push(T::from(value[0]));
        v.push(T::from(value[1]));
        // u256 in le order
        v.push(T::from_n(value));

        UIntLeBlocksPow2Array::<T>(v)
    }
}

impl<T> UIntLeBlocksPow2Array<T> {
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
    #[inline]
    pub fn bytes_len(&self) -> usize {
        (self.len() + 1) / 2
    }
    #[inline]
    pub fn count_blocks(&self, bytes: u8) -> usize {
        self.bytes_len() / (bytes as usize)
    }
    #[inline]
    pub fn le_uint_at(&self, block_index: UIntBlockIndex, bytes: u8) -> &T {
        let n_u8 = self.bytes_len() as u8;
        assert!(bytes % 2 == 0 || bytes == 1);
        assert_eq!(n_u8 % bytes, 0);
        assert!(block_index < n_u8 / bytes);
        let offset = n_u8 * 2 * (bytes - 1) / bytes;
        &self.0[(offset + block_index) as usize]
    }
    #[inline]
    pub fn le_u8_at(&self, block_index: UIntBlockIndex) -> &T {
        self.le_uint_at(block_index, 1)
    }
    #[inline]
    pub fn le_u16_at(&self, block_index: UIntBlockIndex) -> &T {
        self.le_uint_at(block_index, 2)
    }
    #[inline]
    pub fn le_u32_at(&self, block_index: UIntBlockIndex) -> &T {
        self.le_uint_at(block_index, 4)
    }
    #[inline]
    pub fn le_u64_at(&self, block_index: UIntBlockIndex) -> &T {
        self.le_uint_at(block_index, 8)
    }
    pub fn le_hi_lo_at(&self, block_index: UIntBlockIndex, bytes: u8) -> (&T, &T)
    {
        assert!((block_index as usize) < self.count_blocks(bytes));
        let (hi_block, lo_block) = le_block_index_to_hi_lo(block_index);
        (
            self.le_uint_at(hi_block, bytes/2),
            self.le_uint_at(lo_block, bytes/2),
        )
    }
}

