use super::block::*;
use tfhe::core_crypto::commons::traits::UnsignedInteger;

////////////////////////////////////////////////////////////////////////////////
// FromLeU8 + ToLeU8
////////////////////////////////////////////////////////////////////////////////

pub trait FromLeU8<const N: usize> {
    fn from_le_u8(bytes: [u8; N]) -> Self;
}

pub trait ToLeU8<const N: usize> {
    fn to_le_u8(&self) -> [u8; N];
}

macro_rules! impl_from_to_le_u8 {
    ($int:ty, $n:tt) => {
        impl FromLeU8<$n> for $int {
            fn from_le_u8(bytes: [u8; $n]) -> Self {
                Self::from_le_bytes(bytes)
            }
        }
        impl ToLeU8<$n> for $int {
            fn to_le_u8(&self) -> [u8; $n] {
                self.to_le_bytes()
            }
        }
    };
}

impl_from_to_le_u8!(u8, 1);
impl_from_to_le_u8!(i8, 1);
impl_from_to_le_u8!(u16, 2);
impl_from_to_le_u8!(i16, 2);
impl_from_to_le_u8!(u32, 4);
impl_from_to_le_u8!(i32, 4);
impl_from_to_le_u8!(u64, 8);
impl_from_to_le_u8!(i64, 8);
impl_from_to_le_u8!(u128, 16);
impl_from_to_le_u8!(i128, 16);

////////////////////////////////////////////////////////////////////////////////
// ToU8 + ToU16 + ToU32 + ToU64
////////////////////////////////////////////////////////////////////////////////

pub trait ToU8<const N: usize>: ToLeU8<N> {
    const COUNT_U8: u32;
    fn to_be_u8(&self) -> [u8; N];
    //fn to_le_u8(&self) -> [u8; N];
    fn be_u8_at(&self, block_index: u32) -> u8;
    fn le_u8_at(&self, block_index: u32) -> u8;
    fn le_u8_block(&self, block_index: UIntBlockIndex) -> U8Block;
}
pub trait ToU16<const N: usize>: UnsignedInteger {
    const COUNT_U16: u32;
    fn to_be_u16(&self) -> [u16; N];
    fn to_le_u16(&self) -> [u16; N];
    fn be_u16_at(&self, block_index: u32) -> u16;
    fn le_u16_at(&self, block_index: u32) -> u16;
    fn from_be_u16(be: [u16; N]) -> Self;
    fn from_le_u16(le: [u16; N]) -> Self;
    fn le_u16_block(&self, block_index: UIntBlockIndex) -> U16Block;
}
pub trait ToU32<const N: usize>: UnsignedInteger {
    const COUNT_U32: u32;
    fn to_be_u32(&self) -> [u32; N];
    fn to_le_u32(&self) -> [u32; N];
    fn be_u32_at(&self, block_index: u32) -> u32;
    fn le_u32_at(&self, block_index: u32) -> u32;
    fn from_be_u32(be: [u32; N]) -> Self;
    fn from_le_u32(le: [u32; N]) -> Self;
    fn le_u32_block(&self, block_index: UIntBlockIndex) -> U32Block;
}
pub trait ToU64<const N: usize>: UnsignedInteger {
    const COUNT_U64: u32;
    fn to_be_u64(&self) -> [u64; N];
    fn to_le_u64(&self) -> [u64; N];
    fn be_u64_at(&self, block_index: u32) -> u64;
    fn le_u64_at(&self, block_index: u32) -> u64;
    fn from_be_u64(be: [u64; N]) -> Self;
    fn from_le_u64(le: [u64; N]) -> Self;
    fn le_u64_block(&self, block_index: UIntBlockIndex) -> U64Block;
}

////////////////////////////////////////////////////////////////////////////////
// ToU8
////////////////////////////////////////////////////////////////////////////////

macro_rules! tou8_impl {
    (
    ) => {
        #[inline]
        fn be_u8_at(&self, block_index: u32) -> u8 {
            assert!(block_index < Self::COUNT_U8);
            (self >> u8::BITS * (Self::COUNT_U8 - 1 - block_index)) as u8
        }
        #[inline]
        fn le_u8_at(&self, block_index: u32) -> u8 {
            assert!(block_index < Self::COUNT_U8);
            (self >> u8::BITS * block_index) as u8
        }
        #[inline]
        fn le_u8_block(&self, block_index: UIntBlockIndex) -> U8Block {
            assert!((block_index as u32) < Self::COUNT_U8);
            ((self >> u8::BITS * (block_index as u32)) as u8, block_index)
        }
    };
}

impl ToU8<1> for u8 {
    const COUNT_U8: u32 = 1;
    #[inline]
    fn to_be_u8(&self) -> [u8; 1] {
        [*self]
    }
    #[inline]
    fn be_u8_at(&self, block_index: u32) -> u8 {
        assert_eq!(block_index, 0);
        *self
    }
    #[inline]
    fn le_u8_at(&self, block_index: u32) -> u8 {
        assert_eq!(block_index, 0);
        *self
    }
    #[inline]
    fn le_u8_block(&self, block_index: UIntBlockIndex) -> U8Block {
        assert_eq!(block_index, 0);
        (*self, 0)
    }
}

impl ToU8<2> for u16 {
    const COUNT_U8: u32 = 2;
    #[inline]
    fn to_be_u8(&self) -> [u8; 2] {
        [(self >> u8::BITS) as u8, *self as u8]
    }
    tou8_impl!();
}

impl ToU8<4> for u32 {
    const COUNT_U8: u32 = 4;
    #[inline]
    fn to_be_u8(&self) -> [u8; 4] {
        [
            (self >> (u8::BITS * 3)) as u8,
            (self >> (u8::BITS * 2)) as u8,
            (self >> u8::BITS) as u8,
            *self as u8,
        ]
    }
    tou8_impl!();
}

impl ToU8<8> for u64 {
    const COUNT_U8: u32 = 8;
    #[inline]
    fn to_be_u8(&self) -> [u8; 8] {
        [
            (self >> (u8::BITS * 7)) as u8,
            (self >> (u8::BITS * 6)) as u8,
            (self >> (u8::BITS * 5)) as u8,
            (self >> (u8::BITS * 4)) as u8,
            (self >> (u8::BITS * 3)) as u8,
            (self >> (u8::BITS * 2)) as u8,
            (self >> u8::BITS) as u8,
            *self as u8,
        ]
    }

    tou8_impl!();
}

impl ToU8<16> for u128 {
    const COUNT_U8: u32 = 16;
    #[inline]
    fn to_be_u8(&self) -> [u8; 16] {
        [
            (self >> (u8::BITS * 15)) as u8,
            (self >> (u8::BITS * 14)) as u8,
            (self >> (u8::BITS * 13)) as u8,
            (self >> (u8::BITS * 12)) as u8,
            (self >> (u8::BITS * 11)) as u8,
            (self >> (u8::BITS * 10)) as u8,
            (self >> (u8::BITS * 9)) as u8,
            (self >> (u8::BITS * 8)) as u8,
            (self >> (u8::BITS * 7)) as u8,
            (self >> (u8::BITS * 6)) as u8,
            (self >> (u8::BITS * 5)) as u8,
            (self >> (u8::BITS * 4)) as u8,
            (self >> (u8::BITS * 3)) as u8,
            (self >> (u8::BITS * 2)) as u8,
            (self >> u8::BITS) as u8,
            *self as u8,
        ]
    }
    tou8_impl!();
}

impl ToLeU8<32> for (u128, u128) {
    #[inline]
    fn to_le_u8(&self) -> [u8; 32] {
        let mut a = [0u8; 32];
        a[0..16].copy_from_slice(&self.0.to_le_u8());
        a[16..].copy_from_slice(&self.1.to_le_u8());
        a
    }
}

impl ToU8<32> for (u128, u128) {
    const COUNT_U8: u32 = 32;
    #[inline]
    fn to_be_u8(&self) -> [u8; 32] {
        let mut a = [0u8; 32];
        a[0..16].copy_from_slice(&self.1.to_be_u8());
        a[16..].copy_from_slice(&self.0.to_be_u8());
        a
    }
    #[inline]
    fn be_u8_at(&self, block_index: u32) -> u8 {
        assert!(block_index < Self::COUNT_U8);
        if block_index < Self::COUNT_U8 / 2 {
            self.1.be_u8_at(block_index)
        } else {
            self.0.be_u8_at(block_index - Self::COUNT_U8 / 2)
        }
    }
    #[inline]
    fn le_u8_at(&self, block_index: u32) -> u8 {
        assert!(block_index < Self::COUNT_U8);
        if block_index < Self::COUNT_U8 / 2 {
            self.0.le_u8_at(block_index)
        } else {
            self.1.le_u8_at(block_index - Self::COUNT_U8 / 2)
        }
    }
    #[inline]
    fn le_u8_block(&self, block_index: UIntBlockIndex) -> U8Block {
        (self.le_u8_at(block_index as u32), block_index)
    }
}

impl ToLeU8<32> for [u128; 2] {
    #[inline]
    fn to_le_u8(&self) -> [u8; 32] {
        let mut a = [0u8; 32];
        a[0..16].copy_from_slice(&self[0].to_le_u8());
        a[16..].copy_from_slice(&self[1].to_le_u8());
        a
    }
}

////////////////////////////////////////////////////////////////////////////////
// ToU8<32> for [u128; 2]
////////////////////////////////////////////////////////////////////////////////

impl ToU8<32> for [u128; 2] {
    const COUNT_U8: u32 = 32;
    #[inline]
    fn to_be_u8(&self) -> [u8; 32] {
        let mut a = [0u8; 32];
        a[0..16].copy_from_slice(&self[1].to_be_u8());
        a[16..].copy_from_slice(&self[0].to_be_u8());
        a
    }
    #[inline]
    fn be_u8_at(&self, block_index: u32) -> u8 {
        assert!(block_index < Self::COUNT_U8);
        if block_index < Self::COUNT_U8 / 2 {
            self[1].be_u8_at(block_index)
        } else {
            self[0].be_u8_at(block_index - Self::COUNT_U8 / 2)
        }
    }
    #[inline]
    fn le_u8_at(&self, block_index: u32) -> u8 {
        assert!(block_index < Self::COUNT_U8);
        if block_index < Self::COUNT_U8 / 2 {
            self[0].le_u8_at(block_index)
        } else {
            self[1].le_u8_at(block_index - Self::COUNT_U8 / 2)
        }
    }
    #[inline]
    fn le_u8_block(&self, block_index: UIntBlockIndex) -> U8Block {
        (self.le_u8_at(block_index as u32), block_index)
    }
}

////////////////////////////////////////////////////////////////////////////////
// ToU16
////////////////////////////////////////////////////////////////////////////////

macro_rules! tou16_impl {
    (
    ) => {
        #[inline]
        fn be_u16_at(&self, block_index: u32) -> u16 {
            assert!(block_index < Self::COUNT_U16);
            (self >> (u16::BITS * (Self::COUNT_U16 - 1 - block_index))) as u16
        }
        #[inline]
        fn le_u16_at(&self, block_index: u32) -> u16 {
            assert!(block_index < Self::COUNT_U16);
            (self >> (u16::BITS * block_index)) as u16
        }
        #[inline]
        fn le_u16_block(&self, block_index: UIntBlockIndex) -> U16Block {
            assert!((block_index as u32) < Self::COUNT_U16);
            (
                (self >> (u16::BITS * (block_index as u32))) as u16,
                block_index,
            )
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
// ToU16<1> for u16
////////////////////////////////////////////////////////////////////////////////

impl ToU16<1> for u16 {
    const COUNT_U16: u32 = 1;
    #[inline]
    fn to_be_u16(&self) -> [u16; 1] {
        [*self]
    }
    #[inline]
    fn to_le_u16(&self) -> [u16; 1] {
        [*self]
    }
    #[inline]
    fn be_u16_at(&self, block: u32) -> u16 {
        assert_eq!(block, 0);
        *self
    }
    #[inline]
    fn le_u16_at(&self, block: u32) -> u16 {
        assert_eq!(block, 0);
        *self
    }
    #[inline]
    fn from_be_u16(be: [u16; 1]) -> Self {
        be[0]
    }
    #[inline]
    fn from_le_u16(le: [u16; 1]) -> Self {
        le[0]
    }
    #[inline]
    fn le_u16_block(&self, block_index: UIntBlockIndex) -> U16Block {
        assert!(block_index == 0);
        (*self, 0)
    }
}

////////////////////////////////////////////////////////////////////////////////
// ToU16<2> for u32
////////////////////////////////////////////////////////////////////////////////

impl ToU16<2> for u32 {
    const COUNT_U16: u32 = 2;
    #[inline]
    fn to_be_u16(&self) -> [u16; 2] {
        [(self >> u16::BITS) as u16, *self as u16]
    }
    #[inline]
    fn to_le_u16(&self) -> [u16; 2] {
        [*self as u16, (self >> u16::BITS) as u16]
    }
    #[inline]
    fn from_be_u16(be: [u16; 2]) -> Self {
        ((be[0] as u32) << u16::BITS) | (be[1] as u32)
    }
    #[inline]
    fn from_le_u16(le: [u16; 2]) -> Self {
        (le[0] as u32) | ((le[1] as u32) << u16::BITS)
    }
    tou16_impl!();
}

////////////////////////////////////////////////////////////////////////////////
// ToU16<4> for u64
////////////////////////////////////////////////////////////////////////////////

impl ToU16<4> for u64 {
    const COUNT_U16: u32 = 4;
    #[inline]
    fn to_be_u16(&self) -> [u16; 4] {
        [
            (self >> (u16::BITS * 3)) as u16,
            (self >> (u16::BITS * 2)) as u16,
            (self >> u16::BITS) as u16,
            *self as u16,
        ]
    }
    #[inline]
    fn to_le_u16(&self) -> [u16; 4] {
        [
            *self as u16,
            (self >> u16::BITS) as u16,
            (self >> (u16::BITS * 2)) as u16,
            (self >> (u16::BITS * 3)) as u16,
        ]
    }
    #[inline]
    fn from_be_u16(be: [u16; 4]) -> Self {
        ((be[0] as u64) << (u16::BITS * 3))
            | ((be[1] as u64) << (u16::BITS * 2))
            | ((be[2] as u64) << u16::BITS)
            | (be[3] as u64)
    }
    #[inline]
    fn from_le_u16(le: [u16; 4]) -> Self {
        (le[0] as u64)
            | ((le[1] as u64) << u16::BITS)
            | ((le[2] as u64) << (u16::BITS * 2))
            | ((le[3] as u64) << (u16::BITS * 3))
    }
    tou16_impl!();
}

////////////////////////////////////////////////////////////////////////////////
// ToU16<8> for u128
////////////////////////////////////////////////////////////////////////////////

impl ToU16<8> for u128 {
    const COUNT_U16: u32 = 8;
    #[inline]
    fn to_be_u16(&self) -> [u16; 8] {
        [
            (self >> (u16::BITS * 7)) as u16,
            (self >> (u16::BITS * 6)) as u16,
            (self >> (u16::BITS * 5)) as u16,
            (self >> (u16::BITS * 4)) as u16,
            (self >> (u16::BITS * 3)) as u16,
            (self >> (u16::BITS * 2)) as u16,
            (self >> u16::BITS) as u16,
            *self as u16,
        ]
    }
    #[inline]
    fn to_le_u16(&self) -> [u16; 8] {
        [
            *self as u16,
            (self >> u16::BITS) as u16,
            (self >> (u16::BITS * 2)) as u16,
            (self >> (u16::BITS * 3)) as u16,
            (self >> (u16::BITS * 4)) as u16,
            (self >> (u16::BITS * 5)) as u16,
            (self >> (u16::BITS * 6)) as u16,
            (self >> (u16::BITS * 7)) as u16,
        ]
    }
    #[inline]
    fn from_be_u16(be: [u16; 8]) -> Self {
        ((be[0] as u128) << (u16::BITS * 7))
            | ((be[1] as u128) << (u16::BITS * 6))
            | ((be[2] as u128) << (u16::BITS * 5))
            | ((be[3] as u128) << (u16::BITS * 4))
            | ((be[4] as u128) << (u16::BITS * 3))
            | ((be[5] as u128) << (u16::BITS * 2))
            | ((be[6] as u128) << u16::BITS)
            | (be[7] as u128)
    }
    #[inline]
    fn from_le_u16(le: [u16; 8]) -> Self {
        (le[0] as u128)
            | ((le[1] as u128) << u16::BITS)
            | ((le[2] as u128) << (u16::BITS * 2))
            | ((le[3] as u128) << (u16::BITS * 3))
            | ((le[4] as u128) << (u16::BITS * 4))
            | ((le[5] as u128) << (u16::BITS * 5))
            | ((le[6] as u128) << (u16::BITS * 6))
            | ((le[7] as u128) << (u16::BITS * 7))
    }
    tou16_impl!();
}

#[cfg(test)]
mod test_u16 {
    use super::ToU16;

    #[test]
    fn test() {
        let a = 1234567890u64;
        let b = a.to_be_u16();
        assert_eq!(u64::from_be_u16(b), a);

        let a = 1234567890u32;
        let b = a.to_be_u16();
        assert_eq!(u32::from_be_u16(b), a);
    }
}

////////////////////////////////////////////////////////////////////////////////
// ToU32
////////////////////////////////////////////////////////////////////////////////

macro_rules! tou32_impl {
    (
    ) => {
        #[inline]
        fn be_u32_at(&self, block: u32) -> u32 {
            assert!(block < Self::COUNT_U32);
            (self >> (u32::BITS * (Self::COUNT_U32 - 1 - block))) as u32
        }
        #[inline]
        fn le_u32_at(&self, block: u32) -> u32 {
            assert!(block < Self::COUNT_U32);
            (self >> (u32::BITS * block)) as u32
        }
        #[inline]
        fn le_u32_block(&self, block_index: UIntBlockIndex) -> U32Block {
            assert!((block_index as u32) < Self::COUNT_U32);
            (
                (self >> (u32::BITS * (block_index as u32))) as u32,
                block_index,
            )
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
// ToU32<1> for u32
////////////////////////////////////////////////////////////////////////////////

impl ToU32<1> for u32 {
    const COUNT_U32: u32 = 1;
    #[inline]
    fn to_be_u32(&self) -> [u32; 1] {
        [*self]
    }
    #[inline]
    fn to_le_u32(&self) -> [u32; 1] {
        [*self]
    }
    #[inline]
    fn be_u32_at(&self, block: u32) -> u32 {
        assert_eq!(block, 0);
        *self
    }
    #[inline]
    fn le_u32_at(&self, block: u32) -> u32 {
        assert_eq!(block, 0);
        *self
    }
    #[inline]
    fn from_be_u32(be: [u32; 1]) -> Self {
        be[0]
    }
    #[inline]
    fn from_le_u32(le: [u32; 1]) -> Self {
        le[0]
    }
    #[inline]
    fn le_u32_block(&self, block_index: UIntBlockIndex) -> U32Block {
        assert_eq!(block_index, 0);
        (*self, 0)
    }
}

////////////////////////////////////////////////////////////////////////////////
// ToU32<2> for u64
////////////////////////////////////////////////////////////////////////////////

impl ToU32<2> for u64 {
    const COUNT_U32: u32 = 2;
    #[inline]
    fn to_be_u32(&self) -> [u32; 2] {
        [(self >> u32::BITS) as u32, *self as u32]
    }
    #[inline]
    fn to_le_u32(&self) -> [u32; 2] {
        [*self as u32, (self >> u32::BITS) as u32]
    }
    #[inline]
    fn from_be_u32(be: [u32; 2]) -> Self {
        ((be[0] as u64) << u32::BITS) | (be[1] as u64)
    }

    #[inline]
    fn from_le_u32(le: [u32; 2]) -> Self {
        (le[0] as u64) | ((le[1] as u64) << u32::BITS)
    }
    tou32_impl!();
}

////////////////////////////////////////////////////////////////////////////////
// ToU32<4> for u128
////////////////////////////////////////////////////////////////////////////////

impl ToU32<4> for u128 {
    const COUNT_U32: u32 = 4;
    #[inline]
    fn to_be_u32(&self) -> [u32; 4] {
        [
            (self >> (u32::BITS * 3)) as u32,
            (self >> (u32::BITS * 2)) as u32,
            (self >> u32::BITS) as u32,
            *self as u32,
        ]
    }
    #[inline]
    fn to_le_u32(&self) -> [u32; 4] {
        [
            *self as u32,
            (self >> u32::BITS) as u32,
            (self >> (u32::BITS * 2)) as u32,
            (self >> (u32::BITS * 3)) as u32,
        ]
    }
    #[inline]
    fn from_be_u32(be: [u32; 4]) -> Self {
        ((be[0] as u128) << (u32::BITS * 3))
            | ((be[1] as u128) << (u32::BITS * 2))
            | ((be[2] as u128) << u32::BITS)
            | (be[3] as u128)
    }

    #[inline]
    fn from_le_u32(le: [u32; 4]) -> Self {
        (le[0] as u128)
            | ((le[1] as u128) << u32::BITS)
            | ((le[2] as u128) << (u32::BITS * 2))
            | ((le[3] as u128) << (u32::BITS * 3))
    }
    tou32_impl!();
}

#[cfg(test)]
mod test_u32 {
    use super::ToU32;

    #[test]
    fn test() {
        let a = 1234567890u64;
        let b = a.to_be_u32();
        assert_eq!(u64::from_be_u32(b), a);
    }
}

////////////////////////////////////////////////////////////////////////////////
// ToU64
////////////////////////////////////////////////////////////////////////////////

macro_rules! tou64_impl {
    (
    ) => {
        #[inline]
        fn be_u64_at(&self, block: u32) -> u64 {
            assert!(block < Self::COUNT_U64);
            (self >> u64::BITS * (Self::COUNT_U64 - 1 - block)) as u64
        }
        #[inline]
        fn le_u64_at(&self, block: u32) -> u64 {
            assert!(block < Self::COUNT_U64);
            (self >> u64::BITS * block) as u64
        }
        #[inline]
        fn le_u64_block(&self, block_index: UIntBlockIndex) -> U64Block {
            assert!((block_index as u32) < Self::COUNT_U64);
            (
                (self >> u64::BITS * (block_index as u32)) as u64,
                block_index,
            )
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
// ToU64<1> for u64
////////////////////////////////////////////////////////////////////////////////

impl ToU64<1> for u64 {
    const COUNT_U64: u32 = 1;
    #[inline]
    fn to_be_u64(&self) -> [u64; 1] {
        [*self]
    }
    #[inline]
    fn to_le_u64(&self) -> [u64; 1] {
        [*self]
    }
    #[inline]
    fn be_u64_at(&self, block: u32) -> u64 {
        assert_eq!(block, 0);
        *self
    }
    #[inline]
    fn le_u64_at(&self, block: u32) -> u64 {
        assert_eq!(block, 0);
        *self
    }
    #[inline]
    fn from_be_u64(be: [u64; 1]) -> Self {
        be[0]
    }

    #[inline]
    fn from_le_u64(le: [u64; 1]) -> Self {
        le[0]
    }
    #[inline]
    fn le_u64_block(&self, block_index: UIntBlockIndex) -> U64Block {
        assert_eq!(block_index, 0);
        (*self, 0)
    }
}

////////////////////////////////////////////////////////////////////////////////
// ToU64<2> for u128
////////////////////////////////////////////////////////////////////////////////

impl ToU64<2> for u128 {
    const COUNT_U64: u32 = 2;
    #[inline]
    fn to_be_u64(&self) -> [u64; 2] {
        [(self >> u64::BITS) as u64, *self as u64]
    }
    #[inline]
    fn to_le_u64(&self) -> [u64; 2] {
        [*self as u64, (self >> u64::BITS) as u64]
    }
    #[inline]
    fn from_be_u64(be: [u64; 2]) -> Self {
        ((be[0] as u128) << u64::BITS) | (be[1] as u128)
    }

    #[inline]
    fn from_le_u64(le: [u64; 2]) -> Self {
        (le[0] as u128) | ((le[1] as u128) << u64::BITS)
    }
    tou64_impl!();
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test_128 {
    use super::*;

    #[test]
    fn test() {
        let a = 12356789_u64;
        assert_eq!(a.to_be_bytes(), a.to_be_u8());
        // assert_eq!(
        //     u64::from_be_bytes(a.to_be_bytes()),
        //     u64::from_be_u8(a.to_be_u8())
        // );

        let a = 12356789_u64;
        assert_eq!(a.to_le_bytes(), a.to_le_u8());
        assert_eq!(
            u64::from_le_bytes(a.to_le_bytes()),
            u64::from_le_u8(a.to_le_u8())
        );

        let zero_u128: u128 = 0;
        let a = u128::from_be_u64([0, 0]);
        assert_eq!(zero_u128, a);

        let max_u128: u128 = u128::MAX;
        let a = u128::from_be_u64([u64::MAX, u64::MAX]);
        assert_eq!(max_u128, a);

        let a_u128: u128 = 1234567890_u128;
        let a = a_u128.to_be_u64();
        assert_eq!(a[0], 0);
        assert_eq!(a[1], a_u128 as u64);

        let a_u128: u128 = 1234567890_u128;
        let a = a_u128.to_le_u64();
        assert_eq!(a[1], 0);
        assert_eq!(a[0], a_u128 as u64);

        let a = u128::from_le_u64([1234567890, 0]);
        assert_eq!(a_u128, a);

        let a = u128::from_be_u64([0, 1234567890]);
        assert_eq!(a_u128, a);

        let a = (12356789_u128, 0_u128);
        let b = a.to_le_u8();
        let c = 12356789_u32.to_le_u8();
        assert_eq!(b[0..4], c);
        assert_eq!(b[4..], [0u8; 28]);

        let v = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let a = u128::from_le_bytes(v);
        let b = a.to_le_u8();
        assert_eq!(v, b);
        assert_eq!(u128::from_le_u8(v), a);

        let a = u128::from_be_bytes(v);
        let b = a.to_be_u8();
        assert_eq!(v, b);
        // assert_eq!(u128::from_be_u8(v), a);

        let v1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let v2 = [
            17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
        ];
        let a_lo = u128::from_le_bytes(v1);
        let a_hi = u128::from_le_bytes(v2);
        let a = (a_lo, a_hi).to_le_u8();
        assert_eq!(v1, a[..16]);
        assert_eq!(v2, a[16..]);

        for i in 0..16 {
            assert_eq!((a_lo, a_hi).le_u8_at(i), v1[i as usize])
        }
        for i in 16..32 {
            assert_eq!((a_lo, a_hi).le_u8_at(i), v2[(i - 16) as usize])
        }

        let a_lo = u128::from_be_bytes(v1); // biggest 1
        let a_hi = u128::from_be_bytes(v2); // biggest 17
        let a = (a_lo, a_hi).to_be_u8();
        assert_eq!(v2, a[..16]);
        assert_eq!(v1, a[16..]);

        for i in 0..16 {
            assert_eq!((a_lo, a_hi).be_u8_at(i), v2[i as usize])
        }
        for i in 16..32 {
            assert_eq!((a_lo, a_hi).be_u8_at(i), v1[(i - 16) as usize])
        }
    }
}
