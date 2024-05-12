use arrow_array::{types::*, PrimitiveArray};
use super::block::*;
use crate::ascii::ascii_to_le_u64x4;

////////////////////////////////////////////////////////////////////////////////
// LeUxxBlockIterator
////////////////////////////////////////////////////////////////////////////////

pub trait LeU16BlockIterator {
    fn iter_le_u16(&self) -> impl Iterator<Item = U16Block>;
}
pub trait AsciiU16BlockIterator {
    fn iter_ascii_u16(&self, block_offset: UIntBlockIndex) -> impl Iterator<Item = U16Block>;
}
pub trait LeU32BlockIterator {
    fn iter_le_u32(&self) -> impl Iterator<Item = U32Block>;
}
pub trait AsciiU32BlockIterator {
    fn iter_ascii_u32(&self, block_offset: UIntBlockIndex) -> impl Iterator<Item = U32Block>;
}
pub trait LeU64BlockIterator {
    fn iter_le_u64(&self) -> impl Iterator<Item = U64Block>;
}
pub trait AsciiU64BlockIterator {
    fn iter_ascii_u64(&self, block_offset: UIntBlockIndex) -> impl Iterator<Item = U64Block>;
}
pub trait LeU64x4Iterator {
    fn iter_le_u64x4(&self) -> impl Iterator<Item = U64x4>;
}
pub trait AsciiWordsIterator {
    fn iter_ascii_words(&self) -> impl Iterator<Item = Vec<u8>>;
}

////////////////////////////////////////////////////////////////////////////////
// impl LeUxxBlockIterator for Vec<Uxx>
////////////////////////////////////////////////////////////////////////////////

impl LeU16BlockIterator for Vec<u16> {
    fn iter_le_u16(&self) -> impl Iterator<Item = U16Block> {
        use super::to_uint::ToU16;
        self.iter().map(|u16_ref| u16_ref.le_u16_block(0))
    }
}

impl LeU16BlockIterator for PrimitiveArray<UInt16Type> {
    fn iter_le_u16(&self) -> impl Iterator<Item = U16Block> {
        use super::to_uint::ToU16;
        self.iter().map(|u16_ref| u16_ref.unwrap().le_u16_block(0))
    }
}

impl LeU16BlockIterator for PrimitiveArray<Int16Type> {
    fn iter_le_u16(&self) -> impl Iterator<Item = U16Block> {
        use super::to_uint::ToU16;
        self.iter()
            .map(|o| o.unwrap().unsigned_abs().le_u16_block(0))
    }
}

impl LeU16BlockIterator for Vec<u32> {
    fn iter_le_u16(&self) -> impl Iterator<Item = U16Block> {
        use super::to_uint::ToU16;
        self.iter()
            .flat_map(|u32_ref| [u32_ref.le_u16_block(0), u32_ref.le_u16_block(1)])
    }
}

impl LeU16BlockIterator for PrimitiveArray<UInt32Type> {
    fn iter_le_u16(&self) -> impl Iterator<Item = U16Block> {
        use super::to_uint::ToU16;
        self.iter()
            .map(|o| o.unwrap())
            .flat_map(|the_u32| [the_u32.le_u16_block(0), the_u32.le_u16_block(1)])
    }
}

impl LeU16BlockIterator for PrimitiveArray<Int32Type> {
    fn iter_le_u16(&self) -> impl Iterator<Item = U16Block> {
        use super::to_uint::ToU16;
        self.iter()
            .map(|o| o.unwrap().unsigned_abs())
            .flat_map(|the_u32| [the_u32.le_u16_block(0), the_u32.le_u16_block(1)])
    }
}

impl LeU16BlockIterator for Vec<u64> {
    fn iter_le_u16(&self) -> impl Iterator<Item = U16Block> {
        use super::to_uint::ToU16;
        self.iter().flat_map(|u64_ref| {
            [
                u64_ref.le_u16_block(0),
                u64_ref.le_u16_block(1),
                u64_ref.le_u16_block(2),
                u64_ref.le_u16_block(3),
            ]
        })
    }
}

impl LeU16BlockIterator for PrimitiveArray<UInt64Type> {
    fn iter_le_u16(&self) -> impl Iterator<Item = U16Block> {
        use super::to_uint::ToU16;
        self.iter().map(|o| o.unwrap()).flat_map(|the_u64| {
            [
                the_u64.le_u16_block(0),
                the_u64.le_u16_block(1),
                the_u64.le_u16_block(2),
                the_u64.le_u16_block(3),
            ]
        })
    }
}

impl LeU16BlockIterator for PrimitiveArray<Int64Type> {
    fn iter_le_u16(&self) -> impl Iterator<Item = U16Block> {
        use super::to_uint::ToU16;
        self.iter()
            .map(|o| o.unwrap().unsigned_abs())
            .flat_map(|the_u64| {
                [
                    the_u64.le_u16_block(0),
                    the_u64.le_u16_block(1),
                    the_u64.le_u16_block(2),
                    the_u64.le_u16_block(3),
                ]
            })
    }
}

impl LeU32BlockIterator for Vec<u32> {
    fn iter_le_u32(&self) -> impl Iterator<Item = U32Block> {
        use super::to_uint::ToU32;
        self.iter().map(|u32_ref| u32_ref.le_u32_block(0))
    }
}

impl LeU32BlockIterator for PrimitiveArray<UInt32Type> {
    fn iter_le_u32(&self) -> impl Iterator<Item = U32Block> {
        use super::to_uint::ToU32;
        self.iter().map(|o| o.unwrap().le_u32_block(0))
    }
}

impl LeU32BlockIterator for PrimitiveArray<Int32Type> {
    fn iter_le_u32(&self) -> impl Iterator<Item = U32Block> {
        use super::to_uint::ToU32;
        self.iter().map(|o| o.unwrap().unsigned_abs().le_u32_block(0))
    }
}

impl LeU32BlockIterator for Vec<u64> {
    fn iter_le_u32(&self) -> impl Iterator<Item = U32Block> {
        use super::to_uint::ToU32;
        self.iter()
            .flat_map(|u64_ref| [u64_ref.le_u32_block(0), u64_ref.le_u32_block(1)])
    }
}

impl LeU32BlockIterator for PrimitiveArray<UInt64Type> {
    fn iter_le_u32(&self) -> impl Iterator<Item = U32Block> {
        use super::to_uint::ToU32;
        self.iter()
            .map(|o| o.unwrap())
            .flat_map(|the_u64| [the_u64.le_u32_block(0), the_u64.le_u32_block(1)])
    }
}

impl LeU32BlockIterator for PrimitiveArray<Int64Type> {
    fn iter_le_u32(&self) -> impl Iterator<Item = U32Block> {
        use super::to_uint::ToU32;
        self.iter()
            .map(|o| o.unwrap().unsigned_abs())
            .flat_map(|the_u64| [the_u64.le_u32_block(0), the_u64.le_u32_block(1)])
    }
}

impl LeU64BlockIterator for Vec<u64> {
    fn iter_le_u64(&self) -> impl Iterator<Item = U64Block> {
        use super::to_uint::ToU64;
        self.iter().map(|u64_ref| u64_ref.le_u64_block(0))
    }
}

impl LeU64BlockIterator for PrimitiveArray<UInt64Type> {
    fn iter_le_u64(&self) -> impl Iterator<Item = U64Block> {
        use super::to_uint::ToU64;
        self.iter().map(|o| o.unwrap().le_u64_block(0))
    }
}

impl LeU64BlockIterator for PrimitiveArray<Int64Type> {
    fn iter_le_u64(&self) -> impl Iterator<Item = U64Block> {
        use super::to_uint::ToU64;
        self.iter().map(|o| o.unwrap().unsigned_abs().le_u64_block(0))
    }
}

impl LeU64x4Iterator for Vec<String> {
    fn iter_le_u64x4(&self) -> impl Iterator<Item = U64x4> {
        self.iter().map(|s| ascii_to_le_u64x4(s.as_str()))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::*;
    use crate::uint::block::U16Block;
    use super::LeU16BlockIterator;

    #[test]
    fn test() {
        let a = UInt16Array::from(vec![1, 2, 3, 256, u16::MAX]);
        let v: Vec<U16Block> = a.iter_le_u16().collect();
        assert_eq!(v, vec![(1, 0), (2, 0), (3, 0), (256, 0), (u16::MAX, 0)]);

        let a = UInt32Array::from(vec![1, 2, 3, u16::MAX as u32, (u16::MAX as u32) + 1]);
        let v: Vec<U16Block> = a.iter_le_u16().collect();
        assert_eq!(
            v,
            vec![
                (1, 0),
                (0, 1),
                (2, 0),
                (0, 1),
                (3, 0),
                (0, 1),
                (u16::MAX, 0),
                (0, 1),
                (0, 0),
                (1, 1)
            ]
        );

        let a = UInt64Array::from(vec![1, (u16::MAX as u64) + 1, (u32::MAX as u64) + 1]);
        let v: Vec<U16Block> = a.iter_le_u16().collect();
        assert_eq!(
            v,
            vec![
                (1, 0),
                (0, 1),
                (0, 2),
                (0, 3),
                (0, 0),
                (1, 1),
                (0, 2),
                (0, 3),
                (0, 0),
                (0, 1),
                (1, 2),
                (0, 3),
            ]
        );
    }
}
