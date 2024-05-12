use crate::sql_ast::ComparatorMask;
use crate::uint::block::*;
use crate::uint::maps::*;
use crate::uint::traits::*;
use crate::{default_into::DefaultInto, types::ThreadSafeBool};

use super::{
    eq_gt::Bytes64EqGt,
    eq_gt::EqGt,
    eq_gt_lt::EqGtLt,
    equ::{Bytes64Equ, Equ},
    fill::Fill,
    hi_lo_logic_op::HiLoLogicOp,
    zero_max::ZeroMaxPow2Array,
};

////////////////////////////////////////////////////////////////////////////////
// U64HiLoLogicOpTree
////////////////////////////////////////////////////////////////////////////////
pub struct U64HiLoLogicOpTree<LogicOp, B> {
    // len = 32
    u8_blocks: U8BlockMap<LogicOp>,
    // len = 16
    u16_blocks: U16BlockMap<LogicOp>,
    // len = 8
    u32_blocks: U32BlockMap<LogicOp>,
    // len = 4
    u64_blocks: U64BlockMap<LogicOp>,
    zero_max: ZeroMaxPow2Array<B>,
}

////////////////////////////////////////////////////////////////////////////////

pub type U64EqGtTree<B> = U64HiLoLogicOpTree<EqGt<B>, B>;
pub type U64EquTree<B> = U64HiLoLogicOpTree<Equ<B>, B>;

////////////////////////////////////////////////////////////////////////////////
// U64EqGtTree new
////////////////////////////////////////////////////////////////////////////////

impl<B> U64EqGtTree<B> {
    pub fn new(bytes64_eq_gt: Bytes64EqGt<B>) -> Self {
        U64EqGtTree::<B> {
            u8_blocks: bytes64_eq_gt.le_bytes,
            u16_blocks: U16BlockMap::<EqGt<B>>::new(),
            u32_blocks: U32BlockMap::<EqGt<B>>::new(),
            u64_blocks: U64BlockMap::<EqGt<B>>::new(),
            zero_max: bytes64_eq_gt.zero_max,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// U64EquTree new
////////////////////////////////////////////////////////////////////////////////

impl<B> U64EquTree<B> {
    pub fn new(bytes64_equ: Bytes64Equ<B>) -> Self {
        U64EquTree::<B> {
            u8_blocks: bytes64_equ.le_bytes,
            u16_blocks: U16BlockMap::<Equ<B>>::new(),
            u32_blocks: U32BlockMap::<Equ<B>>::new(),
            u64_blocks: U64BlockMap::<Equ<B>>::new(),
            zero_max: bytes64_equ.zero_max,
        }
    }
    pub fn clear(&mut self) {
        self.u8_blocks = U8BlockMap::<Equ<B>>::new();
        self.u16_blocks = U16BlockMap::<Equ<B>>::new();
        self.u32_blocks = U32BlockMap::<Equ<B>>::new();
        self.u64_blocks = U64BlockMap::<Equ<B>>::new();
        self.zero_max.drop();
    }
}

impl<B> U64EquTree<B>
where
    B: Clone,
{
    fn ascii_eq_to_u16<'a>(
        &'a self,
        other: u16,
        block_index: UIntBlockIndex,
        stack: &mut Vec<&'a B>,
    ) {
        assert!(block_index < 4);

        if let Some(equ) = self.u16_blocks.get_at((other, block_index)) {
            stack.push(equ.eq());
            return;
        }

        let [u8_0, u8_1] = other.to_le_bytes();
        assert_eq!(u8_1, 0);

        if u8_0 == 0 {
            stack.push(&self.zero_max.le_u16_at(block_index).is_zero.eq);
        } else {
            stack.push(self.u8_blocks.get_at((u8_0, 2 * block_index)).unwrap().eq());
            stack.push(&self.zero_max.le_u8_at(2 * block_index + 1).is_zero.eq);
        }
    }

    fn ascii_eq_to_u32<'a>(
        &'a self,
        other: u32,
        block_index: UIntBlockIndex,
        stack: &mut Vec<&'a B>,
    ) {
        assert!(block_index < 2);
        if let Some(equ) = self.u32_blocks.get_at((other, block_index)) {
            stack.push(equ.eq());
            return;
        }
        let [u16_0, u16_1] = other.to_le_u16();
        if u16_1 == 0 {
            if u16_0 == 0 {
                stack.push(&self.zero_max.le_u32_at(block_index).is_zero.eq);
            } else {
                self.ascii_eq_to_u16(u16_0, 2 * block_index, stack);
            }
        } else {
            assert_ne!(u16_0, 0);
            self.ascii_eq_to_u16(u16_0, 2 * block_index, stack);
            self.ascii_eq_to_u16(u16_1, 2 * block_index + 1, stack);
        }
    }

    pub fn ascii_eq_to_u64<'a>(&'a self, other: u64, stack: &mut Vec<&'a B>) {
        if let Some(equ) = self.as_u64(other) {
            stack.push(equ.eq());
            return;
        }
        let [u32_0, u32_1] = other.to_le_u32();
        if u32_1 == 0 {
            if u32_0 == 0 {
                stack.push(&self.zero_max.le_u64_at(0).is_zero.eq);
            } else {
                self.ascii_eq_to_u32(u32_0, 0, stack);
            }
        } else {
            assert_ne!(u32_0, 0);
            self.ascii_eq_to_u32(u32_0, 0, stack);
            self.ascii_eq_to_u32(u32_1, 1, stack);
        }
    }
}

#[cfg(test)]
impl<LogicOp, B> U64HiLoLogicOpTree<LogicOp, B> {
    pub fn count_u8_mem(&self) -> usize {
        self.u8_blocks.count_mem()
    }
    pub fn count_u16_mem(&self) -> usize {
        self.u16_blocks.count_mem()
    }
    pub fn count_u32_mem(&self) -> usize {
        self.u32_blocks.count_mem()
    }
    pub fn count_u64_mem(&self) -> usize {
        self.u64_blocks.count_mem()
    }
}

impl<LogicOp, B> U64HiLoLogicOpTree<LogicOp, B> {
    /// Equivalent to (self as bool) == other
    pub fn as_bool(&self, other: bool) -> Option<&LogicOp> {
        let other_u8: u8 = if other { 1 } else { 0 };
        self.u8_blocks.get_at(other_u8.le_u8_block(0))
    }
    /// Equivalent to (self as u8) == other
    pub fn as_u8(&self, other: u8) -> Option<&LogicOp> {
        self.u8_blocks.get_at(other.le_u8_block(0))
    }
    /// Equivalent to (self as u16) == other
    pub fn as_u16(&self, other: u16) -> Option<&LogicOp> {
        let aa = other.le_u16_block(0);
        assert_eq!(aa.0, other);
        assert_eq!(aa.1, 0);
        self.u16_blocks.get_at(other.le_u16_block(0))
    }
    /// Equivalent to (self as u32) == other
    pub fn as_u32(&self, other: u32) -> Option<&LogicOp> {
        self.u32_blocks.get_at(other.le_u32_block(0))
    }
    /// Equivalent to (self as u16) == other
    pub fn as_u64(&self, other: u64) -> Option<&LogicOp> {
        self.u64_blocks.get_at(other.le_u64_block(0))
    }

    #[inline]
    fn assert_not_u16_overflow(&self, u16_block: U16Block) {
        assert!(
            (blk_index!(u16_block) as usize)
                < self.zero_max.count_blocks((u16::BITS / u8::BITS) as u8)
        );
    }
    #[inline]
    fn assert_not_u32_overflow(&self, u32_block: U32Block) {
        assert!(
            (blk_index!(u32_block) as usize)
                < self.zero_max.count_blocks((u32::BITS / u8::BITS) as u8)
        );
    }
    #[inline]
    fn assert_not_u64_overflow(&self, u64_block: U64Block) {
        assert!(
            (blk_index!(u64_block) as usize)
                < self.zero_max.count_blocks((u64::BITS / u8::BITS) as u8)
        );
    }

    #[inline]
    pub fn contains_u16_block(&self, u16_block: U16Block) -> bool {
        self.assert_not_u16_overflow(u16_block);
        self.u16_blocks.contains_at(u16_block)
    }
    #[inline]
    pub fn contains_u32_block(&self, u32_block: U32Block) -> bool {
        self.assert_not_u32_overflow(u32_block);
        self.u32_blocks.contains_at(u32_block)
    }
    #[inline]
    pub fn contains_u64_block(&self, u64_block: U64Block) -> bool {
        self.assert_not_u64_overflow(u64_block);
        self.u64_blocks.contains_at(u64_block)
    }
}

impl<LogicOp> U64HiLoLogicOpTree<LogicOp, LogicOp::BooleanType>
where
    LogicOp: HiLoLogicOp,
    <LogicOp as HiLoLogicOp>::BooleanType: ThreadSafeBool,
{
    pub fn fill_with_iter_and_secondary_tree<'a, I, UInt, LogicOp2>(
        &'a mut self,
        iter: I,
        chunck_size: usize,
        other_tree: Option<&'a U64HiLoLogicOpTree<LogicOp2, LogicOp2::BooleanType>>,
    ) -> Fill<'a, LogicOp, I, LogicOp2>
    where
        I: Iterator<Item = UIntBlock<UInt>>,
        LogicOp2: HiLoLogicOp,
        <LogicOp2 as HiLoLogicOp>::BooleanType: ThreadSafeBool,
        LogicOp: From<LogicOp2>,
    {
        Fill::new(self, iter, chunck_size, other_tree)
    }

    pub fn fill_with_iter<I, UInt>(
        &mut self,
        iter: I,
        chunck_size: usize,
    ) -> Fill<LogicOp, I, LogicOp>
    where
        I: Iterator<Item = UIntBlock<UInt>>,
    {
        Fill::new(self, iter, chunck_size, None)
    }
}

////////////////////////////////////////////////////////////////////////////////
// AsUInt
////////////////////////////////////////////////////////////////////////////////
pub trait AsUInt<UInt, LogicOp> {
    fn as_uint(&self, unsigned_num: UInt) -> Option<&LogicOp>;
}

impl<LogicOp> AsUInt<bool, LogicOp> for U64HiLoLogicOpTree<LogicOp, LogicOp::BooleanType>
where
    LogicOp: HiLoLogicOp,
    <LogicOp as HiLoLogicOp>::BooleanType: ThreadSafeBool,
{
    fn as_uint(&self, unsigned_num: bool) -> Option<&LogicOp> {
        self.as_bool(unsigned_num)
    }
}

impl<LogicOp> AsUInt<u8, LogicOp> for U64HiLoLogicOpTree<LogicOp, LogicOp::BooleanType>
where
    LogicOp: HiLoLogicOp,
    <LogicOp as HiLoLogicOp>::BooleanType: ThreadSafeBool,
{
    fn as_uint(&self, unsigned_num: u8) -> Option<&LogicOp> {
        self.as_u8(unsigned_num)
    }
}

impl<LogicOp> AsUInt<u16, LogicOp> for U64HiLoLogicOpTree<LogicOp, LogicOp::BooleanType>
where
    LogicOp: HiLoLogicOp,
    <LogicOp as HiLoLogicOp>::BooleanType: ThreadSafeBool,
{
    fn as_uint(&self, unsigned_num: u16) -> Option<&LogicOp> {
        self.as_u16(unsigned_num)
    }
}

impl<LogicOp> AsUInt<u32, LogicOp> for U64HiLoLogicOpTree<LogicOp, LogicOp::BooleanType>
where
    LogicOp: HiLoLogicOp,
    <LogicOp as HiLoLogicOp>::BooleanType: ThreadSafeBool,
{
    fn as_uint(&self, unsigned_num: u32) -> Option<&LogicOp> {
        self.as_u32(unsigned_num)
    }
}

impl<LogicOp> AsUInt<u64, LogicOp> for U64HiLoLogicOpTree<LogicOp, LogicOp::BooleanType>
where
    LogicOp: HiLoLogicOp,
    <LogicOp as HiLoLogicOp>::BooleanType: ThreadSafeBool,
{
    fn as_uint(&self, unsigned_num: u64) -> Option<&LogicOp> {
        self.as_u64(unsigned_num)
    }
}

////////////////////////////////////////////////////////////////////////////////
// CompareToUnsignedInteger
////////////////////////////////////////////////////////////////////////////////
pub trait CompareToUnsignedInteger<UInt, B> {
    fn compare_to_unsigned(&self, left_uint: UInt, comparator: &ComparatorMask<B>) -> B;
    fn eq_gt_lt_from_unsigned(&self, left_uint: UInt) -> EqGtLt<B>;
}

impl<UInt, B> CompareToUnsignedInteger<UInt, B> for U64HiLoLogicOpTree<EqGt<B>, B>
where
    Self: AsUInt<UInt, EqGt<B>>,
    B: ThreadSafeBool + DefaultInto<B>,
{
    fn compare_to_unsigned(&self, left_uint: UInt, comparator: &ComparatorMask<B>) -> B {
        let eq_gt = self.as_uint(left_uint).unwrap();
        comparator.or_and_eq_gt(&eq_gt.eq, &eq_gt.gt)
    }
    fn eq_gt_lt_from_unsigned(&self, left_uint: UInt) -> EqGtLt<B> {
        let eq_gt = self.as_uint(left_uint).unwrap();
        EqGtLt::from_eq_gt(&eq_gt.eq, &eq_gt.gt)
    }
}

////////////////////////////////////////////////////////////////////////////////
// CompareToSignedInteger
////////////////////////////////////////////////////////////////////////////////

pub trait CompareToSignedInteger<UInt, B> {
    fn compare_to_signed(
        &self,
        left_signed_num: (UInt, bool),
        right_is_strictly_negative: &B,
        not_right_is_strictly_negative: &B,
        comparator: &ComparatorMask<B>,
    ) -> B;
    fn eq_gt_lt_from_signed(
        &self,
        left_signed_num: (UInt, bool),
        right_is_strictly_negative: &B,
        not_right_is_strictly_negative: &B,
    ) -> EqGtLt<B>;
}

impl<UInt, B> CompareToSignedInteger<UInt, B> for U64HiLoLogicOpTree<EqGt<B>, B>
where
    Self: AsUInt<UInt, EqGt<B>>,
    B: ThreadSafeBool + DefaultInto<B>,
{
    fn compare_to_signed(
        &self,
        left_signed_num: (UInt, bool),
        right_is_strictly_negative: &B,
        not_right_is_strictly_negative: &B,
        comparator: &ComparatorMask<B>,
    ) -> B {
        // signed_num.0 == abs(signed_num)
        // signed_num.1 == signed_num is STRICTLY negative
        let eq_gt = self.as_uint(left_signed_num.0).unwrap();
        if left_signed_num.1 {
            // eq: { X == the_i8 (<0) } = { X < 0 AND |X| == |the_i8| }
            // lt: { X <  the_i8 (<0) } = { X < 0 AND |X| >  |the_i8| }
            let (eq, lt) = rayon::join(
                || eq_gt.eq.refref_bitand(right_is_strictly_negative),
                || eq_gt.gt.refref_bitand(right_is_strictly_negative),
            );
            comparator.or_and_eq_lt(&eq, &lt)
        } else {
            // eq: { X == the_i8 (>=0) } = { X >= 0 AND |X| == |the_i8| }
            // gt: { X >  the_i8 (>=0) } = { X >= 0 AND |X| >  |the_i8| }
            let (eq, gt) = rayon::join(
                || eq_gt.eq.refref_bitand(not_right_is_strictly_negative),
                || eq_gt.gt.refref_bitand(not_right_is_strictly_negative),
            );
            comparator.or_and_eq_gt(&eq, &gt)
        }
    }

    fn eq_gt_lt_from_signed(
        &self,
        left_signed_num: (UInt, bool),
        right_is_strictly_negative: &B,
        not_right_is_strictly_negative: &B,
    ) -> EqGtLt<B> {
        // signed_num.0 == abs(signed_num)
        // signed_num.1 == signed_num is STRICTLY negative
        let eq_gt = self.as_uint(left_signed_num.0).unwrap();
        if left_signed_num.1 {
            // eq: { X == the_i8 (<0) } = { X < 0 AND |X| == |the_i8| }
            // lt: { X <  the_i8 (<0) } = { X < 0 AND |X| >  |the_i8| }
            let (eq, lt) = rayon::join(
                || eq_gt.eq.refref_bitand(right_is_strictly_negative),
                || eq_gt.gt.refref_bitand(right_is_strictly_negative),
            );
            EqGtLt::from_eq_lt(&eq, &lt)
        } else {
            // eq: { X == the_i8 (>=0) } = { X >= 0 AND |X| == |the_i8| }
            // gt: { X >  the_i8 (>=0) } = { X >= 0 AND |X| >  |the_i8| }
            let (eq, gt) = rayon::join(
                || eq_gt.eq.refref_bitand(not_right_is_strictly_negative),
                || eq_gt.gt.refref_bitand(not_right_is_strictly_negative),
            );
            EqGtLt::from_eq_gt(&eq, &gt)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Compute and Insert vec
////////////////////////////////////////////////////////////////////////////////

impl<LogicOp> U64HiLoLogicOpTree<LogicOp, LogicOp::BooleanType>
where
    LogicOp: HiLoLogicOp + Clone + Send + Sync,
    <LogicOp as HiLoLogicOp>::BooleanType: ThreadSafeBool,
{
    pub(super) fn compute_and_insert_vec_16<LogicOp2>(
        &mut self,
        u16_block_vec: &Vec<U16Block>,
        buffer: &mut Vec<LogicOp>,
        other_tree: Option<&U64HiLoLogicOpTree<LogicOp2, LogicOp2::BooleanType>>,
    ) where
        LogicOp2: HiLoLogicOp + Clone + Send + Sync,
        <LogicOp2 as HiLoLogicOp>::BooleanType: ThreadSafeBool,
        LogicOp: From<LogicOp2>,
    {
        let m: Option<&U16BlockMap<LogicOp2>> = match other_tree {
            Some(t) => Some(&t.u16_blocks),
            None => None,
        };

        LogicOp::compute_and_insert_into(
            u16_block_vec,
            &mut self.u16_blocks,
            &self.u8_blocks,
            &self.zero_max,
            buffer,
            m,
        );
    }

    pub(super) fn compute_and_insert_vec_32<LogicOp2>(
        &mut self,
        u32_block_vec: &Vec<U32Block>,
        buffer: &mut Vec<LogicOp>,
        other_tree: Option<&U64HiLoLogicOpTree<LogicOp2, LogicOp2::BooleanType>>,
    ) where
        LogicOp2: HiLoLogicOp + Clone + Send + Sync,
        <LogicOp2 as HiLoLogicOp>::BooleanType: ThreadSafeBool,
        LogicOp: From<LogicOp2>,
    {
        let m: Option<&U32BlockMap<LogicOp2>> = match other_tree {
            Some(t) => Some(&t.u32_blocks),
            None => None,
        };

        LogicOp::compute_and_insert_into(
            u32_block_vec,
            &mut self.u32_blocks,
            &self.u16_blocks,
            &self.zero_max,
            buffer,
            m,
        );
    }

    pub(super) fn compute_and_insert_vec_64<LogicOp2>(
        &mut self,
        u64_block_vec: &Vec<U64Block>,
        buffer: &mut Vec<LogicOp>,
        other_tree: Option<&U64HiLoLogicOpTree<LogicOp2, LogicOp2::BooleanType>>,
    ) where
        LogicOp2: HiLoLogicOp + Clone + Send + Sync,
        <LogicOp2 as HiLoLogicOp>::BooleanType: ThreadSafeBool,
        LogicOp: From<LogicOp2>,
    {
        let m: Option<&U64BlockMap<LogicOp2>> = match other_tree {
            Some(t) => Some(&t.u64_blocks),
            None => None,
        };

        LogicOp::compute_and_insert_into(
            u64_block_vec,
            &mut self.u64_blocks,
            &self.u32_blocks,
            &self.zero_max,
            buffer,
            m,
        );
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use crate::uint::iter::*;
    use crate::{
        hi_lo_tree::{ClearBytes64EqGt, U64EqGtTree},
        uint::traits::{ToU16, ToU32},
        OrderedTables, Table,
    };
    use arrow_array::{RecordBatch, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
    use arrow_schema::{DataType, Field, Schema};
    use rand::Rng;
    use std::sync::Arc;

    #[test]
    fn test() {
        let mut rng = rand::thread_rng();

        let secret_u64 = 1234567890_u64;
        let secret_u64_bytes = ClearBytes64EqGt::from(secret_u64);

        let u8_0 = 12_u8;
        let u8_1 = 129_u8;

        let v8 = [u8_0, u8_1];
        let v16: [u16; 2 * 2] = std::array::from_fn(|i| u16::from_le_bytes([v8[i / 2], v8[i % 2]]));
        let v32: [u32; 4 * 4] = std::array::from_fn(|i| u32::from_le_u16([v16[i / 4], v16[i % 4]]));
        let v64: [u64; 16 * 16] =
            std::array::from_fn(|i| u64::from_le_u32([v32[i / 16], v32[i % 16]]));

        let schema = Schema::new(vec![
            Field::new("u8", DataType::UInt8, false),
            Field::new("u16", DataType::UInt16, false),
            Field::new("u32", DataType::UInt32, false),
            Field::new("u64", DataType::UInt64, false),
        ]);

        let n = v64.len() * 10;
        let mut v_u8: Vec<u8> = vec![0; n];
        v_u8.iter_mut().zip(v8.iter()).for_each(|(dst, src)| {
            *dst = *src;
        });
        let mut v_u16: Vec<u16> = vec![0; n];
        v_u16.iter_mut().zip(v16.iter()).for_each(|(dst, src)| {
            *dst = *src;
        });
        let mut v_u32: Vec<u32> = vec![0; n];
        v_u32.iter_mut().zip(v32.iter()).for_each(|(dst, src)| {
            *dst = *src;
        });
        let mut v_u64: Vec<u64> = vec![0; n];
        v_u64.iter_mut().zip(v64.iter()).for_each(|(dst, src)| {
            *dst = *src;
        });

        v_u8.iter_mut().skip(v8.len()).for_each(|x| {
            *x = v8[rng.gen_range(0..v8.len())];
        });
        v_u16.iter_mut().skip(v16.len()).for_each(|x| {
            *x = v16[rng.gen_range(0..v16.len())];
        });
        v_u32.iter_mut().skip(v32.len()).for_each(|x| {
            *x = v32[rng.gen_range(0..v32.len())];
        });
        v_u64.iter_mut().skip(v64.len()).for_each(|x| {
            *x = v64[rng.gen_range(0..v64.len())];
        });

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(UInt8Array::from(v_u8)),
                Arc::new(UInt16Array::from(v_u16)),
                Arc::new(UInt32Array::from(v_u32)),
                Arc::new(UInt64Array::from(v_u64)),
            ],
        )
        .unwrap();

        let table = Table::new("table1", batch);
        let tables = OrderedTables::new(vec![table]).unwrap();
        let chunck_size = n;

        let mut eq_gt_cache = U64EqGtTree::<bool>::new(secret_u64_bytes);
        eq_gt_cache
            .fill_with_iter(tables.iter_le_u16(), chunck_size)
            .fill16();
        eq_gt_cache
            .fill_with_iter(tables.iter_le_u32(), chunck_size)
            .fill32();
        eq_gt_cache
            .fill_with_iter(tables.iter_le_u64(), chunck_size)
            .fill64();

        assert_eq!(eq_gt_cache.count_u8_mem(), 256 * 8);
        assert_eq!(eq_gt_cache.count_u16_mem(), 16);
        assert_eq!(eq_gt_cache.count_u32_mem(), 32);
        assert_eq!(eq_gt_cache.count_u64_mem(), 256);
    }
}
