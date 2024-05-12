use crate::{
    default_into::DefaultInto,
    hi_lo_tree::hi_lo_logic_op::HiLoLogicOp,
    uint::block::{U16Block, U32Block, U64Block},
    types::ThreadSafeBool,
};

use super::{block_chunck::UIntBlockChunck, u64_hi_lo_logic_op_tree::U64HiLoLogicOpTree};

////////////////////////////////////////////////////////////////////////////////
// Fill
////////////////////////////////////////////////////////////////////////////////

pub struct Fill<'a, LogicOp, I, LogicOp2 = LogicOp>
where
    LogicOp: HiLoLogicOp,
    LogicOp2: HiLoLogicOp,
    LogicOp: From<LogicOp2>,
{
    iter: I,
    tree: &'a mut U64HiLoLogicOpTree<LogicOp, LogicOp::BooleanType>,
    chunck_size: usize,
    other_tree: Option<&'a U64HiLoLogicOpTree<LogicOp2, LogicOp2::BooleanType>>,
}

impl<'a, LogicOp, I, LogicOp2> Fill<'a, LogicOp, I, LogicOp2>
where
    LogicOp: HiLoLogicOp,
    LogicOp2: HiLoLogicOp,
    LogicOp: From<LogicOp2>,
{
    pub fn new(
        tree: &'a mut U64HiLoLogicOpTree<LogicOp, LogicOp::BooleanType>,
        iter: I,
        chunck_size: usize,
        other_tree: Option<&'a U64HiLoLogicOpTree<LogicOp2, LogicOp2::BooleanType>>,
    ) -> Fill<'a, LogicOp, I, LogicOp2> {
        Fill {
            iter,
            tree,
            chunck_size,
            other_tree,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Fill16
////////////////////////////////////////////////////////////////////////////////

impl<'a, LogicOp, I, LogicOp2> Fill<'a, LogicOp, I, LogicOp2>
where
    LogicOp: HiLoLogicOp + Clone + Send + Sync + DefaultInto<LogicOp>,
    <LogicOp as HiLoLogicOp>::BooleanType: ThreadSafeBool,
    LogicOp2: HiLoLogicOp + Clone + Send + Sync + DefaultInto<LogicOp2>,
    <LogicOp2 as HiLoLogicOp>::BooleanType: ThreadSafeBool,
    LogicOp: From<LogicOp2>,
    I: Iterator<Item = U16Block>,
{
    pub fn fill16(&mut self) {
        let mut buffer = vec![LogicOp::default_into(); self.chunck_size];
        let mut chunck = UIntBlockChunck::<u16>::new(self.chunck_size);

        let mut end = false;
        while !end {
            chunck.reset();

            while !chunck.is_full() {
                match self.iter.next() {
                    Some(u16_block) => {
                        if self.tree.contains_u16_block(u16_block) {
                            continue;
                        }
                        chunck.insert_block(&u16_block);
                    }
                    None => {
                        end = true;
                        break;
                    }
                }
            } // end while fill chunck_set

            if chunck.is_empty() {
                return;
            }

            self.tree
                .compute_and_insert_vec_16(chunck.vec(), &mut buffer, self.other_tree);
        } // while !end
    }
}

////////////////////////////////////////////////////////////////////////////////
// Fill32
////////////////////////////////////////////////////////////////////////////////

impl<'a, LogicOp, I, LogicOp2> Fill<'a, LogicOp, I, LogicOp2>
where
    LogicOp: HiLoLogicOp + Clone + Send + Sync + DefaultInto<LogicOp>,
    <LogicOp as HiLoLogicOp>::BooleanType: ThreadSafeBool,
    LogicOp2: HiLoLogicOp + Clone + Send + Sync + DefaultInto<LogicOp2>,
    <LogicOp2 as HiLoLogicOp>::BooleanType: ThreadSafeBool,
    LogicOp: From<LogicOp2>,
    I: Iterator<Item = U32Block>,
{
    pub fn fill32(&mut self) {
        let mut buffer = vec![LogicOp::default_into(); self.chunck_size];
        let mut chunck = UIntBlockChunck::<u32>::new(self.chunck_size);

        let mut end = false;
        while !end {
            chunck.reset();

            while !chunck.is_full() {
                match self.iter.next() {
                    Some(u32_block) => {
                        if self.tree.contains_u32_block(u32_block) {
                            continue;
                        }
                        chunck.insert_block(&u32_block);
                    }
                    None => {
                        end = true;
                        break;
                    }
                }
            } // end while fill chunck_set

            if chunck.is_empty() {
                return;
            }

            self.tree
                .compute_and_insert_vec_32(chunck.vec(), &mut buffer, self.other_tree);
        } // while !end
    }
}

////////////////////////////////////////////////////////////////////////////////
// Fill64
////////////////////////////////////////////////////////////////////////////////

impl<'a, LogicOp, I, LogicOp2> Fill<'a, LogicOp, I, LogicOp2>
where
    LogicOp: HiLoLogicOp + Clone + Send + Sync + DefaultInto<LogicOp>,
    <LogicOp as HiLoLogicOp>::BooleanType: ThreadSafeBool,
    LogicOp2: HiLoLogicOp + Clone + Send + Sync + DefaultInto<LogicOp2>,
    <LogicOp2 as HiLoLogicOp>::BooleanType: ThreadSafeBool,
    LogicOp: From<LogicOp2>,
    I: Iterator<Item = U64Block>,
{
    pub fn fill64(&mut self) {
        let mut buffer = vec![LogicOp::default_into(); self.chunck_size];
        let mut chunck = UIntBlockChunck::<u64>::new(self.chunck_size);

        let mut end = false;
        while !end {
            chunck.reset();

            while !chunck.is_full() {
                match self.iter.next() {
                    Some(u64_block) => {
                        if self.tree.contains_u64_block(u64_block) {
                            continue;
                        }
                        chunck.insert_block(&u64_block);
                    }
                    None => {
                        end = true;
                        break;
                    }
                }
            } // end while fill chunck_set

            if chunck.is_empty() {
                return;
            }

            self.tree
                .compute_and_insert_vec_64(chunck.vec(), &mut buffer, self.other_tree);
        } // while !end
    }
}

#[cfg(test)]
mod test {
    use crate::{
        hi_lo_tree::{eq_gt::ClearBytes64EqGt, u64_hi_lo_logic_op_tree::U64EqGtTree},
        uint::iter::{LeU16BlockIterator, LeU32BlockIterator, LeU64BlockIterator},
    };

    #[test]
    fn test_fill() {
        let secret_u64 = 1234567890u64;
        let v: Vec<u64> = vec![12345_u64];
        let secret_bytes = ClearBytes64EqGt::from(secret_u64);
        let mut secret_tree = U64EqGtTree::<bool>::new(secret_bytes);

        secret_tree
            .fill_with_iter(v.iter_le_u16(), 100)
            .fill16();

        secret_tree
            .fill_with_iter(v.iter_le_u32(), 100)
            .fill32();

        secret_tree
            .fill_with_iter(v.iter_le_u64(), 100)
            .fill64();
    }
}
