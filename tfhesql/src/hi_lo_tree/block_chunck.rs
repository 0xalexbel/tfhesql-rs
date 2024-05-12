use std::collections::HashSet;
use std::hash::Hash;

use crate::uint::block::UIntBlock;

////////////////////////////////////////////////////////////////////////////////
// UIntBlockChunck
////////////////////////////////////////////////////////////////////////////////

pub struct UIntBlockChunck<UInt>
{
    capacity: usize,
    set: HashSet<UIntBlock<UInt>>,
    vec: Vec<UIntBlock<UInt>>,
}

////////////////////////////////////////////////////////////////////////////////

impl<UInt> UIntBlockChunck<UInt>
where
    UInt: Hash + PartialEq + Eq + Clone,
{
    pub fn new(chunck_size: usize) -> Self {
        UIntBlockChunck::<UInt> {
            capacity: chunck_size,
            set: HashSet::new(),
            vec: Vec::<UIntBlock<UInt>>::with_capacity(chunck_size),
        }
    }

    #[inline]
    pub fn vec(&self) -> &Vec<UIntBlock<UInt>> {
        &self.vec
    }

    pub fn insert_block(&mut self, uint_block: &UIntBlock<UInt>) {
        if !self.set.contains(uint_block) {
            self.set.insert(uint_block.clone());
            assert!(self.vec.capacity() > self.vec.len());
            assert!(self.vec.capacity() == self.capacity);
            self.vec.push(uint_block.clone());
        }
    }

    pub fn is_full(&self) -> bool {
        assert!(self.vec.capacity() == self.capacity);
        self.set.len() >= self.capacity
    }

    pub fn is_empty(&self) -> bool {
        assert!(self.vec.capacity() == self.capacity);
        self.set.len() == 0
    }

    pub fn reset(&mut self) {
        self.set.clear();
        self.vec.clear();
    }
}
