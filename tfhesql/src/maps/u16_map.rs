use std::borrow::Borrow;

use super::traits::{UIntMapMut, UIntMapKeyValuesRef, UIntMap};

// 72 bytes
#[derive(Default)]
pub struct U16Map<T> {
    // Number of mask values = 65536 / 8 = 8192
    mask: Vec<u8>,
    // indices[k] == index of u16=k in the values vector
    indices: Vec<u16>,
    // Maximum size = 65536, sorted in 'insert' order
    key_value_pairs: Vec<(u16, T)>,
}

impl<T> U16Map<T> {
    const MASK_BITS: u16 = (std::mem::size_of::<u8>() * 8) as u16;
    const N_INDICES: usize = (u16::MAX as usize) + 1;

    #[inline]
    fn get_key_mask_index(&self, k: u16) -> usize {
        (k / Self::MASK_BITS) as usize
    }

    #[inline]
    fn get_key_mask_bit(&self, k: u16) -> u8 {
        (k % Self::MASK_BITS) as u8
    }

    #[inline]
    fn get_key_mask(&self, k: u16) -> u8 {
        let m_idx = self.get_key_mask_index(k);
        self.mask[m_idx]
    }
}

impl<T> UIntMap for U16Map<T> {
    type UIntType = u16;
    type ValueType = T;

    #[inline]
    fn len(&self) -> usize {
        self.key_value_pairs.len()
    }

    fn get(&self, k: u16) -> Option<&T> {
        if !self.contains_key(k) {
            return None;
        }
        let idx = self.indices[k as usize];
        assert!((idx as usize) < self.key_value_pairs.len());
        Some(self.key_value_pairs[idx as usize].1.borrow())
    }

    fn contains_key(&self, k: u16) -> bool {
        let m = self.get_key_mask(k);
        let m_bit = self.get_key_mask_bit(k);
        (1u8 << m_bit) & m != 0
    }

    fn key_value_iter<'a>(&'a self) -> impl Iterator<Item = (u16, &'a T)> where T: 'a {
        self.key_value_pairs.iter().map(|(i, v)| {
            (*i, v)
        })
    }
}

impl<T> UIntMapKeyValuesRef for U16Map<T> {
    fn key_values(&self) -> &Vec<(u16, T)> {
        &self.key_value_pairs
    }
}

impl<T> UIntMapMut for U16Map<T> {
    fn new() -> Self {
        Self {
            mask: vec![0; Self::N_INDICES / (Self::MASK_BITS as usize)],
            indices: vec![0; Self::N_INDICES],
            key_value_pairs: vec![],
        }
    }
    fn insert(&mut self, k: u16, value: T) {
        assert!(!self.contains_key(k));
        assert!(self.key_value_pairs.len() < Self::N_INDICES);

        let m_idx = self.get_key_mask_index(k);
        let m_bit = self.get_key_mask_bit(k);
        self.mask[m_idx] |= 1u8 << m_bit;
        self.indices[k as usize] = self.key_value_pairs.len() as u16;
        self.key_value_pairs.push((k, value));
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::*;

    #[derive(Debug, PartialEq, Clone)]
    struct TestData {
        i: u64,
        j: u64,
    }
    type TestDataMap = U16Map<TestData>;

    #[test]
    fn test_new() {
        let mut map = TestDataMap::new();
        assert!(map.is_empty());
        assert!(map.len() == 0);
        assert!(!map.contains_key(0));
        assert!(!map.contains_key(u16::MAX));
        assert!(!map.contains_key(u16::MAX / 2));

        let data = TestData { i: 0, j: 1 };
        map.insert(120, data.clone());
        assert!(map.contains_key(120));
        assert!(map.len() == 1);
        assert!(!map.is_empty());
        let v = map.get(120).unwrap();
        assert!(v == &data);

        (0..16).for_each(|i| {
            assert!(!map.contains_key(i));
            map.insert(i, data.clone());
            assert!(map.contains_key(i));
            assert!(map.len() == (1 + i + 1) as usize);
            let v = map.get(i).unwrap();
            assert!(v == &data);
        });
    }

    #[test]
    fn test_insert_all() {
        let mut map = TestDataMap::new();
        let data = TestData { i: 0, j: 1 };
        (0_u32..65536_u32).for_each(|i_32| {
            let i = i_32 as u16;
            assert!(!map.contains_key(i));
            map.insert(i, data.clone());
            assert!(map.contains_key(i));
            assert!(map.len() == (i as usize) + 1);
            let v = map.get(i).unwrap();
            assert!(v == &data);
        });
        assert!(map.len() == 65536);
        let mut count = 0;
        map.key_value_iter().for_each(|x| {
            assert!(x.1.i == 0);
            assert!(x.1.j == 1);
            let v = map.get(x.0).unwrap();
            assert_eq!(*v, *x.1);
            count += 1;
        });
        assert!(count == 65536);
    }
}
