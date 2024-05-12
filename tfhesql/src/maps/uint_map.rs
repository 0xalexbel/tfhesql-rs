use std::{collections::HashMap, usize};

use super::traits::{UIntMapMut, UIntMapKeyValuesRef, UIntMap};

// 72 bytes
pub type U32Map<T> = UIntHashMap<u32, T>;
// 72 bytes
pub type U64Map<T> = UIntHashMap<u64, T>;

#[derive(Default)]
pub struct UIntHashMap<UInt, T> {
    indices: HashMap<UInt, UInt>,
    key_value_pairs: Vec<(UInt, T)>,
}

impl<UInt, T> UIntMap for UIntHashMap<UInt, T> 
where
    UInt: Copy + Eq + std::hash::Hash + TryInto<usize> + TryFrom<usize>
{
    type UIntType = UInt;
    type ValueType = T;

    #[inline]
    fn len(&self) -> usize {
        self.key_value_pairs.len()
    }

    #[inline]
    fn contains_key(&self, k: UInt) -> bool {
        self.indices.contains_key(&k)
    }

    fn get(&self, k: UInt) -> Option<&T> {
        let opt_idx = self.indices.get(&k);
        match opt_idx {
            Some(idx) => {
                let result: Result<usize, _> = TryInto::<usize>::try_into(*idx);
                match result {
                    Ok(usize_idx) => Some(&(self.key_value_pairs[usize_idx].1)),
                    Err(_) => panic!("Error"),
                }
            }
            None => None,
        }
    }

    fn key_value_iter<'a>(&'a self) -> impl Iterator<Item = (UInt, &'a T)> where T: 'a {
        self.key_value_pairs.iter().map(|(i, v)| {
            (*i, v)
        })
    }  
}

impl<UInt, T> UIntMapKeyValuesRef for UIntHashMap<UInt, T> 
where
    UInt: Copy + Eq + std::hash::Hash + TryInto<usize> + TryFrom<usize>
{
    fn key_values(&self) -> &Vec<(UInt, T)> {
        &self.key_value_pairs
    }
}

impl<UInt, T> UIntMapMut for UIntHashMap<UInt, T> 
where
    UInt: Copy + Eq + std::hash::Hash + TryInto<usize> + TryFrom<usize>
{
    fn new() -> Self {
        Self {
            indices: HashMap::new(),
            key_value_pairs: vec![],
        }
    }

    fn insert(&mut self, k: UInt, value: T) {
        let next_idx: UInt = self.key_value_pairs.len().try_into().ok().unwrap();
        let old_idx = self.indices.insert(k, next_idx);
        assert!(old_idx.is_none());
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
    type TestDataMapU32 = U32Map<TestData>;
    type TestDataMapU64 = U64Map<TestData>;

    #[test]
    fn test_new() {
        let mut map = TestDataMapU32::new();
        assert!(map.is_empty());
        assert!(map.len() == 0);
        assert!(!map.contains_key(0));
        assert!(!map.contains_key(u32::MAX));
        assert!(!map.contains_key(u32::MAX / 2));

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
        let mut map = TestDataMapU32::new();
        let data = TestData { i: 0, j: 1 };
        (0_u32..65536_u32).for_each(|u_32| {
            let i = u_32;
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

    #[test]
    fn test_insert_all_64() {
        let mut map = TestDataMapU64::new();
        let data = TestData { i: 0, j: 1 };
        (0_u64..65536_u64).for_each(|u_64| {
            let i = u_64;
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
            count += 1;
        });
        assert!(count == 65536);
    }
}
