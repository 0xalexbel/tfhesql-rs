use super::traits::UIntMap;
use crate::types::MemoryCastInto;
use crate::{encrypt::derive1_encrypt_decrypt, uint::block::U8Block};

use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct U8Map<T> {
    // len is always 256
    values: Vec<T>,
}

impl<T> U8Map<T> {
    pub fn values(&self) -> &Vec<T> {
        &self.values
    }
    pub fn from_values(values: Vec<T>) -> Self {
        assert_eq!(values.len(), 256);
        U8Map::<T> { values }
    }
}

derive1_encrypt_decrypt! { U8Map<T> {values:Vec<T>} }

impl<T, U> MemoryCastInto<U8Map<U>> for U8Map<T>
where
    T: MemoryCastInto<U>,
{
    fn mem_cast_into(self) -> U8Map<U> {
        U8Map::<U> {
            values: self
                .values
                .into_iter()
                .map(|x| MemoryCastInto::<U>::mem_cast_into(x))
                .collect(),
        }
    }
}

impl<T> From<u8> for U8Map<T>
where
    T: From<U8Block>,
{
    /// Complexity : O(256)
    #[inline]
    fn from(value: u8) -> Self {
        let values: Vec<T> = (0u16..(u8::MAX as u16 + 1))
            .map(|rhs| T::from((value, rhs as u8)))
            .collect();
        assert_eq!(values.len(), 256);
        U8Map { values }
    }
}

impl<T> UIntMap for U8Map<T> {
    type UIntType = u8;
    type ValueType = T;

    fn len(&self) -> usize {
        assert_eq!(self.values.len(), 256);
        self.values.len()
    }

    fn contains_key(&self, k: u8) -> bool {
        (k as usize) < self.values.len()
    }

    fn get(&self, k: u8) -> Option<&T> {
        assert_eq!(self.values.len(), 256);
        if (k as usize) < self.values.len() {
            return Some(&self.values[k as usize]);
        }
        None
    }

    fn key_value_iter<'a>(&'a self) -> impl Iterator<Item = (u8, &'a T)>
    where
        T: 'a,
    {
        assert_eq!(self.values.len(), 256);
        self.values
            .iter()
            .enumerate()
            .map(move |(i, v)| (i as u8, v))
    }
}
