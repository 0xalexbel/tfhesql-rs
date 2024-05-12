pub trait UIntMap {
    type UIntType;
    type ValueType;

    fn len(&self) -> usize;
    fn contains_key(&self, k: Self::UIntType) -> bool;
    fn get(&self, k: Self::UIntType) -> Option<&Self::ValueType>;

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn key_value_iter<'a>(&'a self) -> impl Iterator<Item = (Self::UIntType, &'a Self::ValueType)>
    where
        Self::ValueType: 'a;
}

pub trait UIntMapKeyValuesRef: UIntMap {
    fn key_values(&self) -> &Vec<(Self::UIntType, Self::ValueType)>;
}

pub trait UIntMapMut: UIntMap {
    fn new() -> Self;
    fn insert(&mut self, k: Self::UIntType, value: Self::ValueType);
}

