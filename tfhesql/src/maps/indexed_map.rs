#[cfg(feature = "parallel")]
use rayon::iter::*;
use std::collections::HashMap;

////////////////////////////////////////////////////////////////////////////////
// IndexedMap
////////////////////////////////////////////////////////////////////////////////

pub struct IndexedMap<K, V> {
    indices: HashMap<K, usize>,
    keys: Vec<K>,
    values: Vec<V>,
}

////////////////////////////////////////////////////////////////////////////////

impl<K, V> Default for IndexedMap<K, V> {
    fn default() -> Self {
        Self {
            indices: HashMap::<K, usize>::new(),
            keys: vec![],
            values: vec![],
        }
    }
}

impl<K, V> IndexedMap<K, V>
where
    K: std::hash::Hash + std::cmp::Eq + Clone,
    V: Clone,
{
    pub fn insert_key(&mut self, key: K, default: &V) -> usize {
        if let Some(existing_index) = self.indices.get(&key) {
            return *existing_index;
        }

        let next_value_index = self.values.len();
        self.keys.push(key.clone());
        self.values.push(default.clone());

        self.indices.insert(key, next_value_index);
        next_value_index
    }
}

impl<K, V> IndexedMap<(K, ()), V>
where
    K: std::hash::Hash + std::cmp::Eq + Clone,
{
    pub fn get(&self, key: K) -> &V {
        let index = self.indices.get(&(key, ())).unwrap();
        &self.values[*index]
    }
}

impl<K, V> IndexedMap<(K, ()), V>
where
    K: std::hash::Hash + std::cmp::Eq + Clone + Send + Sync,
    V: Send,
{
    #[cfg(feature = "parallel")]
    pub fn for_each<F>(&mut self, op: F)
    where
        F: Fn(&mut V, &K) + Send + Sync,
    {
        self.values
            .par_iter_mut()
            .zip(self.keys.par_iter())
            .for_each(|(dst, (the_value, ..))| op(dst, the_value));
    }

    #[cfg(not(feature = "parallel"))]
    pub fn for_each<F>(&mut self, op: F)
    where
        F: Fn(&mut V, &K) + Send + Sync,
    {
        self.values
            .iter_mut()
            .zip(self.keys.iter())
            .for_each(|(dst, (the_value, ..))| op(dst, the_value));
    }
}

impl<K, V> IndexedMap<(K, usize), V>
where
    K: std::hash::Hash + std::cmp::Eq + Clone,
{
    pub fn get(&self, key: K, coord: usize) -> &V {
        let index = self.indices.get(&(key, coord)).unwrap();
        &self.values[*index]
    }
}

impl<K, V> IndexedMap<(K, usize), V>
where
    K: std::hash::Hash + std::cmp::Eq + Clone + Send + Sync,
    V: Send,
{
    #[cfg(feature = "parallel")]
    pub fn for_each<F>(&mut self, op: F)
    where
        F: Fn(&mut V, &K, &usize) + Send + Sync,
    {
        self.values
            .par_iter_mut()
            .zip(self.keys.par_iter())
            .for_each(|(dst, (the_value, the_usize))| op(dst, the_value, the_usize));
    }

    #[cfg(not(feature = "parallel"))]
    pub fn for_each<F>(&mut self, op: F)
    where
        F: Fn(&mut V, &K, &usize) + Send + Sync,
    {
        self.values
            .iter_mut()
            .zip(self.keys.iter())
            .for_each(|(dst, (the_value, the_usize))| op(dst, the_value, the_usize));
    }
}
