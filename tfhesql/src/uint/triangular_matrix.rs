use crate::types::BooleanType;
use rayon::iter::*;

////////////////////////////////////////////////////////////////////////////////
// TriangularMatrix
////////////////////////////////////////////////////////////////////////////////

pub struct TriangularMatrix<T> {
    dim: usize,
    elements: Vec<T>,
}

////////////////////////////////////////////////////////////////////////////////

impl<T> TriangularMatrix<T>
where
    T: BooleanType + Clone,
{
    pub fn bool_alloc(dim: usize) -> Self {
        let elements = vec![T::get_false(); dim * (dim + 1) / 2];
        TriangularMatrix { dim, elements }
    }
}

impl<T> TriangularMatrix<T>
where
    T: Sync + Send + Clone,
{
    pub fn par_filter_map_with<U, P, R>(&self, small: &TriangularMatrix<U>, filter_op: P) -> Vec<R>
    where
        U: Sync + Send + Clone,
        P: Fn(&T, &U) -> Option<R> + Sync + Send,
        R: Send,
    {
        let small_dim = small.dim();
        let small_len = small.len();

        assert!(small_len <= self.len());

        small
            .elements
            .par_iter()
            .enumerate()
            .filter_map(|(small_index, small_element)| {
                let small_coords =
                    TriangularMatrix::<U>::compute_coords(small_index, small_len, small_dim);
                let element = self.get(small_coords.0, small_coords.1);
                filter_op(element, small_element)
            })
            .collect::<Vec<R>>()
    }

    pub fn filter_map_with<U, P, R>(&self, small: &TriangularMatrix<U>, filter_op: P) -> Vec<R>
    where
        U: Sync + Send + Clone,
        P: Fn(&T, &U) -> Option<R> + Sync + Send,
        R: Send,
    {
        let small_dim = small.dim();
        let small_len = small.len();

        assert!(small_len <= self.len());

        small
            .elements
            .iter()
            .enumerate()
            .filter_map(|(small_index, small_element)| {
                let small_coords =
                    TriangularMatrix::<U>::compute_coords(small_index, small_len, small_dim);
                let element = self.get(small_coords.0, small_coords.1);
                filter_op(element, small_element)
            })
            .collect::<Vec<R>>()
    }
}

impl<T> TriangularMatrix<T> {
    pub fn new_empty() -> Self {
        TriangularMatrix::<T> {
            dim: 0,
            elements: vec![],
        }
    }

    pub fn from_vec(elements: Vec<T>, dim: usize) -> Self {
        assert_eq!(elements.len(), dim * (dim + 1) / 2);
        TriangularMatrix::<T> { elements, dim }
    }

    #[inline]
    pub fn dim(&self) -> usize {
        self.dim
    }

    #[inline]
    pub fn elements(&self) -> &Vec<T> {
        &self.elements
    }

    #[inline]
    pub fn elements_mut(&mut self) -> &mut Vec<T> {
        &mut self.elements
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    fn _compute_dim(&self) -> usize {
        let delta = 1 + self.elements.len() * 8;
        let sqrt_delta = f64::sqrt(delta as f64) as u64;
        assert!((sqrt_delta * sqrt_delta) == (delta as u64));
        assert_eq!((sqrt_delta - 1) % 2, 0);
        ((sqrt_delta - 1) / 2) as usize
    }

    #[inline]
    pub fn compute_coords(index: usize, len: usize, dim: usize) -> (usize, usize) {
        assert!(index < len);
        let i = dim - (f64::ceil(f64::sqrt((2 * (len - index)) as f64 + 0.25) - 0.5) as usize);
        let j = index - (dim * i - i * (i + 1) / 2);
        assert!(i < dim);
        assert!(j < dim);
        assert!(i <= j);
        assert_eq!(Self::compute_index(i, j, dim), index);
        (i, j)
    }

    #[inline]
    pub fn compute_index(i: usize, j: usize, dim: usize) -> usize {
        let offset = dim * i - i * (i + 1) / 2;
        assert!(i <= j);
        assert!(j < dim);
        offset + j
    }

    #[cfg(test)]
    #[inline]
    pub fn coords(&self, index: usize) -> (usize, usize) {
        assert!(index < self.elements.len());
        let i = self.dim
            - (f64::ceil(f64::sqrt((2 * (self.len() - index)) as f64 + 0.25) - 0.5) as usize);
        let j = index - self.offset(i);
        assert_eq!(self.index(i, j), index);
        (i, j)
    }

    #[cfg(test)]
    #[inline]
    fn offset(&self, i: usize) -> usize {
        self.dim * i - i * (i + 1) / 2
    }

    #[cfg(test)]
    #[inline]
    pub fn index(&self, i: usize, j: usize) -> usize {
        let offset = self.dim * i - i * (i + 1) / 2;
        assert!(i <= j);
        assert!(j < self.dim);
        offset + j
    }

    #[inline]
    pub fn get(&self, i: usize, j: usize) -> &T {
        let offset = self.dim * i - i * (i + 1) / 2;
        assert!(i <= j);
        assert!(j < self.dim);
        &self.elements[offset + j]
    }

    #[inline]
    pub fn _get_mut(&mut self, i: usize, j: usize) -> &mut T {
        let offset = self.dim * i - i * (i + 1) / 2;
        assert!(i <= j);
        assert!(j < self.dim);
        &mut self.elements[offset + j]
    }
}

////////////////////////////////////////////////////////////////////////////////

impl From<&TriangularMatrix<[bool; 3]>> for Vec<u8> {
    fn from(value: &TriangularMatrix<[bool; 3]>) -> Self {
        let vec_bool_3 = value.elements();
        let mut v = vec![];
        let n = vec_bool_3.len() / 4;
        for i in 0..n {
            let bit_01: u8 = match vec_bool_3[4 * i] {
                [true, false, false] => 1,
                [false, true, false] => 2,
                [false, false, true] => 3,
                _ => panic!("Invalid argument"),
            };
            let bit_23: u8 = match vec_bool_3[4 * i + 1] {
                [true, false, false] => 1,
                [false, true, false] => 2,
                [false, false, true] => 3,
                _ => panic!("Invalid argument"),
            };
            let bit_45: u8 = match vec_bool_3[4 * i + 2] {
                [true, false, false] => 1,
                [false, true, false] => 2,
                [false, false, true] => 3,
                _ => panic!("Invalid argument"),
            };
            let bit_67: u8 = match vec_bool_3[4 * i + 3] {
                [true, false, false] => 1,
                [false, true, false] => 2,
                [false, false, true] => 3,
                _ => panic!("Invalid argument"),
            };
            let the_u8 = bit_01 | (bit_23 << 2) | (bit_45 << 4) | (bit_67 << 6);
            v.push(the_u8);
        }

        if 4 * n < vec_bool_3.len() {
            let bit_01: u8 = match vec_bool_3[4 * n] {
                [true, false, false] => 1,
                [false, true, false] => 2,
                [false, false, true] => 3,
                _ => panic!("Invalid argument"),
            };

            let mut bit_23: u8 = 0;
            if 4 * n + 1 < vec_bool_3.len() {
                bit_23 = match vec_bool_3[4 * n + 1] {
                    [true, false, false] => 1,
                    [false, true, false] => 2,
                    [false, false, true] => 3,
                    _ => panic!("Invalid argument"),
                };
            }

            let mut bit_45: u8 = 0;
            if 4 * n + 2 < vec_bool_3.len() {
                bit_45 = match vec_bool_3[4 * n + 2] {
                    [true, false, false] => 1,
                    [false, true, false] => 2,
                    [false, false, true] => 3,
                    _ => panic!("Invalid argument"),
                };
            }

            let bit_67: u8 = 0;
            assert!(4 * n + 3 >= vec_bool_3.len());

            let the_u8 = bit_01 | (bit_23 << 2) | (bit_45 << 4) | (bit_67 << 6);
            v.push(the_u8);
        }
        v
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::TriangularMatrix;

    #[test]
    fn test() {
        let v = vec![1, 2, 3, 4, 5, 6];
        let t = TriangularMatrix::from_vec(v, 3);
        assert_eq!(t._compute_dim(), t.dim);
        assert_eq!(t.get(0, 0), &1);
        assert_eq!(t.get(0, 1), &2);
        assert_eq!(t.get(0, 2), &3);
        assert_eq!(t.get(1, 1), &4);
        assert_eq!(t.get(1, 2), &5);
        assert_eq!(t.get(2, 2), &6);
        assert_eq!(t.coords(0), (0, 0));
        assert_eq!(t.coords(1), (0, 1));
        assert_eq!(t.coords(2), (0, 2));
        assert_eq!(t.coords(3), (1, 1));
        assert_eq!(t.coords(4), (1, 2));
        assert_eq!(t.coords(5), (2, 2));
    }
}
