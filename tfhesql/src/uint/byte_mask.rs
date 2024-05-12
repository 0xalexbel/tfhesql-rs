use super::triangular_matrix::TriangularMatrix;
use crate::default_into::*;
use crate::encrypt::derive1_encrypt_decrypt;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::bitops::*;
use crate::types::*;
use rayon::iter::*;

////////////////////////////////////////////////////////////////////////////////
// Mask
////////////////////////////////////////////////////////////////////////////////

pub trait SizedMask {
    const LEN: usize;
}

#[derive(Clone, PartialEq, Eq, Default, Debug, serde::Deserialize, serde::Serialize)]
pub struct Mask<T> {
    pub mask: Vec<T>,
}

derive1_encrypt_decrypt! { Mask<T> {mask:Vec<T>} }

////////////////////////////////////////////////////////////////////////////////

pub type ByteMask<U8> = Mask<U8>;
pub type BoolMask<B> = Mask<B>;

pub type ClearBoolMask = Mask<bool>;
pub type ClearByteMask = Mask<u8>;

////////////////////////////////////////////////////////////////////////////////

impl<T, U> ValueFrom<&Mask<T>> for Mask<U>
where
    for<'a> U: ValueFrom<&'a T> + Send,
    T: Send + Sync,
{
    fn value_from(value: &Mask<T>) -> Self {
        let mask: Vec<U> = value.mask.par_iter().map(|x| U::value_from(x)).collect();
        Mask::<U> { mask }
    }
}

impl<T> RefBitAnd for Mask<T>
where
    T: RefBitAnd<Output = T> + Send + Sync + Clone + DefaultInto<T>,
{
    type Output = Self;
    fn ref_bitand(&self, rhs: Self) -> Self::Output {
        let mask: Vec<T> = self
            .mask
            .par_iter()
            .zip(rhs.mask.par_iter())
            .map(|(left, right)| left.refref_bitand(right))
            .collect();
        Mask::<T> { mask }
    }
    fn refref_bitand(&self, rhs: &Self) -> Self::Output {
        let mask: Vec<T> = self
            .mask
            .par_iter()
            .zip(rhs.mask.par_iter())
            .map(|(left, right)| left.refref_bitand(right))
            .collect();
        Mask::<T> { mask }
    }
}

impl<T> RefBitAnd<T> for Mask<T>
where
    T: RefBitAnd<Output = T> + Send + Sync,
{
    type Output = Self;
    fn ref_bitand(&self, rhs: T) -> Self::Output {
        let mask: Vec<T> = self
            .mask
            .par_iter()
            .map(|m| m.refref_bitand(&rhs))
            .collect();
        Mask::<T> { mask }
    }
    fn refref_bitand(&self, rhs: &T) -> Self::Output {
        let mask: Vec<T> = self.mask.par_iter().map(|m| m.refref_bitand(rhs)).collect();
        Mask::<T> { mask }
    }
}

impl<FromT, ToT> RefBitOr<Mask<ToT>> for Mask<FromT>
where
    FromT: Clone + Sync + Send + RefBitOr<ToT, Output = ToT>,
    ToT: Clone + Send + Sync + ValueFrom<FromT> + DefaultInto<ToT>,
{
    type Output = Mask<ToT>;

    fn ref_bitor(&self, rhs: Mask<ToT>) -> Mask<ToT> {
        self.refref_bitor(&rhs)
    }

    fn refref_bitor(&self, rhs: &Mask<ToT>) -> Mask<ToT> {
        let len = self.len().max(rhs.len());
        let mut row = Mask::<ToT>::alloc(len, ToT::default_into());
        row.mask
            .par_iter_mut()
            .zip(self.mask.par_iter().zip(rhs.mask.par_iter()))
            .for_each(|(dst, (lhs, rhs))| *dst = lhs.refref_bitor(rhs));
        if self.len() == rhs.len() {
            return row;
        }

        if len == self.len() {
            for i in rhs.len()..len {
                row.mask.push(ToT::value_from(self.mask[i].clone()))
            }
        } else {
            assert_eq!(len, rhs.len());
            for i in self.len()..len {
                row.mask.push(rhs.mask[i].clone())
            }
        }
        row
    }
}

impl<T> RefBitOr<T> for Mask<T>
where
    T: Clone + Send + Sync + ValueFrom<T> + DefaultInto<T> + RefBitOr<T, Output = T>,
{
    type Output = Self;
    fn ref_bitor(&self, rhs: T) -> Self::Output {
        let mask: Vec<T> = self.mask.par_iter().map(|m| m.refref_bitor(&rhs)).collect();
        Mask::<T> { mask }
    }
    fn refref_bitor(&self, rhs: &T) -> Self::Output {
        let mask: Vec<T> = self.mask.par_iter().map(|m| m.refref_bitor(rhs)).collect();
        Mask::<T> { mask }
    }
}

impl<T> RefNot for Mask<T>
where
    T: RefNot + Send + Sync,
{
    fn ref_not(&self) -> Mask<T> {
        let mask: Vec<T> = self.mask.par_iter().map(|x| x.ref_not()).collect();
        Mask::<T> { mask }
    }
}

impl std::fmt::Display for ClearByteMask {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut s = vec![];
        self.mask.iter().for_each(|m| {
            s.push(format!("{:#04x}", m));
        });
        f.write_str(s.join(", ").as_str())
    }
}

impl std::fmt::Display for ClearBoolMask {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut s = vec![];
        self.mask.iter().for_each(|m| {
            if *m {
                s.push("1".to_string());
            } else {
                s.push("0".to_string());
            }
        });
        f.write_str(s.join(", ").as_str())
    }
}

impl<T> Mask<T> {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.mask.len()
    }
    #[inline]
    pub fn get(&self, index: usize) -> &T {
        &self.mask[index]
    }
    #[inline]
    pub fn new_empty() -> Self {
        Mask { mask: vec![] }
    }
}

impl<T> Mask<T>
where
    T: Clone,
{
    #[inline]
    pub fn alloc(len: usize, default: T) -> Self {
        Mask {
            mask: vec![default; len],
        }
    }
}

impl<T> Mask<T>
where
    T: BooleanType + Clone,
{
    #[inline]
    pub fn all_false(len: usize) -> Self {
        Mask {
            mask: vec![T::get_false(); len],
        }
    }
    #[inline]
    pub fn all_true(len: usize) -> Self {
        Mask {
            mask: vec![T::get_true(); len],
        }
    }
}

impl<T> Mask<T>
where
    T: UIntType + Clone,
{
    #[inline]
    pub fn none(len: usize) -> Self {
        Mask {
            mask: vec![T::get_zero(); len],
        }
    }
    #[inline]
    pub fn set(&mut self, index: usize) {
        self.mask[index] = T::get_max();
    }
    #[inline]
    pub fn all(len: usize) -> Self {
        assert!(len > 0);
        let mut v = Self::none(len);
        v.mask.iter_mut().for_each(|m| *m = T::get_max());
        v
    }
    #[inline]
    pub fn set_all(&mut self) {
        self.mask.iter_mut().for_each(|m| {
            *m = T::get_max();
        });
    }
    #[inline]
    pub fn set_from_to(&mut self, from_index: usize, to_index: usize) {
        for i in from_index..=to_index {
            self.mask[i] = T::get_max();
        }
    }
    #[inline]
    pub fn unset(&mut self, index: usize) {
        self.mask[index] = T::get_zero();
    }
    #[inline]
    pub fn unset_all(&mut self) {
        self.mask.iter_mut().for_each(|m| {
            *m = T::get_zero();
        });
    }
    #[inline]
    pub fn max_u64(&self) -> u64 {
        let mut the_u64 = 0_u64;
        for i in 0..self.len() {
            the_u64 |= (1 << i) as u64;
        }
        the_u64
    }

    pub fn extract(&self, indices: &[usize]) -> Vec<&T> {
        indices.iter().map(|i| &self.mask[*i]).collect()
    }
}

impl<T> Mask<T>
where
    T: ThreadSafeBool,
{
    pub fn triangular_matrix(&self, rhs: &Mask<T>) -> TriangularMatrix<T> {
        let mut trm = TriangularMatrix::<T>::bool_alloc(self.len());
        let dim = trm.dim();
        let len = trm.len();
        assert_eq!(rhs.len(), self.len());
        trm.elements_mut()
            .par_iter_mut()
            .enumerate()
            .for_each(|(index, dst)| {
                let (i, j) = TriangularMatrix::<T>::compute_coords(index, len, dim);
                let left = &self.mask[i];
                let right = &rhs.mask[j];
                *dst = left.refref_bitand(right);
            });
        trm
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClearByteMask
////////////////////////////////////////////////////////////////////////////////

impl ClearByteMask {
    #[inline]
    pub fn is_set(&self, index: usize) -> bool {
        self.mask[index] == u8::get_max()
    }

    #[inline]
    pub fn is_unset(&self, index: usize) -> bool {
        self.mask[index] == u8::get_zero()
    }

    #[inline]
    pub fn index_of_first_set(&self) -> Option<usize> {
        (0..self.len()).find(|&i| self.mask[i] == u8::get_max())
    }

    #[inline]
    pub fn to_u8_unchecked(&self) -> u8 {
        let mut the_u8 = 0_u8;
        for i in 0..self.len() {
            if self.mask[i] == u8::get_max() {
                the_u8 |= (1 << i) as u8;
            }
        }
        the_u8
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClearBoolMask
////////////////////////////////////////////////////////////////////////////////

impl ClearBoolMask {
    #[inline]
    pub fn from_vec(mask: Vec<bool>) -> Self {
        ClearBoolMask { mask }
    }

    #[inline]
    pub fn is_set(&self, index: usize) -> bool {
        self.mask[index] == bool::get_max()
    }

    #[inline]
    pub fn is_unset(&self, index: usize) -> bool {
        self.mask[index] == bool::get_zero()
    }

    #[inline]
    pub fn index_of_first_set(&self) -> Option<usize> {
        (0..self.len()).find(|&i| self.mask[i] == bool::get_max())
    }

    #[inline]
    pub fn count_set(&self) -> usize {
        let mut count: usize = 0;
        for i in 0..self.len() {
            if self.mask[i] == bool::get_max() {
                count += 1;
            }
        }
        count
    }

    #[inline]
    pub fn to_u8_unchecked(&self) -> u8 {
        let mut the_u8 = 0_u8;
        for i in 0..self.len() {
            if self.mask[i] == bool::get_max() {
                the_u8 |= (1 << i) as u8;
            }
        }
        the_u8
    }

    #[inline]
    pub fn serial_and_or(&self, rhs: &ClearBoolMask) -> bool {
        assert_eq!(self.len(), rhs.len());
        let mut b = false;
        for i in 0..self.len() {
            b |= self.mask[i] & rhs.mask[i];
        }
        b
    }
}

////////////////////////////////////////////////////////////////////////////////
// MaskMatrix
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MaskMatrix<T> {
    pub columns: Vec<Mask<T>>,
}

pub type ByteMaskMatrix<U8> = MaskMatrix<U8>;
pub type ClearByteMaskMatrix = MaskMatrix<u8>;

derive1_encrypt_decrypt! { MaskMatrix<T> {columns:Vec<Mask<T>>} }

////////////////////////////////////////////////////////////////////////////////

impl<T> MaskMatrix<T> {
    #[inline]
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    #[inline]
    pub fn num_rows(&self) -> usize {
        match self.columns.first() {
            Some(c) => c.len(),
            None => 0,
        }
    }
}

impl<T> MaskMatrix<T>
where
    T: ThreadSafeUInt,
{
    fn row_mask(&self, index: usize) -> Mask<T> {
        let mut r = Mask::<T>::none(self.columns.len());
        self.columns.iter().enumerate().for_each(|(i, c)| {
            r.mask[i] = c.mask[index].clone();
        });
        r
    }

    #[inline]
    fn none(n_rows: usize, n_cols: usize) -> Self {
        MaskMatrix {
            columns: vec![Mask::none(n_rows); n_cols],
        }
    }

    #[inline]
    fn one_col(column: Mask<T>) -> Self {
        MaskMatrix {
            columns: vec![column],
        }
    }

    #[cfg(test)]
    #[inline]
    fn set(&mut self, row: usize, col: usize) {
        self.columns[col].set(row);
    }

    #[cfg(test)]
    #[inline]
    fn set_col(&mut self, col: usize) {
        self.columns[col].set_all();
    }

    #[cfg(test)]
    #[inline]
    fn set_row(&mut self, row: usize) {
        self.columns.iter_mut().for_each(|c| {
            c.set(row);
        });
    }

    pub fn par_vec_and_vec(row_mask: &Mask<T>, col_mask: &Mask<T>) -> Self {
        if col_mask.len() == 1 {
            return Self::one_col(row_mask.clone());
        }
        let mut m = Self::none(row_mask.len(), col_mask.len());
        m.columns
            .par_iter_mut()
            .zip(col_mask.mask.par_iter())
            .for_each(|(dst_column, col_mask)| {
                dst_column
                    .mask
                    .par_iter_mut()
                    .zip(row_mask.mask.par_iter())
                    .for_each(|(dst_mask, rm)| {
                        *dst_mask = col_mask.refref_bitand(rm);
                    })
            });
        m
    }
}

impl<T, U> ValueFrom<&MaskMatrix<T>> for MaskMatrix<U>
where
    for<'a> U: ValueFrom<&'a T> + Send,
    T: Send + Sync,
{
    fn value_from(value: &MaskMatrix<T>) -> Self {
        let columns = Vec::<Mask<U>>::value_from(&value.columns);
        MaskMatrix::<U> { columns }
    }
}

impl<T> RefBitAnd for MaskMatrix<T>
where
    T: RefBitAnd<Output = T> + Send + Sync + Clone + DefaultInto<T>,
{
    type Output = Self;

    fn ref_bitand(&self, rhs: Self) -> Self::Output {
        let columns: Vec<Mask<T>> = self
            .columns
            .par_iter()
            .zip(rhs.columns.par_iter())
            .map(|(left, right)| left.refref_bitand(right))
            .collect();
        MaskMatrix::<T> { columns }
    }

    fn refref_bitand(&self, rhs: &Self) -> Self::Output {
        let columns: Vec<Mask<T>> = self
            .columns
            .par_iter()
            .zip(rhs.columns.par_iter())
            .map(|(left, right)| left.refref_bitand(right))
            .collect();
        MaskMatrix::<T> { columns }
    }
}

impl<T> RefBitOr for MaskMatrix<T>
where
    T: RefBitOr<Output = T> + Send + Sync + Clone + DefaultInto<T> + ValueFrom<T>,
{
    type Output = Self;

    fn ref_bitor(&self, rhs: Self) -> Self::Output {
        // let columns: Vec<Mask<T>> = self
        //     .columns
        //     .par_iter()
        //     .zip(rhs.columns.par_iter())
        //     .map(|(left, right)| left.refref_bitor(right))
        //     .collect();
        // MaskMatrix::<T> { columns }
        self.refref_bitor(&rhs)
    }

    fn refref_bitor(&self, rhs: &Self) -> Self::Output {
        let columns: Vec<Mask<T>> = self
            .columns
            .par_iter()
            .zip(rhs.columns.par_iter())
            .map(|(left, right)| left.refref_bitor(right))
            .collect();
        MaskMatrix::<T> { columns }
    }
}

impl<T> RefNot for MaskMatrix<T>
where
    T: RefNot + Send + Sync,
{
    fn ref_not(&self) -> MaskMatrix<T> {
        let columns: Vec<Mask<T>> = self.columns.par_iter().map(|x| x.ref_not()).collect();
        MaskMatrix::<T> { columns }
    }
}

impl std::fmt::Display for ClearByteMaskMatrix {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut s = vec![];
        let n = self.num_rows();
        for i in 0..n {
            s.push(format!("{}", self.row_mask(i)));
        }
        f.write_str(s.join("\n").as_str())
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        // 1 x (0,0,0,0) = 0,0,0,0
        // 1               0,0,0,0
        // 1               0,0,0,0
        // 1               0,0,0,0
        let row_mask = Mask::<u8>::all(4);
        let col_mask = Mask::<u8>::none(4);
        let matrix = MaskMatrix::par_vec_and_vec(&row_mask, &col_mask);
        assert_eq!(matrix, MaskMatrix::none(4, 4));

        let row_mask = Mask::<[u8; 3]>::all(4);
        let col_mask = Mask::<[u8; 3]>::none(4);
        let matrix = MaskMatrix::par_vec_and_vec(&row_mask, &col_mask);
        assert_eq!(matrix, MaskMatrix::none(4, 4));

        // 0 x (1,0,0,0) = 0,0,0,0
        // 1               1,0,0,0
        // 0               0,0,0,0
        // 0               0,0,0,0
        let mut row_mask = Mask::<u8>::none(4);
        let mut col_mask = Mask::<u8>::none(4);
        row_mask.set(1);
        col_mask.set(0);
        let matrix = MaskMatrix::par_vec_and_vec(&row_mask, &col_mask);
        let mut expected_matrix = MaskMatrix::<u8>::none(4, 4);
        expected_matrix.set(1, 0);
        assert_eq!(matrix, expected_matrix);

        let mut row_mask = Mask::<[u8; 3]>::none(4);
        let mut col_mask = Mask::<[u8; 3]>::none(4);
        row_mask.set(1);
        col_mask.set(0);
        let matrix = MaskMatrix::par_vec_and_vec(&row_mask, &col_mask);
        let mut expected_matrix = MaskMatrix::<[u8; 3]>::none(4, 4);
        expected_matrix.set(1, 0);
        assert_eq!(matrix, expected_matrix);

        // 1 x (1,0,0,0) = 1,0,0,0
        // 1               1,0,0,0
        // 1               1,0,0,0
        // 1               1,0,0,0
        let row_mask = Mask::<u8>::all(4);
        let mut col_mask = Mask::<u8>::none(4);
        col_mask.set(0);
        let matrix = MaskMatrix::par_vec_and_vec(&row_mask, &col_mask);
        let mut expected_matrix = MaskMatrix::<u8>::none(4, 4);
        expected_matrix.set_col(0);
        assert_eq!(matrix, expected_matrix);

        let row_mask = Mask::<[u8; 3]>::all(4);
        let mut col_mask = Mask::<[u8; 3]>::none(4);
        col_mask.set(0);
        let matrix = MaskMatrix::par_vec_and_vec(&row_mask, &col_mask);
        let mut expected_matrix = MaskMatrix::<[u8; 3]>::none(4, 4);
        expected_matrix.set_col(0);
        assert_eq!(matrix, expected_matrix);

        // 1 x (1,1,1,1) = 1,1,1,1
        // 0               0,0,0,0
        // 0               0,0,0,0
        // 0               0,0,0,0
        let mut row_mask = Mask::<u8>::none(4);
        let col_mask = Mask::<u8>::all(4);
        row_mask.set(0);
        let matrix = MaskMatrix::par_vec_and_vec(&row_mask, &col_mask);
        let mut expected_matrix = MaskMatrix::<u8>::none(4, 4);
        expected_matrix.set_row(0);
        assert_eq!(matrix, expected_matrix);

        let mut row_mask = Mask::<[u8; 3]>::none(4);
        let col_mask = Mask::<[u8; 3]>::all(4);
        row_mask.set(0);
        let matrix = MaskMatrix::par_vec_and_vec(&row_mask, &col_mask);
        let mut expected_matrix = MaskMatrix::<[u8; 3]>::none(4, 4);
        expected_matrix.set_row(0);
        assert_eq!(matrix, expected_matrix);
    }
}
