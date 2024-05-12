use rayon::iter::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::usize;

use crate::bitops::{par_bitor_vec_ref, RefBitAnd, RefBitOr};
use crate::default_into::{DefaultInto, ValueFrom};
use crate::uint::mask::{ByteMask, ByteMaskMatrix, ClearBoolMask};
use crate::uint::{ByteArray, ClearByteArray};
use crate::utils::arrow::array_read_columns_from_rows;
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};

use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::{default_into::*, FheSqlError};

////////////////////////////////////////////////////////////////////////////////
// ByteRows
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteRows<U8> {
    rows: Vec<ByteArray<U8>>,
}

derive1_encrypt_decrypt! { ByteRows<U8> {rows: Vec<ByteArray<U8>>} }

pub(crate) type ClearByteRows = ByteRows<u8>;

////////////////////////////////////////////////////////////////////////////////

impl<U8> Default for ByteRows<U8> {
    fn default() -> Self {
        Self { rows: vec![] }
    }
}

impl<U8> ByteRows<U8> {
    #[inline]
    pub fn len(&self) -> usize {
        self.rows.len()
    }
    #[inline]
    pub fn from_byte_array_vec(byte_array_vec: Vec<ByteArray<U8>>) -> Self {
        ByteRows::<U8> {
            rows: byte_array_vec,
        }
    }
    #[inline]
    pub fn into_byte_array_vec(self) -> Vec<ByteArray<U8>> {
        self.rows
    }
    #[inline]
    pub fn rows_mut(&mut self) -> &mut Vec<ByteArray<U8>> {
        &mut self.rows
    }
    #[inline]
    pub fn get_row(&self, index: usize) -> Option<&ByteArray<U8>> {
        self.rows.get(index)
    }
    pub fn max_row_width(&self) -> usize {
        let mut max_w = 0;
        for i in 0..self.rows.len() {
            max_w = max_w.max(self.rows[i].len());
        }
        max_w
    }
    pub fn first_value(&self) -> Option<&U8> {
        match self.rows.iter().find(|r| !r.is_empty()) {
            Some(a) => Some(&a.bytes[0]),
            None => None,
        }
    }

    // Only for debug and analysis
    #[allow(dead_code)]
    pub fn count_mem(&self) -> usize {
        self.rows.iter().fold(0, |acc, a| acc + a.len())
    }
}

impl<U8> ByteRows<U8>
where
    U8: Clone,
{
    pub fn alloc(num_rows: usize) -> Self {
        ByteRows::<U8> {
            rows: vec![ByteArray::<U8>::default(); num_rows],
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClearByteRows
////////////////////////////////////////////////////////////////////////////////

impl ClearByteRows {
    pub(crate) fn par_bitand_rows_x_one<U8>(&self, rows_x_one_mask: &ByteMask<U8>) -> ByteRows<U8>
    where
        U8: Send + Sync + RefBitAnd<Output = U8> + Clone + ValueFrom<u8>,
    {
        assert!(self.rows.len() <= rows_x_one_mask.len());

        let mut t = ByteRows::<U8> {
            rows: vec![ByteArray::<U8>::default(); self.rows.len()],
        };

        t.rows
            .par_iter_mut()
            .zip(self.rows.par_iter().zip(rows_x_one_mask.mask.par_iter()))
            .for_each(|(dst, (clear_row, m))| *dst = clear_row.par_bitand(m));
        t
    }

    pub(crate) fn extract_record_batch(
        self,
        schema_ref: &SchemaRef,
        field_mask: &ClearBoolMask,
        select_mask: &ClearBoolMask,
        compressed: bool,
    ) -> Result<RecordBatch, FheSqlError> {
        let err_count: AtomicUsize = AtomicUsize::new(0);
        assert_eq!(select_mask.len(), self.len());

        let decompressed_byte_rows = self
            .rows
            .into_iter()
            .map(|mut byte_array| {
                // Find EOF Before decompression
                let n = byte_array.last_index_of(u8::MAX).unwrap_or(0);

                if n == 0 {
                    return ClearByteArray::default();
                }
                assert!(n > 0);

                byte_array.bytes.truncate(n);
                assert_eq!(byte_array.len(), n);

                if !compressed {
                    // Ignore EOF marker
                    return byte_array;
                }

                match byte_array.decompress() {
                    Ok(decompressed_byte_array) => decompressed_byte_array,
                    Err(_) => {
                        err_count.fetch_add(1, Ordering::Relaxed);
                        ClearByteArray::default()
                    }
                }
            })
            .collect::<Vec<ClearByteArray>>();

        let e = err_count.load(Ordering::Relaxed);
        if e > 0 {
            return Err(FheSqlError::DecryptError(format!(
                "RecordBatch decompression failed errors={}",
                e,
            )));
        }

        assert_eq!(decompressed_byte_rows.len(), select_mask.len());

        let (columns, fields) = array_read_columns_from_rows(
            schema_ref,
            field_mask,
            select_mask,
            decompressed_byte_rows,
        );

        let batch_schema = Schema::new(fields);
        let rb = match RecordBatch::try_new(Arc::new(batch_schema), columns) {
            Ok(rb) => rb,
            Err(_) => {
                return Err(FheSqlError::DecryptError(
                    "RecordBatch decompression failed".to_string(),
                ))
            }
        };

        Ok(rb)
    }
}

////////////////////////////////////////////////////////////////////////////////
// ByteRowsList
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteRowsList<U8> {
    list: Vec<ByteRows<U8>>,
}

pub type ClearByteRowsList = ByteRowsList<u8>;

////////////////////////////////////////////////////////////////////////////////

impl<U8> ByteRowsList<U8> {
    pub fn alloc(len: usize) -> Self {
        let mut v: Vec<ByteRows<U8>> = vec![];
        for _ in 0..len {
            v.push(ByteRows::<U8>::default());
        }
        ByteRowsList::<U8> { list: v }
    }

    #[inline]
    pub fn from_byte_rows(byte_rows: Vec<ByteRows<U8>>) -> Self {
        ByteRowsList::<U8> { list: byte_rows }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.list.len()
    }

    #[inline]
    pub fn row(&self, index: usize, row_index: usize) -> Option<&ByteArray<U8>> {
        self.list[index].get_row(row_index)
    }

    pub fn max_num_rows(&self) -> usize {
        let mut max_num = 0;
        for i in 0..self.len() {
            max_num = max_num.max(self.list[i].len());
        }
        max_num
    }

    // Only for debug and analysis
    #[allow(dead_code)]
    pub fn count_mem(&self) -> usize {
        self.list.iter().fold(0, |acc, t| acc + t.count_mem())
    }
}

impl<U8> ByteRowsList<U8>
where
    U8: Send
        + Sync
        + RefBitAnd<Output = U8>
        + RefBitOr<Output = U8>
        + Clone
        + ValueFrom<U8>
        + ValueFrom<u8>
        + DefaultInto<U8>,
{
    pub(crate) fn par_flatten_row_by_row(self, padding: bool) -> ByteRows<U8> {
        // Allocate a new ByteRows large enough to contain every listed byte rows
        let mut byte_rows = ByteRows::<U8>::alloc(self.max_num_rows());
        // Performs a row by row + byte by byte OR between each listed byte rows
        byte_rows
            .rows_mut()
            .par_iter_mut()
            .enumerate()
            .for_each(|(row_index, dst)| {
                // List all the rows required for the row by row OR operation
                let mut rows = vec![];
                for index in 0..self.len() {
                    if let Some(r) = self.row(index, row_index) {
                        rows.push(r)
                    }
                }
                assert_ne!(rows.len(), 0);
                // row by row OR
                *dst = par_bitor_vec_ref(rows).unwrap();
            });

        // Padding: add Zeros at the end of each rows so that all the rows will have the same len
        if padding {
            let max_row_width = byte_rows.max_row_width();
            let enc_v = match byte_rows.first_value() {
                Some(v) => v.clone(),
                None => return byte_rows,
            };

            byte_rows.rows_mut().par_iter_mut().for_each(|r| {
                if r.len() >= max_row_width {
                    assert_eq!(r.len(), max_row_width);
                    return;
                }
                r.bytes.par_extend(
                    (0..max_row_width - r.len())
                        .into_par_iter()
                        .map(|_| enc_v.ref_bitand(U8::value_from(0))),
                );
                assert_eq!(r.len(), max_row_width);
            });
        }

        byte_rows
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClearByteRowsList
////////////////////////////////////////////////////////////////////////////////

impl ClearByteRowsList {
    pub fn par_bitand<U8>(self, select_x_table_mask_matrix: &ByteMaskMatrix<U8>) -> ByteRowsList<U8>
    where
        U8: Send
            + Sync
            + RefBitAnd<Output = U8>
            + RefBitOr<Output = U8>
            + Clone
            + ValueFrom<u8>
            + DefaultInto<U8>,
    {
        assert!(self.list.len() == select_x_table_mask_matrix.num_columns());
        let mut obt = ByteRowsList::<U8>::alloc(self.len());
        obt.list
            .par_iter_mut()
            .zip(
                self.list
                    .par_iter()
                    .zip(select_x_table_mask_matrix.columns.par_iter()),
            )
            .for_each(|(dst, (clear_table, select_row_mask))| {
                *dst = clear_table.par_bitand_rows_x_one(select_row_mask)
            });

        obt
    }

    // pub(crate) fn num_ors<U8>(&self) -> usize
    // where
    //     U8: Send
    //         + Sync
    //         + RefBitAnd<Output = U8>
    //         + RefBitOr<Output = U8>
    //         + Clone
    //         + ValueFrom<u8>
    //         + DefaultInto<U8>,
    // {
    //     let mut tree = BTreeMap::<Vec<u8>, U8>::new();
    //     (0..self.max_num_rows()).into_iter().for_each(|row_index| {
    //         let mut rows = vec![];
    //         for index in 0..self.len() {
    //             match self.row(index, row_index) {
    //                 Some(r) => rows.push(r),
    //                 None => (),
    //             }
    //         }
    //         ClearByteArrayList::list_sorted_byte_columns_ref(&rows, &mut tree, U8::value_from(0));
    //     });
    //     let n = tree.keys().fold(0, |acc, k| {
    //         let non_zeros = k.iter().fold(0, |acc, a| if a != &0 { acc + 1 } else { acc } );
    //         if non_zeros <= 1 {
    //             acc
    //         } else {
    //             acc + non_zeros - 1
    //         }
    //     });
    //     println!("================================");
    //     println!(" Number of Keys = {}", n);
    //     println!("================================");
    //     n
    // }
}
