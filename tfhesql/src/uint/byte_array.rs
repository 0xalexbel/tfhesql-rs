use super::byte_mask::ByteMask;
use super::byte_mask::ClearBoolMask;
use crate::ascii::le_u8x32_to_string;
use crate::bitops::*;
use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::types::*;
use crate::utils::arrow::array_read_columns_from_columns;
use crate::utils::arrow::array_read_columns_from_rows;
use crate::FheSqlError;
use arrow_array::RecordBatch;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;
use rayon::iter::*;
use std::mem::take;
use std::sync::Arc;

////////////////////////////////////////////////////////////////////////////////
// ByteArray
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ByteArray<U8> {
    pub(crate) bytes: Vec<U8>,
}

derive1_encrypt_decrypt! { ByteArray<U8> {bytes: Vec<U8>} }

pub type ClearByteArray = ByteArray<u8>;

////////////////////////////////////////////////////////////////////////////////

impl<U8> Default for ByteArray<U8> {
    fn default() -> Self {
        Self { bytes: vec![] }
    }
}

impl<U8> ByteArray<U8> {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    #[inline]
    pub fn from_bytes(bytes: Vec<U8>) -> Self {
        ByteArray::<U8> { bytes }
    }
}

impl<U8> ByteArray<U8>
where
    U8: Clone,
{
    pub fn alloc(len: usize, default: U8) -> Self {
        ByteArray::<U8> {
            bytes: vec![default; len],
        }
    }
}

impl<U8> ByteArray<U8>
where
    U8: ThreadSafeUInt,
{
    // When using padding
    #[allow(dead_code)]
    pub fn par_extend(&mut self, to_len: usize, encrypted_value: U8) {
        if self.len() >= to_len {
            return;
        }
        self.bytes.par_extend(
            (0..to_len - self.len())
                .into_par_iter()
                .map(|_| encrypted_value.ref_bitand(U8::get_zero())),
        )
    }
}

impl<FromU8, ToU8> RefBitOr<ByteArray<ToU8>> for ByteArray<FromU8>
where
    FromU8: Clone + Sync + Send + RefBitOr<ToU8, Output = ToU8>,
    ToU8: Clone + Send + Sync + ValueFrom<FromU8> + DefaultInto<ToU8>,
{
    type Output = ByteArray<ToU8>;

    fn ref_bitor(&self, rhs: ByteArray<ToU8>) -> ByteArray<ToU8> {
        self.refref_bitor(&rhs)
    }

    fn refref_bitor(&self, rhs: &ByteArray<ToU8>) -> ByteArray<ToU8> {
        let min_len = self.len().min(rhs.len());
        let mut row = ByteArray::<ToU8>::alloc(min_len, ToU8::default_into());
        row.bytes
            .par_iter_mut()
            .zip(self.bytes.par_iter().zip(rhs.bytes.par_iter()))
            .for_each(|(dst, (lhs, rhs))| *dst = lhs.refref_bitor(rhs));
        if self.len() == rhs.len() {
            return row;
        }

        if min_len == self.len() {
            for i in min_len..rhs.len() {
                row.bytes.push(rhs.bytes[i].clone())
            }
        } else {
            assert_eq!(min_len, rhs.len());
            for i in min_len..self.len() {
                row.bytes.push(ToU8::value_from(self.bytes[i].clone()))
            }
        }
        row
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClearByteArray
////////////////////////////////////////////////////////////////////////////////

impl ClearByteArray {
    #[inline(always)]
    pub fn get(&self, index: usize) -> u8 {
        self.bytes[index]
    }

    pub(crate) fn par_bitand<U8>(&self, mask: &U8) -> ByteArray<U8>
    where
        U8: Send + Sync + RefBitAnd<Output = U8> + Clone + ValueFrom<u8>,
    {
        let mut arr = ByteArray::<U8>::alloc(self.len(), mask.clone());
        arr.bytes
            .par_iter_mut()
            .zip(self.bytes.par_iter())
            .for_each(|(dst, clear_byte)| {
                *dst = dst.ref_bitand(U8::value_from(*clear_byte));
            });
        arr
    }

    pub(crate) fn last_index_of(&self, value: u8) -> Option<usize> {
        self.bytes
            .iter()
            .enumerate()
            .rev()
            .find(|x| x.1 == &value)
            .map(|(idx, _)| idx)
    }

    pub(crate) fn push(&mut self, value: u8) {
        self.bytes.push(value)
    }

    pub fn compress(&self) -> ClearByteArray {
        // Compress using ZLib
        use flate2::write::ZlibEncoder;
        use std::io::prelude::*;
        let mut e = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(&self.bytes).unwrap();
        ClearByteArray {
            bytes: e.finish().unwrap(),
        }
    }

    pub fn decompress(self) -> Result<ClearByteArray, FheSqlError> {
        // Decompress using ZLib
        use flate2::read::ZlibDecoder;
        use std::io::prelude::*;
        let mut d = ZlibDecoder::new(self.bytes.as_slice());
        let mut bytes: Vec<u8> = vec![];
        match d.read_to_end(&mut bytes) {
            Ok(_) => Ok(ClearByteArray { bytes }),
            Err(_) => Err(FheSqlError::DecryptError(
                "RecordBatch decompression failed".to_string(),
            )),
        }
    }

    fn extract_record_batch_byte_array(
        &mut self,
        compressed: bool,
    ) -> Result<ClearByteArray, FheSqlError> {
        // Find EOF Before decompression
        let n = self.last_index_of(u8::MAX).unwrap_or(0);

        let decompressed_byte_array: ClearByteArray;
        if n == 0 {
            take(&mut self.bytes);
            decompressed_byte_array = ClearByteArray::default();
        } else {
            self.bytes.truncate(n);
            assert_eq!(self.len(), n);
            if compressed {
                let compressed_byte_array = ClearByteArray {
                    bytes: take(&mut self.bytes),
                };
                decompressed_byte_array = compressed_byte_array.decompress()?
            } else {
                decompressed_byte_array = ClearByteArray {
                    bytes: take(&mut self.bytes),
                };
            }
        }

        Ok(decompressed_byte_array)
    }

    #[inline]
    pub(crate) fn write_u64(&mut self, value: usize) {
        (value as u64).to_le_bytes().iter().for_each(|u8_ref| {
            self.push(*u8_ref);
        });
    }
    #[inline]
    pub(crate) fn read_u64(&self, offset: usize) -> usize {
        let u8x8: [u8; 8] = std::array::from_fn(|i| self.bytes[i + offset]);
        u64::from_le_bytes(u8x8) as usize
    }
    #[inline]
    pub(crate) fn write_header(&mut self, num_rows: usize, num_cols: usize) {
        self.write_u64(num_rows);
        self.write_u64(num_cols);
    }
    #[inline]
    pub(crate) fn read_header(&self) -> (usize, usize, usize) {
        let num_rows = self.read_u64(0);
        let num_cols = self.read_u64(8);
        (16, num_rows, num_cols)
    }

    fn byte_array_to_byte_rows(byte_array: ClearByteArray) -> Vec<ClearByteArray> {
        let (offset, num_rows, _) = byte_array.read_header();
        let row_w = byte_array.read_u64(offset);
        assert_eq!(row_w * num_rows, byte_array.len() - offset - 8);
        let mut rows = vec![];

        let raw_bytes_rows = &byte_array.bytes.as_slice()[offset + 8..];
        let mut i = 0;
        let mut j = 0;
        loop {
            let a = ClearByteArray::from_bytes(raw_bytes_rows[i..i + row_w].to_vec());
            rows.push(a);

            i += row_w;
            j += 1;

            if j == num_rows {
                assert_eq!(i, raw_bytes_rows.len());
                break;
            }
        }

        rows
    }

    fn byte_array_to_byte_columns(byte_array: ClearByteArray) -> Vec<ClearByteArray> {
        let (offset, num_rows, num_cols) = byte_array.read_header();

        let mut cols = vec![];

        let raw_bytes_columns = &byte_array.bytes.as_slice()[offset..];
        let mut i = 0;
        let mut j = 0;
        loop {
            // Read column width
            let col_w = raw_bytes_columns[i] as usize;
            let sz = col_w * num_rows;
            i += 1;

            let a = ClearByteArray::from_bytes(raw_bytes_columns[i..i + sz].to_vec());
            cols.push(a);

            i += sz;
            j += 1;

            if j == num_cols {
                assert_eq!(i, raw_bytes_columns.len());
                break;
            }
        }

        cols
    }

    pub(crate) fn extract_record_batch(
        mut self,
        schema_ref: &SchemaRef,
        field_mask: &ClearBoolMask,
        select_mask: &ClearBoolMask,
        in_row_order: bool,
        compressed: bool,
    ) -> Result<RecordBatch, FheSqlError> {
        let decompressed_byte_array = self.extract_record_batch_byte_array(compressed)?;

        let (columns, fields);
        if in_row_order {
            let byte_rows = Self::byte_array_to_byte_rows(decompressed_byte_array);
            assert!(byte_rows.len() <= select_mask.len());
            (columns, fields) =
                array_read_columns_from_rows(schema_ref, field_mask, select_mask, byte_rows);
        } else {
            let byte_columns = Self::byte_array_to_byte_columns(decompressed_byte_array);
            (columns, fields) =
                array_read_columns_from_columns(schema_ref, field_mask, select_mask, byte_columns);
        }

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

    pub fn into_bool_vec(self, select_mask: &ClearBoolMask) -> Vec<bool> {
        assert_eq!(self.len(), select_mask.len());
        self.bytes
            .iter()
            .enumerate()
            .filter(|x| select_mask.is_set(x.0))
            .map(|(_, u8x1)| u8x1 != &0)
            .collect()
    }
    pub fn into_i8_vec(self, select_mask: &ClearBoolMask) -> Vec<i8> {
        self.bytes
            .iter()
            .enumerate()
            .filter(|x| select_mask.is_set(x.0))
            .map(|(_, u8x1)| i8::from_le_bytes([*u8x1]))
            .collect()
    }
    pub fn into_u8_vec(self, select_mask: &ClearBoolMask) -> Vec<u8> {
        self.bytes
            .iter()
            .enumerate()
            .filter(|x| select_mask.is_set(x.0))
            .map(|(_, u8x1)| u8::from_le_bytes([*u8x1]))
            .collect()
    }
    pub fn into_i16_vec(self, select_mask: &ClearBoolMask) -> Vec<i16> {
        use rayon::slice::ParallelSlice;
        self.bytes
            .par_chunks_exact(2)
            .enumerate()
            .filter(|x| select_mask.is_set(x.0))
            .map(|(_, u8x2)| i16::from_le_bytes([u8x2[0], u8x2[1]]))
            .collect()
    }
    pub fn into_u16_vec(self, select_mask: &ClearBoolMask) -> Vec<u16> {
        use rayon::slice::ParallelSlice;
        self.bytes
            .par_chunks_exact(2)
            .enumerate()
            .filter(|x| select_mask.is_set(x.0))
            .map(|(_, u8x2)| u16::from_le_bytes([u8x2[0], u8x2[1]]))
            .collect()
    }
    pub fn into_i32_vec(self, select_mask: &ClearBoolMask) -> Vec<i32> {
        use rayon::slice::ParallelSlice;
        self.bytes
            .par_chunks_exact(4)
            .enumerate()
            .filter(|x| select_mask.is_set(x.0))
            .map(|(_, u8x4)| i32::from_le_bytes([u8x4[0], u8x4[1], u8x4[2], u8x4[3]]))
            .collect()
    }
    pub fn into_u32_vec(self, select_mask: &ClearBoolMask) -> Vec<u32> {
        use rayon::slice::ParallelSlice;
        self.bytes
            .par_chunks_exact(4)
            .enumerate()
            .filter(|x| select_mask.is_set(x.0))
            .map(|(_, u8x4)| u32::from_le_bytes([u8x4[0], u8x4[1], u8x4[2], u8x4[3]]))
            .collect()
    }
    pub fn into_i64_vec(self, select_mask: &ClearBoolMask) -> Vec<i64> {
        use rayon::slice::ParallelSlice;
        self.bytes
            .par_chunks_exact(8)
            .enumerate()
            .filter(|x| select_mask.is_set(x.0))
            .map(|(_, u8x8)| {
                i64::from_le_bytes([
                    u8x8[0], u8x8[1], u8x8[2], u8x8[3], u8x8[4], u8x8[5], u8x8[6], u8x8[7],
                ])
            })
            .collect()
    }
    pub fn into_u64_vec(self, select_mask: &ClearBoolMask) -> Vec<u64> {
        use rayon::slice::ParallelSlice;
        self.bytes
            .par_chunks_exact(8)
            .enumerate()
            .filter(|x| select_mask.is_set(x.0))
            .map(|(_, u8x8)| {
                u64::from_le_bytes([
                    u8x8[0], u8x8[1], u8x8[2], u8x8[3], u8x8[4], u8x8[5], u8x8[6], u8x8[7],
                ])
            })
            .collect()
    }
    pub fn into_ascii32_vec(self, select_mask: &ClearBoolMask) -> Vec<String> {
        use rayon::slice::ParallelSlice;
        self.bytes
            .par_chunks_exact(32)
            .enumerate()
            .filter(|x| select_mask.is_set(x.0))
            .map(|(_, x)| {
                let u8x32: [u8; 32] = std::array::from_fn(|i| x[i]);
                le_u8x32_to_string(&u8x32)
            })
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////
// ByteArrayList
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteArrayList<U8> {
    pub(crate) list: Vec<ByteArray<U8>>,
}

derive1_encrypt_decrypt! { ByteArrayList<U8> {list: Vec<ByteArray<U8>>} }

pub type ClearByteArrayList = ByteArrayList<u8>;

////////////////////////////////////////////////////////////////////////////////

impl<U8> ByteArrayList<U8> {
    #[inline]
    pub fn len(&self) -> usize {
        self.list.len()
    }

    #[inline]
    pub fn byte(&self, index: usize, byte_index: usize) -> Option<&U8> {
        self.list[index].bytes.get(byte_index)
    }

    pub fn max_len(&self) -> usize {
        self.list.iter().fold(0, |acc, a| acc.max(a.len()))
    }

    // Only for debug and analysis
    #[allow(dead_code)]
    pub fn count_mem(&self) -> usize {
        self.list.iter().fold(0, |acc, a| acc + a.len())
    }
}

impl<U8> ByteArrayList<U8>
where
    U8: Clone,
{
    pub fn alloc(len: usize) -> Self {
        ByteArrayList::<U8> {
            list: vec![ByteArray::<U8>::default(); len],
        }
    }
}

impl<U8> ByteArrayList<U8>
where
    U8: Send
        + Sync
        + RefBitAnd<Output = U8>
        + RefBitOr<Output = U8>
        + Clone
        + ValueFrom<u8>
        + DefaultInto<U8>,
{
    pub(crate) fn par_flatten(mut self) -> ByteArray<U8> {
        assert!(self.len() >= 1);
        if self.len() == 1 {
            return take(&mut self.list[0]);
        }
        // Allocate a new ByteArray which size equals the maximum size of each listed byte array
        let mut arr = ByteArray::<U8>::alloc(self.max_len(), U8::value_from(0));
        // Performs a byte by byte OR between each listed byte array
        arr.bytes
            .par_iter_mut()
            .enumerate()
            .for_each(|(byte_index, dst)| {
                let mut bytes = vec![];
                for index in 0..self.len() {
                    if let Some(b) = self.byte(index, byte_index) {
                        bytes.push(b)
                    }
                }
                assert_ne!(bytes.len(), 0);
                *dst = par_bitor_vec_ref(bytes).unwrap();
            });
        arr
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClearByteArrayList
////////////////////////////////////////////////////////////////////////////////

impl ClearByteArrayList {
    pub fn par_bitand<U8>(self, mask: &ByteMask<U8>) -> ByteArrayList<U8>
    where
        U8: Send
            + Sync
            + RefBitAnd<Output = U8>
            + RefBitOr<Output = U8>
            + Clone
            + ValueFrom<u8>
            + DefaultInto<U8>,
        //        U8: Send + Sync + RefBitAnd<Output = U8> + Clone + ValueFrom<u8>,
    {
        assert!(self.list.len() == mask.len());
        let mut bal = ByteArrayList::<U8>::alloc(mask.len());
        bal.list
            .par_iter_mut()
            .zip(self.list.par_iter().zip(mask.mask.par_iter()))
            .for_each(|(dst, (clear_array, m))| {
                *dst = clear_array.par_bitand(m);
            });

        bal
    }
}
