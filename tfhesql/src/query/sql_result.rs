use crate::csv::record_batch_to_csv_string;
use crate::encrypt::traits::{Decrypt, TryTrivialDecrypt};
use crate::error::FheSqlError;
use crate::table::byte_rows::ClearByteRows;
use crate::uint::mask::BoolMask;
use crate::uint::{ByteArray, ClearByteArray};
use crate::OrderedSchemas;
use crate::SqlResultOptions;
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use std::mem::swap;
use std::sync::Arc;
use tfhe::{ClientKey, FheBool, FheUint8};

use super::sql_query::SqlQueryRef;

#[cfg(feature = "stats")]
use crate::server::SqlStats;

////////////////////////////////////////////////////////////////////////////////
// SqlResult
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub(crate) struct SqlResult<U8, B> {
    /// Encrypted part
    table_mask: BoolMask<B>,
    field_mask: BoolMask<B>,
    select_mask: BoolMask<B>,
    byte_arrays: Vec<ByteArray<U8>>,

    /// Clear part
    pub(crate) options: SqlResultOptions,
    pub(crate) ordered_schemas: OrderedSchemas,

    #[cfg(feature = "stats")]
    #[serde(skip_serializing, skip_deserializing)]
    pub(crate) stats: SqlStats,
}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub struct ClearSqlResult(pub(crate) SqlResult<u8, bool>);

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub struct FheSqlResult(pub(crate) SqlResult<FheUint8, FheBool>);

////////////////////////////////////////////////////////////////////////////////

impl<U8, B> SqlResult<U8, B> {
    pub(crate) fn new_empty() -> Self {
        SqlResult::<U8, B> {
            table_mask: BoolMask::<B>::new_empty(),
            field_mask: BoolMask::<B>::new_empty(),
            select_mask: BoolMask::<B>::new_empty(),
            byte_arrays: vec![],

            options: SqlResultOptions::default(),
            ordered_schemas: OrderedSchemas::new_empty(),

            #[cfg(feature = "stats")]
            stats: SqlStats::new_empty(),
        }
    }
    fn is_empty(&self) -> bool {
        self.table_mask.len() == 0
    }

    #[inline]
    pub fn to_json<W>(&self, writer: W) -> serde_json::Result<()>
    where
        W: std::io::Write,
        U8: serde::Serialize,
        B: serde::Serialize,
    {
        serde_json::to_writer(writer, self)
    }
}

impl<U8, B> SqlResult<U8, B>
where
    B: Clone,
{
    pub(crate) fn from_query_ref(
        query_ref: &SqlQueryRef<B>,
        select_mask: BoolMask<B>,
        byte_arrays: Vec<ByteArray<U8>>,
    ) -> Self {
        SqlResult::<U8, B> {
            table_mask: query_ref.header().table_mask.clone(),
            field_mask: query_ref.header().field_mask.clone(),
            select_mask,
            byte_arrays,

            #[cfg(feature = "stats")]
            stats: SqlStats::new_empty(),
            options: *query_ref.options(),
            ordered_schemas: query_ref.ordered_schemas().clone()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// FheSqlResult
////////////////////////////////////////////////////////////////////////////////

impl FheSqlResult {
    fn decrypt(&self, key: &ClientKey) -> ClearSqlResult {
        let table_mask = self.0.table_mask.decrypt(key);
        let field_mask = self.0.field_mask.decrypt(key);
        let select_mask = self.0.select_mask.decrypt(key);
        let byte_arrays = self.0.byte_arrays.decrypt(key);
        ClearSqlResult(SqlResult::<u8, bool> {
            table_mask,
            field_mask,
            select_mask,
            byte_arrays,
            #[cfg(feature = "stats")]
            stats: self.0.stats.clone(),
            options: self.0.options,
            ordered_schemas: self.0.ordered_schemas.clone()
        })
    }

    fn try_decrypt_trivial(
        &self,
    ) -> Result<ClearSqlResult, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
        let table_mask = self.0.table_mask.try_decrypt_trivial()?;
        let field_mask = self.0.field_mask.try_decrypt_trivial()?;
        let select_mask = self.0.select_mask.try_decrypt_trivial()?;
        let byte_arrays = self.0.byte_arrays.try_decrypt_trivial()?;
        Ok(ClearSqlResult(SqlResult::<u8, bool> {
            table_mask,
            field_mask,
            select_mask,
            byte_arrays,
            #[cfg(feature = "stats")]
            stats: self.0.stats.clone(),
            options: self.0.options,
            ordered_schemas: self.0.ordered_schemas.clone(),
        }))
    }

    /// Decrypts the trivialy encrypted sql result and returns
    /// an [arrow `RecordBatch`](arrow_array::record_batch::RecordBatch).
    pub fn try_decrypt_trivial_record_batch(
        &self,
    ) -> Result<RecordBatch, FheSqlError> {
        let clear_sql_result = match self.try_decrypt_trivial() {
            Ok(sql_res) => sql_res,
            Err(err) => return Err(FheSqlError::DecryptError(err.to_string())),
        };
        clear_sql_result.into_record_batch()
    }

    /// Decrypts the encrypted sql result and returns
    /// an [arrow `RecordBatch`](arrow_array::record_batch::RecordBatch).
    pub fn decrypt_record_batch(
        &self,
        key: &ClientKey,
    ) -> Result<RecordBatch, FheSqlError> {
        self.decrypt(key).into_record_batch()
    }

    /// Decrypts the encrypted sql result and returns a string containing the batch in csv format.
    pub fn decrypt_csv(
        &self,
        key: &ClientKey,
    ) -> Result<String, FheSqlError> {
        let rb = self.decrypt(key).into_record_batch()?;
        record_batch_to_csv_string(&rb)
    }

    /// Decrypts the trivialy encrypted sql result and returns a string containing the batch in csv format.
    pub fn try_decrypt_trivial_csv(
        &self,
    ) -> Result<String, FheSqlError> {
        let rb = self.try_decrypt_trivial_record_batch()?;
        record_batch_to_csv_string(&rb)
    }

    /// Helper: Ouputs the encrypted sql result in json format
    #[inline]
    pub fn to_json<W>(&self, writer: W) -> serde_json::Result<()>
    where
        W: std::io::Write,
    {
        self.0.to_json(writer)
    }
}

#[cfg(feature = "stats")]
impl FheSqlResult {
    pub fn print_stats(&self) {
        self.0.stats.total().print()
    }

    pub fn print_detailed_stats(&self) {
        self.0.stats.print()
    }
}

////////////////////////////////////////////////////////////////////////////////
// ClearSqlResult
////////////////////////////////////////////////////////////////////////////////

impl ClearSqlResult {
    /// Consumes the clear sql result and returns
    /// an [arrow `RecordBatch`](arrow_array::record_batch::RecordBatch).
    pub fn into_record_batch(
        self,
    ) -> Result<RecordBatch, FheSqlError> {
        let mut a = self;
        a.0.extract_record_batch()
    }

    /// Consumes the clear sql result and returns
    /// the sql result in csv format
    pub fn into_csv(
        self,
    ) -> Result<String, FheSqlError> {
        let rb = self.into_record_batch()?;
        record_batch_to_csv_string(&rb)
    }

    /// Helper: Ouputs the clear sql result in json format
    #[inline]
    pub fn to_json<W>(&self, writer: W) -> serde_json::Result<()>
    where
        W: std::io::Write,
    {
        self.0.to_json(writer)
    }
}

#[cfg(feature = "stats")]
impl ClearSqlResult {
    pub fn print_stats(&self) {
        self.0.stats.total().print()
    }

    pub fn print_detailed_stats(&self) {
        self.0.stats.print()
    }
}

impl SqlResult<u8, bool> {
    pub fn extract_record_batch(
        &mut self,
    ) -> Result<RecordBatch, FheSqlError> {
        if self.is_empty() {
            let empty_schema = Schema::new(Vec::<Field>::new());
            return Ok(RecordBatch::new_empty(Arc::new(empty_schema)));
        }
        let schema_index = self.table_mask.index_of_first_set().unwrap();
        if schema_index >= self.ordered_schemas.len() {
            return Err(FheSqlError::DecryptError(format!(
                "Out of bounds table schema index, got '{}', only {} schemas are declared",
                schema_index,
                self.ordered_schemas.len()
            )));
        }

        match self.options.format() {
            crate::SqlResultFormat::RowBytes(_) => {
                let mut my_byte_array: Vec<ClearByteArray> = vec![];
                swap(&mut my_byte_array, &mut self.byte_arrays);
                let byte_rows = ClearByteRows::from_byte_array_vec(my_byte_array);
                assert_eq!(self.select_mask.len(), byte_rows.len());
                byte_rows.extract_record_batch(
                    self.ordered_schemas.schema(schema_index),
                    &self.field_mask,
                    &self.select_mask,
                    self.options.compress(),
                )
            }
            crate::SqlResultFormat::TableBytesInRowOrder
            | crate::SqlResultFormat::TableBytesInColumnOrder => {
                assert_eq!(self.byte_arrays.len(), 1);
                let byte_array = self.byte_arrays.remove(0);
                byte_array.extract_record_batch(
                    self.ordered_schemas.schema(schema_index),
                    &self.field_mask,
                    &self.select_mask,
                    self.options.in_row_order(),
                    self.options.compress(),
                )
            }
        }
    }
}
