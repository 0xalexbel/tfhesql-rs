use crate::default_into::{DefaultInto, ValueFrom};
use crate::query::sql_query::SqlQueryRef;
use crate::query::sql_result::SqlResult;
use crate::server::ident_compare_with::IdentCompareWithArray;
use crate::types::*;
use crate::uint::mask::{BoolMask, ByteMaskMatrix, Mask};
use crate::ClearSqlQuery;
use crate::ClearSqlResult;
use crate::FheSqlError;
use crate::FheSqlQuery;
use crate::FheSqlResult;
use crate::OrderedTables;
use crate::SqlResultFormat;
use std::{fmt::Debug, marker::PhantomData, ops::BitOrAssign, sync::Arc};
use tfhe::{FheBool, FheUint8};

use super::distinct::compute_select_distinct;

#[cfg(feature = "stats")]
use crate::server::SqlStats;

////////////////////////////////////////////////////////////////////////////////
// FheSqlServer
////////////////////////////////////////////////////////////////////////////////

pub struct FheSqlServer {}

pub trait FheRunSqlQuery<Q> {
    type Result;
    fn run(query: &Q, tables: &OrderedTables) -> Result<Self::Result, FheSqlError>;
}

impl FheRunSqlQuery<ClearSqlQuery> for FheSqlServer {
    type Result = ClearSqlResult;

    fn run(query: &ClearSqlQuery, tables: &OrderedTables) -> Result<Self::Result, FheSqlError> {
        Ok(ClearSqlResult(SqlServer::<u8, bool>::run(
            Arc::new(query.clone()),
            tables,
        )?))
    }
}

impl FheRunSqlQuery<FheSqlQuery> for FheSqlServer {
    type Result = FheSqlResult;

    fn run(query: &FheSqlQuery, tables: &OrderedTables) -> Result<Self::Result, FheSqlError> {
        Ok(FheSqlResult(SqlServer::<FheUint8, FheBool>::run(
            Arc::new(query.clone()),
            tables,
        )?))
    }
}

////////////////////////////////////////////////////////////////////////////////
// SqlServer
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct SqlServer<U8, B> {
    phantom_t: PhantomData<U8>,
    phantom_b: PhantomData<B>,

    #[cfg(feature = "stats")]
    stats: SqlStats,
}

////////////////////////////////////////////////////////////////////////////////

impl<U8, B> SqlServer<U8, B> {
    #[inline]
    fn new() -> Self {
        SqlServer {
            phantom_t: Default::default(),
            phantom_b: Default::default(),
            #[cfg(feature = "stats")]
            stats: SqlStats::new_empty(),
        }
    }
}

impl<U8, B> SqlServer<U8, B>
where
    B: ThreadSafeUInt + ThreadSafeBool + DefaultInto<B> + ValueFrom<B>,
    for<'a> U8: ThreadSafeUInt
        + ValueFrom<&'a B>
        + ValueFrom<u8>
        + BitOrAssign
        + DefaultInto<U8>
        + ValueFrom<U8>,
{
    fn run(
        query_ref: SqlQueryRef<B>,
        tables: &OrderedTables,
    ) -> Result<SqlResult<U8, B>, FheSqlError> {
        if tables.ordered_schemas() != query_ref.ordered_schemas() {
            return Err(FheSqlError::InvalidQueryError(
                "The requested query schemas and tables schemas are incompatible".to_string(),
            ));
        }

        let mut srv = SqlServer::<U8, B>::new();

        #[cfg(feature = "stats")]
        {
            let stats = PerfStats::new("Total");
            let result = srv.inner_run(query_ref, tables);
            srv.stats_finish(stats, result)
        }

        #[cfg(not(feature = "stats"))]
        srv.inner_run(query_ref, tables)
    }

    fn inner_run(
        &mut self,
        query_ref: SqlQueryRef<B>,
        tables: &OrderedTables,
    ) -> Result<SqlResult<U8, B>, FheSqlError> {
        if query_ref.is_empty() {
            return Ok(SqlResult::<U8, B>::new_empty());
        }

        let select_mask = self.compute_select_mask(query_ref.clone(), tables)?;

        self.compute_result(query_ref, tables, select_mask)
    }

    fn compute_result(
        &mut self,
        query_ref: SqlQueryRef<B>,
        tables: &OrderedTables,
        select_mask: BoolMask<B>,
    ) -> Result<SqlResult<U8, B>, FheSqlError>
    where
        B: ThreadSafeUInt,
        U8: ValueFrom<U8> + DefaultInto<U8>,
    {
        #[cfg(feature = "stats")]
        let stats = PerfStats::new("Compute Result");

        assert_eq!(select_mask.len(), tables.max_num_rows());

        let (byte_table_mask, byte_select_mask) = rayon::join(
            || Mask::<U8>::value_from(&query_ref.header().table_mask),
            || Mask::<U8>::value_from(&select_mask),
        );

        let enc_byte_arrays = match query_ref.options().format() {
            SqlResultFormat::RowBytes(padding) => {
                let select_by_table_byte_matrix =
                    ByteMaskMatrix::<U8>::par_vec_and_vec(&byte_select_mask, &byte_table_mask);

                // Convert each table into a list of byte rows
                let clear_byte_rows_list = tables.to_byte_rows_list(query_ref.options().compress());

                // Apply Table(t) AND Select(r) to each row of each previously converted table.
                assert!(clear_byte_rows_list.len() == select_by_table_byte_matrix.num_columns());
                let enc_masked_byte_rows_list =
                    clear_byte_rows_list.par_bitand(&select_by_table_byte_matrix);

                // Flatten the list of tables (as list of byte rows) into a single list of byte rows
                let enc_byte_rows = enc_masked_byte_rows_list.par_flatten_row_by_row(padding);
                assert_eq!(select_mask.len(), enc_byte_rows.len());
                enc_byte_rows.into_byte_array_vec()
            }
            SqlResultFormat::TableBytesInRowOrder | SqlResultFormat::TableBytesInColumnOrder => {
                // Convert each table into a single byte array
                let clear_byte_array_list = tables.to_byte_array_list(
                    query_ref.options().in_row_order(),
                    query_ref.options().compress(),
                );

                // Apply Table(t) mask to each previously converted table.
                assert!(clear_byte_array_list.len() == byte_table_mask.len());
                let enc_masked_byte_array_list = clear_byte_array_list.par_bitand(&byte_table_mask);

                // Flatten the list of tables (as list of byte) into a single list of bytes
                vec![enc_masked_byte_array_list.par_flatten()]
            }
        };

        let result = SqlResult::<U8, B>::from_query_ref(&query_ref, select_mask, enc_byte_arrays);

        #[cfg(feature = "stats")]
        self.stats_close(stats);

        Ok(result)
    }

    fn compute_select_mask(
        &mut self,
        query_ref: SqlQueryRef<B>,
        tables: &OrderedTables,
    ) -> Result<BoolMask<B>, FheSqlError> {
        #[cfg(feature = "stats")]
        let stats = PerfStats::new("Compute Select");

        let mut select_mask: BoolMask<B>;

        if query_ref.is_where_empty() {
            select_mask = BoolMask::<B>::all(tables.max_num_rows());
        } else {
            const CHUNCK_SIZE: usize = 100;

            let mut ident_cmp_array = IdentCompareWithArray::<B>::new_empty(&query_ref);
            select_mask = ident_cmp_array.compute_select(&query_ref, tables, CHUNCK_SIZE);
        }

        // Last Pass : compute SELECT DISTINCT flag
        compute_select_distinct(
            &mut select_mask,
            query_ref.distinct(),
            tables,
            &query_ref.header().table_mask,
            &query_ref.header().not_field_mask,
        );

        #[cfg(feature = "stats")]
        self.stats_close(stats);

        Ok(select_mask)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "stats")]
use crate::stats::PerfStats;

#[cfg(feature = "stats")]
impl<U8, B> SqlServer<U8, B> {
    fn stats_close(&mut self, s: PerfStats) {
        self.stats.close(s)
    }
    fn stats_finish(
        &mut self,
        s: PerfStats,
        result: Result<SqlResult<U8, B>, FheSqlError>,
    ) -> Result<SqlResult<U8, B>, FheSqlError> {
        self.stats_close(s);

        match result {
            Err(err) => Err(err),
            Ok(mut r) => {
                r.stats = self.stats.clone();
                Ok(r)
            }
        }
    }
}
