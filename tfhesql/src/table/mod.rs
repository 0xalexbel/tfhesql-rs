mod schema;
mod row_visitor;
mod block_iter;
mod type_cache;

pub mod byte_rows;
pub mod cmp;
pub mod ascii_cache;
pub mod eq_gt_lt_type_cache;

use arrow_array::*;
use arrow_schema::*;
use rayon::iter::*;

use crate::{
    csv,
    error::FheSqlError,
    uint::{ClearByteArray, ClearByteArrayList},
    utils::{
        arrow::{
            array_column_cell_eq, arrow_shema_data_type_width, write_column_le_bytes,
            write_row_le_bytes,
        },
        path::{absolute_path, csv_sorted_list_in_dir},
    },
};

use byte_rows::{ClearByteRows, ClearByteRowsList};
pub use schema::OrderedSchemas;

////////////////////////////////////////////////////////////////////////////////
// Table
////////////////////////////////////////////////////////////////////////////////

/// A named dataset defined by a list of arrays, each of same length.
/// These arrays are stored in an Arrow [RecordBatch] structure.
/// A record batch includes a [Schema] which descibes each array [DataType]
pub struct Table {
    name: String,
    batch: RecordBatch,
}

impl Table {
    /// Creates a new Table from a name and an Arrow [RecordBatch]
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use tfhesql::Table;
    ///
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(schema),
    ///     vec![Arc::new(id_array)]
    /// ).unwrap();
    ///
    /// let table = Table::new(
    ///     "table1",
    ///     batch
    /// );
    /// ```
    pub fn new(name: &str, batch: RecordBatch) -> Self {
        Table {
            name: name.to_string(),
            batch,
        }
    }

    /// Creates a new Table from a name, an Arrow [Schema] and columns
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::Int32Array;
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use tfhesql::Table;
    ///
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let table = Table::try_new(
    ///     "table1",
    ///     Arc::new(schema),
    ///     vec![Arc::new(id_array)]
    /// ).unwrap();
    /// ```
    pub fn try_new(
        name: &str,
        schema: SchemaRef,
        columns: Vec<ArrayRef>,
    ) -> Result<Self, FheSqlError> {
        let batch = match RecordBatch::try_new(schema, columns) {
            Ok(rb) => rb,
            Err(e) => return Err(FheSqlError::ArrowError(e.to_string())),
        };
        Ok(Self::new(name, batch))
    }

    /// Gets the name of the table.
    #[inline]
    pub fn name(&self) -> &String {
        &self.name
    }

    /// Sets the name of the table.
    #[inline]
    pub fn set_name(&mut self, name: &str) {
        assert!(!name.is_empty());
        self.name = name.to_string();
    }

    /// Returns the table's [RecordBatch] that stores the table dataset
    #[inline]
    pub(crate) fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    /// Creates a new Table from a csv file located at `path`
    #[inline]
    pub fn load(path: &str) -> Result<Table, FheSqlError> {
        csv::load(path, None)
    }

    /// Creates a new Table from `start` line to `end` line in a csv file located at `path`.
    /// `start` and `end` are line numbers.
    #[inline]
    pub fn load_with_bounds(path: &str, start: usize, end: usize) -> Result<Table, FheSqlError> {
        csv::load(path, Some((start, end)))
    }

    /// Returns a reference to the [Schema] of the Table's record batch.
    #[inline]
    pub fn schema_ref(&self) -> &SchemaRef {
        self.batch.schema_ref()
    }

    /// Returns an iterator over the table columns.
    #[inline]
    pub fn iter_columns(&self) -> std::slice::Iter<ArrayRef> {
        self.batch.columns().iter()
    }

    /// Returns an parallel iterator over the table columns.
    #[inline]
    pub fn par_iter_columns(&self) -> rayon::slice::Iter<ArrayRef> {
        self.batch.columns().par_iter()
    }
}

impl Table {
    pub(crate) fn to_bytes_rows(&self, compress: bool) -> ClearByteRows {
        let mut clear_byte_array_vec: Vec<ClearByteArray> =
            vec![ClearByteArray::default(); self.num_rows()];
        clear_byte_array_vec
            .iter_mut()
            .enumerate()
            .for_each(|(row_index, row_buffer)| {
                let mut buffer = ClearByteArray::default();
                self.write_row_le_bytes(row_index, &mut buffer);

                if compress {
                    buffer = buffer.compress();
                }

                // Put EOF marker
                buffer.push(u8::MAX);
                *row_buffer = buffer;
            });

        ClearByteRows::from_byte_array_vec(clear_byte_array_vec)
    }

    pub(crate) fn to_byte_array_in_row_order(&self, compress: bool) -> ClearByteArray {
        let mut buffer = ClearByteArray::default();
        self.write_rows_le_bytes(&mut buffer);

        if compress {
            buffer = buffer.compress();
        }

        // Put EOF marker
        buffer.push(u8::MAX);
        buffer
    }

    pub(crate) fn to_byte_array_in_column_order(&self, compress: bool) -> ClearByteArray {
        let mut buffer = ClearByteArray::default();
        self.write_columns_le_bytes(&mut buffer);

        if compress {
            buffer = buffer.compress();
        }

        // Put EOF marker
        buffer.push(u8::MAX);
        buffer
    }

    #[inline]
    fn row_width(&self) -> usize {
        self.schema_ref().fields.iter().fold(0, |acc, f| {
            acc + arrow_shema_data_type_width(f.data_type()).unwrap()
        })
    }

    #[inline]
    pub(super) fn write_rows_le_bytes(&self, buffer: &mut ClearByteArray) {
        buffer.write_header(self.num_rows(), self.num_columns());
        buffer.write_u64(self.row_width());
        (0..self.batch.num_rows())
            .for_each(|row_index| {
                self.write_row_le_bytes(row_index, buffer);
            });
    }

    #[inline]
    pub(super) fn write_row_le_bytes(&self, row_index: usize, buffer: &mut ClearByteArray) {
        self.batch.columns().iter().for_each(|column_ref| {
            write_row_le_bytes(column_ref, row_index, buffer);
        })
    }

    #[inline]
    pub(super) fn write_columns_le_bytes(&self, buffer: &mut ClearByteArray) {
        buffer.write_header(self.num_rows(), self.num_columns());
        (0..self.batch.num_columns())
            .for_each(|column_index| {
                self.write_column_le_bytes(column_index, buffer);
            });
    }

    #[inline]
    pub(super) fn write_column_le_bytes(&self, column_index: usize, buffer: &mut ClearByteArray) {
        let column_ref = self.batch.columns().get(column_index).unwrap();
        write_column_le_bytes(column_ref, buffer);
    }

    #[inline]
    pub(crate) fn num_columns(&self) -> usize {
        self.batch.num_columns()
    }

    #[inline]
    pub(crate) fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }

    // Return the number of 'true' flags stored in the buffer
    pub(crate) fn compute_line_equality_vector(
        &self,
        row_index1: usize,
        row_index2: usize,
        default: bool,
        out_buffer: &mut [bool],
    ) -> usize {
        assert!(out_buffer.len() >= self.num_columns());
        let row1_out_of_bounds = row_index1 >= self.num_rows();
        let row2_out_of_bounds = row_index2 >= self.num_rows();
        if row1_out_of_bounds && row2_out_of_bounds {
            out_buffer.fill(default);
            if default {
                return self.num_columns();
            } else {
                return 0;
            }
        }
        if row1_out_of_bounds || row2_out_of_bounds {
            out_buffer.fill(false);
            return 0;
        }

        let mut num_true: usize = 0;
        out_buffer
            .iter_mut()
            .zip(self.batch.columns().iter())
            .for_each(|(dst, column)| {
                *dst = array_column_cell_eq(column, row_index1, row_index2);
                if *dst {
                    num_true += 1;
                }
            });

        num_true
    }
}

////////////////////////////////////////////////////////////////////////////////
// OrderedTables
////////////////////////////////////////////////////////////////////////////////

/// A fixed-order list of [`Tables`](Table). The fixed-order nature is critical
/// and should be preserved between the [`FheSqlClient`](crate::FheSqlClient) and
/// the [`FheSqlServer`](`crate::FheSqlServer`) as it is used to generate tables and columns
/// boolean masks.  
pub struct OrderedTables {
    pub(super) tables: Vec<Table>,
    pub(super) ordered_schemas: OrderedSchemas,
}

impl OrderedTables {
    /// Creates a new OrderedTables structure from a vector of [`Table`]. 
    ///
    /// Note: Table order is not preserved
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::Int32Array;
    /// # use arrow_schema::{DataType, Field, Schema};
    /// # use tfhesql::Table;
    /// # use tfhesql::OrderedTables;
    ///
    /// let id_array_1 = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema_1 = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    ///
    /// let id_array_2 = Int32Array::from(vec![6, 7, 8, 9, 10]);
    /// let schema_2 = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
    ///
    /// let table1 = Table::try_new("table1", Arc::new(schema_1),vec![Arc::new(id_array_1)]).unwrap();
    /// let table2 = Table::try_new("table2", Arc::new(schema_2),vec![Arc::new(id_array_2)]).unwrap();
    /// let ordered_tables = OrderedTables::new(vec![table1, table2]).unwrap();
    /// ```
    pub fn new(mut tables: Vec<Table>) -> Result<Self, FheSqlError> {
        tables.sort_by(|a, b| a.name.cmp(&b.name));

        let named_schemas = tables.iter().map(|t| {
            (t.batch.schema_ref().clone(), t.name.clone())
        }).collect();

        let ordered_schemas = OrderedSchemas::from_schemas(named_schemas)?;

        assert_eq!(ordered_schemas.len(), tables.len());
        tables.iter().enumerate().for_each(|(i, t)| {
            assert_eq!(t.name(), ordered_schemas.name(i));
        });

        Ok(OrderedTables { tables, ordered_schemas })
    }

    /// Creates a new OrderedTables structure by parsing all the .csv files located in the specified directory.
    /// 
    /// Note: Tables are sorted by their corresponding csv filename in Rust string comparison order.
    pub fn load_from_directory<P: AsRef<std::path::Path>>(dir: P) -> Result<Self, FheSqlError> {
        let abs_dir = absolute_path(dir)?;
        if !abs_dir.is_dir() {
            return Err(FheSqlError::IoError(format!("Directory does not exist: {}", abs_dir.display())));
        }
        let v = csv_sorted_list_in_dir(abs_dir);
        let mut ordered_tables = vec![];
        v.iter().for_each(|f| {
            let t = match Table::load(f) {
                Ok(s) => s,
                Err(_) => return,
            };
            ordered_tables.push(t);
        });
        OrderedTables::new(ordered_tables)
    }

    /// Returns an immutable reference to the corresponding fixed-order list of all the [Table]'s [Schema].
    #[inline]
    pub fn ordered_schemas(&self) -> &OrderedSchemas {
        &self.ordered_schemas
    }

    /// Returns an immutable reference to the tables
    #[inline]
    pub fn tables(&self) -> &Vec<Table> {
        &self.tables
    }

    /// Returns the number of tables in the list
    #[inline]
    pub fn num_tables(&self) -> usize {
        self.tables.len()
    }

    /// Returns an iterator over the tables of the list.
    #[inline]
    pub fn iter_tables(&self) -> std::slice::Iter<Table> {
        self.tables.iter()
    }

    /// Returns a parallel iterator over the tables of the list.
    #[inline]
    pub fn par_iter_tables(&self) -> rayon::slice::Iter<Table> {
        self.tables.par_iter()
    }
}

impl OrderedTables {
    pub(crate) fn to_byte_rows_list(&self, compress: bool) -> ClearByteRowsList {
        let compressed_tables = self
            .tables
            .par_iter()
            .map(|table| table.to_bytes_rows(compress))
            .collect::<Vec<ClearByteRows>>();
        ClearByteRowsList::from_byte_rows(compressed_tables)
    }

    pub(crate) fn to_byte_array_list(
        &self,
        in_row_order: bool,
        compress: bool,
    ) -> ClearByteArrayList {
        let compressed_tables = if in_row_order {
            self
                .tables
                .par_iter()
                .map(|table| table.to_byte_array_in_row_order(compress))
                .collect::<Vec<ClearByteArray>>()
        } else {
            self
                .tables
                .par_iter()
                .map(|table| table.to_byte_array_in_column_order(compress))
                .collect::<Vec<ClearByteArray>>()
        };
        ClearByteArrayList {
            list: compressed_tables,
        }
    }

    pub(crate) fn min_num_rows(&self) -> usize {
        let mut min_num = usize::MAX;
        for i in 0..self.num_tables() {
            min_num = min_num.min(self.tables[i].num_rows());
        }
        min_num
    }

    pub(crate) fn max_num_rows(&self) -> usize {
        let mut max_num = 0;
        for i in 0..self.num_tables() {
            max_num = max_num.max(self.tables[i].num_rows());
        }
        max_num
    }

    pub(crate) fn encode_predicate_rows_gteq(&self, num_rows: usize) -> u64 {
        assert!(self.num_tables() <= 64);
        let mut code_u64 = 0_u64;
        for i in 0..self.num_tables() {
            if self.tables[i].num_rows() >= num_rows {
                code_u64 |= (1 << i) as u64;
            }
        }
        code_u64
    }

    pub(crate) fn rows_between(&self, min_rows: usize, max_rows: usize) -> Vec<usize> {
        let mut overflow_vec = vec![];
        for i in 0..self.num_tables() {
            let n = self.tables[i].num_rows();
            if min_rows <= n && n < max_rows {
                overflow_vec.push(i);
            }
        }
        overflow_vec
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use crate::{
        test::{
            simple_batch::{simple_batch_0, simple_batch_1, simple_batch_2, simple_batch_3},
            table_customers,
        }, test_util::tfhesql_test_db_file, uint::mask::{ClearBoolMask, ClearByteMask, ClearByteMaskMatrix}, OrderedTables, Table
    };

    #[test]
    fn test_load_csv() {
        let csv_file = tfhesql_test_db_file("medium", "Customers.csv");

        let t = Table::load(&csv_file).unwrap();
        assert_eq!(t.num_columns(), 7);
        assert_eq!(t.num_rows(), 91);
    }

    #[test]
    fn test_mask_tables_by_rows() {
        let compress = true;
        let padding = true;
        let table_index = 1;

        let t1 = Table::new("table1", simple_batch_1());
        let t2 = Table::new("table2", simple_batch_2());
        let t3 = Table::new("table3", simple_batch_3());
        let ordered_tables = OrderedTables::new(vec![t1, t2, t3]).unwrap();

        let field_mask = ClearBoolMask::all(ordered_tables.ordered_schemas().max_num_fields());
        let select_mask = ClearBoolMask::all(ordered_tables.max_num_rows());
        let byte_select_mask = ClearByteMask::all(ordered_tables.max_num_rows());

        let mut table_mask = ClearBoolMask::none(ordered_tables.num_tables());
        table_mask.set(table_index);
        let mut byte_table_mask = ClearByteMask::none(ordered_tables.num_tables());
        byte_table_mask.set(table_index);

        let select_by_table_byte_matrix =
            ClearByteMaskMatrix::par_vec_and_vec(&byte_select_mask, &byte_table_mask);

        let byte_rows_list = ordered_tables.to_byte_rows_list(compress);

        assert!(byte_rows_list.len() == select_by_table_byte_matrix.num_columns());
        let masked_byte_rows_list = byte_rows_list.par_bitand(&select_by_table_byte_matrix);

        let byte_rows = masked_byte_rows_list.par_flatten_row_by_row(padding);

        let rb = byte_rows
            .extract_record_batch(
                ordered_tables.ordered_schemas().schema(table_index),
                &field_mask,
                &select_mask,
                compress,
            )
            .unwrap();

        assert_eq!(&rb, ordered_tables.tables[table_index].batch());
    }

    #[test]
    fn test_mask_tables_by_columns() {
        let compress = false;
        let in_row_order = true;
        let table_index = 1;

        let t1 = Table::new("table1", simple_batch_1());
        let t2 = Table::new("table2", simple_batch_2());
        let t3 = Table::new("table3", simple_batch_3());
        let ordered_tables = OrderedTables::new(vec![t1, t2, t3]).unwrap();

        let field_mask = ClearBoolMask::all(ordered_tables.ordered_schemas().max_num_fields());
        let select_mask = ClearBoolMask::all(ordered_tables.max_num_rows());

        let mut table_mask = ClearBoolMask::none(ordered_tables.num_tables());
        table_mask.set(table_index);
        let mut byte_table_mask = ClearByteMask::none(ordered_tables.num_tables());
        byte_table_mask.set(table_index);

        // Convert each table into a single byte array
        let byte_array_list = ordered_tables.to_byte_array_list(in_row_order, compress);
        let aa = byte_array_list.clone();
        // Apply Table(t) mask to each previously converted table.
        assert!(byte_array_list.len() == byte_table_mask.len());
        let masked_byte_array_list = byte_array_list.par_bitand(&byte_table_mask);

        // Flatten the list of tables (as list of byte) into a single list of bytes
        let byte_array = masked_byte_array_list.par_flatten();
        assert_eq!(&byte_array, &aa.list[1]);

        let rb = byte_array
            .extract_record_batch(
                ordered_tables.ordered_schemas().schema(table_index),
                &field_mask,
                &select_mask,
                in_row_order,
                compress,
            )
            .unwrap();

        assert_eq!(&rb, ordered_tables.tables[table_index].batch());
    }

    #[test]
    fn test_compress_columns() {
        let tables = [
            Table::new("table1", simple_batch_0()),
            Table::new("table1", simple_batch_1()),
            Table::new("table1", simple_batch_2()),
            Table::new("table1", simple_batch_3()),
            table_customers(),
        ];
        tables.iter().for_each(|t| {
            let compress = false;
            let in_row_order = false;
            let a = t.to_byte_array_in_column_order(compress);

            let select_mask = ClearBoolMask::all(t.num_rows());
            let field_mask = ClearBoolMask::all(t.num_columns());

            let rb = a
                .extract_record_batch(
                    t.schema_ref(),
                    &field_mask,
                    &select_mask,
                    in_row_order,
                    compress,
                )
                .unwrap();
            assert_eq!(&rb, t.batch());
        });

        tables.iter().for_each(|t| {
            let compress = true;
            let in_row_order = false;
            let a = t.to_byte_array_in_column_order(compress);

            let select_mask = ClearBoolMask::all(t.num_rows());
            let field_mask = ClearBoolMask::all(t.num_columns());

            let rb = a
                .extract_record_batch(
                    t.schema_ref(),
                    &field_mask,
                    &select_mask,
                    in_row_order,
                    compress,
                )
                .unwrap();
            assert_eq!(&rb, t.batch());
        });
    }

    #[test]
    fn test_compress_rows() {
        let tables = [
            Table::new("table1", simple_batch_0()),
            Table::new("table1", simple_batch_1()),
            Table::new("table1", simple_batch_2()),
            Table::new("table1", simple_batch_3()),
            table_customers(),
        ];
        tables.iter().for_each(|t| {
            let compress = false;
            let in_row_order = true;
            let a = t.to_byte_array_in_row_order(compress);

            let select_mask = ClearBoolMask::all(t.num_rows());
            let field_mask = ClearBoolMask::all(t.num_columns());

            let rb = a
                .extract_record_batch(
                    t.schema_ref(),
                    &field_mask,
                    &select_mask,
                    in_row_order,
                    compress,
                )
                .unwrap();
            assert_eq!(&rb, t.batch());
        });

        tables.iter().for_each(|t| {
            let compress = true;
            let in_row_order = true;
            let a = t.to_byte_array_in_row_order(compress);

            let select_mask = ClearBoolMask::all(t.num_rows());
            let field_mask = ClearBoolMask::all(t.num_columns());

            let rb = a
                .extract_record_batch(
                    t.schema_ref(),
                    &field_mask,
                    &select_mask,
                    in_row_order,
                    compress,
                )
                .unwrap();
            assert_eq!(&rb, t.batch());
        });
    }
}




