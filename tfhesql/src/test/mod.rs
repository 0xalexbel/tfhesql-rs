#![cfg(test)]
#![allow(dead_code)]

mod clear_db;
mod clear_one_col;
mod clear_simple_tables_x_3;
//mod enc_db;
pub mod simple_batch;
mod simple_queries;
mod triv_db;
mod triv_one_col;

use arrow_array::*;
use arrow_schema::*;
use std::sync::Arc;

use crate::{test_util::tfhesql_test_db_file, *};

use self::simple_queries::{InOutBatchResult, SqlInOut};

////////////////////////////////////////////////////////////////////////////////

macro_rules! make_rb_primitive_x_1 {
    ($func_name:ident, $uint:ty, $dt:ident, $arr:ident) => {
        pub fn $func_name(name: &str, v: Vec<$uint>) -> RecordBatch {
            let schema = Schema::new(vec![Field::new(name, DataType::$dt, false)]);
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(<$arr>::from(v))]).unwrap()
        }
    };
}

make_rb_primitive_x_1!(rb_i8x1, i8, Int8, Int8Array);
make_rb_primitive_x_1!(rb_i16x1, i16, Int16, Int16Array);
make_rb_primitive_x_1!(rb_i32x1, i32, Int32, Int32Array);
make_rb_primitive_x_1!(rb_i64x1, i64, Int64, Int64Array);

pub fn rb_utf8x1(name: &str, v: Vec<&str>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new(name, DataType::Utf8, false)]);
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(StringArray::from(v))],
    )
    .unwrap()
}

////////////////////////////////////////////////////////////////////////////////

macro_rules! make_sql_client_x_1 {
    ($func_name:ident, $t:ident, $rb_func:ident) => {
        pub fn $func_name(table: &str, column: &str, v: Vec<$t>) -> FheSqlClient {
            let t = Table::new(table, $rb_func(column, v));
            let tables = OrderedTables::new(vec![t]).unwrap();
            let client_server_ordered_schemas = tables.ordered_schemas();
            FheSqlClient::new(client_server_ordered_schemas.clone()).unwrap()
        }
    };
}

make_sql_client_x_1!(sql_client_i8x1, i8, rb_i8x1);
make_sql_client_x_1!(sql_client_i16x1, i16, rb_i16x1);
make_sql_client_x_1!(sql_client_i32x1, i32, rb_i32x1);
make_sql_client_x_1!(sql_client_i64x1, i64, rb_i64x1);

pub fn sql_client_utf8x1(table: &str, column: &str, v: Vec<&str>) -> FheSqlClient {
    let t = Table::new(table, rb_utf8x1(column, v));
    let tables = OrderedTables::new(vec![t]).unwrap();
    let client_server_ordered_schemas = tables.ordered_schemas();
    FheSqlClient::new(client_server_ordered_schemas.clone()).unwrap()
}

pub fn table_customers() -> Table {
    let csv_file = tfhesql_test_db_file("medium", "Customers.csv");
    let table = Table::load(&csv_file).unwrap();
    assert_eq!(table.name(), "Customers");
    table
}

pub fn table_customers_with_bounds(start: usize, end: usize) -> Table {
    let csv_file = tfhesql_test_db_file("medium", "Customers.csv");
    let table = Table::load_with_bounds(&csv_file, start, end).unwrap();
    assert_eq!(table.name(), "Customers");
    table
}

pub fn table_customers_small() -> Table {
    let csv_file = tfhesql_test_db_file("small", "Customers.csv");
    let table = Table::load(&csv_file).unwrap();
    assert_eq!(table.name(), "Customers");
    table
}

pub fn table_categories() -> Table {
    let csv_file = tfhesql_test_db_file("medium", "Categories.csv");
    let table = Table::load(&csv_file).unwrap();
    assert_eq!(table.name(), "Categories");
    table
}

pub fn table_numbers() -> Table {
    let csv_file = tfhesql_test_db_file("numbers", "Numbers.csv");
    let table = Table::load(&csv_file).unwrap();
    assert_eq!(table.name(), "Numbers");
    table
}

pub fn table_tiny_numbers() -> Table {
    let csv_file = tfhesql_test_db_file("tiny-numbers", "Numbers.csv");
    let table = Table::load(&csv_file).unwrap();
    assert_eq!(table.name(), "Numbers");
    table
}

pub fn simple_sql_client(table: &str, input: RecordBatch) -> FheSqlClient {
    let table = Table::new(table, input);
    let tables: OrderedTables = OrderedTables::new(vec![table]).unwrap();
    let client_server_ordered_schemas = tables.ordered_schemas();
    FheSqlClient::new(client_server_ordered_schemas.clone()).unwrap()
}

pub fn sql_client_customers() -> (FheSqlClient, OrderedTables) {
    let table = table_customers();
    let tables: OrderedTables = OrderedTables::new(vec![table]).unwrap();
    let client_server_ordered_schemas = tables.ordered_schemas();
    (
        FheSqlClient::new(client_server_ordered_schemas.clone()).unwrap(),
        tables,
    )
}

pub fn sql_client_customers_with_bounds(start: usize, end: usize) -> (FheSqlClient, OrderedTables) {
    let table = table_customers_with_bounds(start, end);
    let tables: OrderedTables = OrderedTables::new(vec![table]).unwrap();
    let client_server_ordered_schemas = tables.ordered_schemas();
    (
        FheSqlClient::new(client_server_ordered_schemas.clone()).unwrap(),
        tables,
    )
}

pub fn sql_client_customers_small() -> (FheSqlClient, OrderedTables) {
    let table = table_customers_with_bounds(0, 10);
    let tables: OrderedTables = OrderedTables::new(vec![table]).unwrap();
    let client_server_ordered_schemas = tables.ordered_schemas();
    (
        FheSqlClient::new(client_server_ordered_schemas.clone()).unwrap(),
        tables,
    )
}

pub fn sql_client_customers_categories() -> (FheSqlClient, OrderedTables) {
    let table1 = table_customers();
    let table2 = table_categories();
    let tables: OrderedTables = OrderedTables::new(vec![table1, table2]).unwrap();
    let client_server_ordered_schemas = tables.ordered_schemas();
    let sql_client = FheSqlClient::new(client_server_ordered_schemas.clone()).unwrap();
    assert_eq!(sql_client.ordered_schemas(), tables.ordered_schemas());
    (sql_client, tables)
}

pub fn sql_client_numbers() -> (FheSqlClient, OrderedTables) {
    let table = table_numbers();
    let tables: OrderedTables = OrderedTables::new(vec![table]).unwrap();
    let client_server_ordered_schemas = tables.ordered_schemas();
    (
        FheSqlClient::new(client_server_ordered_schemas.clone()).unwrap(),
        tables,
    )
}

pub fn sql_client_tiny_numbers() -> (FheSqlClient, OrderedTables) {
    let table = table_tiny_numbers();
    let tables: OrderedTables = OrderedTables::new(vec![table]).unwrap();
    let client_server_ordered_schemas = tables.ordered_schemas();
    (
        FheSqlClient::new(client_server_ordered_schemas.clone()).unwrap(),
        tables,
    )
}

////////////////////////////////////////////////////////////////////////////////

pub fn clear_test_in_out<T>(inout: SqlInOut<T>)
where
    SqlInOut<T>: InOutBatchResult,
{
    let input = inout.input_batch();
    let output = inout.output_batch();
    let t1 = Table::new("table1", input);
    let tables: OrderedTables = OrderedTables::new(vec![t1]).unwrap();
    let client_server_ordered_schemas = tables.ordered_schemas();
    let sql_client = FheSqlClient::new(client_server_ordered_schemas.clone()).unwrap();

    let clear_sql_query: ClearSqlQuery = sql_client
        .clear_sql(inout.sql(), SqlResultOptions::default())
        .unwrap();
    let clear_sql_result: ClearSqlResult = FheSqlServer::run(&clear_sql_query, &tables).unwrap();
    let rb = clear_sql_result.clone().into_record_batch().unwrap();

    assert_eq!(rb, output);
}

pub fn triv_test_in_out<T>(inout: SqlInOut<T>)
where
    SqlInOut<T>: InOutBatchResult,
{
    let input = inout.input_batch();
    let output = inout.output_batch();
    let t1 = Table::new("table1", input);
    let tables: OrderedTables = OrderedTables::new(vec![t1]).unwrap();
    let client_server_ordered_schemas = tables.ordered_schemas();
    let sql_client = FheSqlClient::new(client_server_ordered_schemas.clone()).unwrap();

    let enc_sql_query: FheSqlQuery = sql_client
        .trivial_encrypt_sql(inout.sql(), SqlResultOptions::default())
        .unwrap();
    let enc_sql_result: FheSqlResult = FheSqlServer::run(&enc_sql_query, &tables).unwrap();
    let rb = enc_sql_result.try_decrypt_trivial_record_batch().unwrap();

    assert_eq!(rb, output);
}

////////////////////////////////////////////////////////////////////////////////
