use arrow_array::*;
use arrow_cast::pretty::print_batches;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

use crate::{
    test::simple_batch::{simple_batch_1, simple_batch_2, simple_batch_3}, ClearSqlResult, FheRunSqlQuery, FheSqlClient, FheSqlServer, OrderedTables, SqlResultOptions, Table
};

////////////////////////////////////////////////////////////////////////////////
// Test
////////////////////////////////////////////////////////////////////////////////

fn run_test(sql: &str, expected_batch: &RecordBatch) -> (ClearSqlResult, RecordBatch) {
    let t1 = Table::new("table1", simple_batch_1());
    let t2 = Table::new("table2", simple_batch_2());
    let t3 = Table::new("table3", simple_batch_3());

    let tables: OrderedTables = OrderedTables::new(vec![t1, t2, t3]).unwrap();
    let public_ordered_schemas = tables.ordered_schemas();

    let sql_client = FheSqlClient::new(public_ordered_schemas.clone()).unwrap();

    let options = SqlResultOptions::default().with_compress(true);

    let clear_sql_query = sql_client.clear_sql(sql, options).unwrap();
    let sql_result =
        FheSqlServer::run(&clear_sql_query, &tables).unwrap();

    let rb = sql_result.clone()
        .into_record_batch()
        .unwrap();

    assert_eq!(&rb, expected_batch);

    (sql_result, rb)
}

#[test]
fn test1() {
    let sql = "SELECT DISTINCT ProductID FROM table3 WHERE ProductID = Type";
    // One line : 50
    let expected_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ProductID",
            DataType::Int16,
            false,
        )])),
        vec![Arc::new(Int16Array::from(vec![50]))],
    )
    .unwrap();

    let (_sql_result, rb) = run_test(sql, &expected_batch);

    print_batches(&[rb]).unwrap();

    #[cfg(feature = "stats")]
    _sql_result.print_stats();
}

#[test]
fn test2() {
    let sqls = [
        "SELECT DISTINCT ProductID FROM table3 WHERE ProductID = 100",
        "SELECT DISTINCT ProductID FROM table3 WHERE Type > 50",
        "SELECT DISTINCT ProductID FROM table3 WHERE Type >= 52",
        "SELECT DISTINCT ProductID FROM table3 WHERE ProductID <= 100 AND ProductID > 50",
        "SELECT DISTINCT ProductID FROM table3 WHERE Type > 70 AND Type <= 550",
        "SELECT DISTINCT ProductID FROM table3 WHERE Type = 500 AND Type <= 550",
    ];

    // One line : 100
    let expected_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ProductID",
            DataType::Int16,
            false,
        )])),
        vec![Arc::new(Int16Array::from(vec![100]))],
    )
    .unwrap();

    sqls.iter().for_each(|sql| {
        run_test(sql, &expected_batch);
    });
}

#[test]
fn test3() {
    let sqls = [
        "SELECT ProductID FROM table3 WHERE ProductID = 100",
        "SELECT ProductID FROM table3 WHERE Type > 50",
        "SELECT ProductID FROM table3 WHERE Type >= 52",
        "SELECT ProductID FROM table3 WHERE ProductID <= 100 AND ProductID > 50",
    ];

    // One line : 100
    let expected_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "ProductID",
            DataType::Int16,
            false,
        )])),
        vec![Arc::new(Int16Array::from(vec![100, 100]))],
    )
    .unwrap();

    sqls.iter().for_each(|sql| {
        run_test(sql, &expected_batch);
    });
}
