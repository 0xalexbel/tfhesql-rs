use arrow_array::*;
use arrow_cast::pretty::print_batches;
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

use crate::{
    test::simple_batch::{
        simple_batch_1, simple_batch_2, simple_batch_3, simple_batch_4, simple_batch_5,
    },
    ClearSqlResult, FheRunSqlQuery, FheSqlClient, FheSqlServer, OrderedTables, SqlResultOptions,
    Table,
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
    let sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();

    let rb = sql_result.clone().into_record_batch().unwrap();

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

#[test]
fn test4() {
    let t1 = Table::new("table1", simple_batch_4());
    let tables: OrderedTables = OrderedTables::new(vec![t1]).unwrap();
    let public_ordered_schemas = tables.ordered_schemas();

    let sqls = ["SELECT SomeInteger FROM table1 WHERE SomeInteger = 10"];

    // One line : 10
    let expected_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "SomeInteger",
            DataType::UInt64,
            false,
        )])),
        vec![Arc::new(UInt64Array::from(vec![10]))],
    )
    .unwrap();

    let sql_client = FheSqlClient::new(public_ordered_schemas.clone()).unwrap();
    let options = SqlResultOptions::default().with_compress(true);

    let clear_sql_query = sql_client.clear_sql(sqls[0], options).unwrap();
    let sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();

    let rb = sql_result.clone().into_record_batch().unwrap();

    assert_eq!(&rb, &expected_batch);
}

#[test]
fn test5() {
    let t1 = Table::new("table1", simple_batch_4());
    let tables: OrderedTables = OrderedTables::new(vec![t1]).unwrap();
    let public_ordered_schemas = tables.ordered_schemas();

    let sqls = ["SELECT * FROM table1 WHERE SomeInteger = 10"];

    // One line : 10
    let expected_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("SomeString", DataType::Utf8, false),
            Field::new("SomeInteger", DataType::UInt64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![""])),
            Arc::new(UInt64Array::from(vec![10])),
        ],
    )
    .unwrap();

    let sql_client = FheSqlClient::new(public_ordered_schemas.clone()).unwrap();
    let options = SqlResultOptions::default().with_compress(true);

    let clear_sql_query = sql_client.clear_sql(sqls[0], options).unwrap();
    let sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();

    let rb = sql_result.clone().into_record_batch().unwrap();

    assert_eq!(&rb, &expected_batch);
}

#[test]
fn test6() {
    let t1 = Table::new("table1", simple_batch_4());
    let tables: OrderedTables = OrderedTables::new(vec![t1]).unwrap();
    let public_ordered_schemas = tables.ordered_schemas();

    let sqls = ["SELECT * FROM table1 WHERE SomeString = ''"];

    // One line : 10
    let expected_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("SomeString", DataType::Utf8, false),
            Field::new("SomeInteger", DataType::UInt64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![""])),
            Arc::new(UInt64Array::from(vec![10])),
        ],
    )
    .unwrap();

    let sql_client = FheSqlClient::new(public_ordered_schemas.clone()).unwrap();
    let options = SqlResultOptions::default().with_compress(true);

    let clear_sql_query = sql_client.clear_sql(sqls[0], options).unwrap();
    let sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();

    let rb = sql_result.clone().into_record_batch().unwrap();

    assert_eq!(&rb, &expected_batch);
}

#[test]
fn test7() {
    /*
    -- create a table
    CREATE TABLE table_1 (
      some_int INTEGER NOT NULL,
      some_bool BOOL NOT NULL,
      some_str TEXT NOT NULL
    );
    -- insert some values
    INSERT INTO table_1 VALUES (0, TRUE, 'first line');
    INSERT INTO table_1 VALUES (123, FALSE, 'some other line');
    INSERT INTO table_1 VALUES (3, TRUE, 'first line');
    INSERT INTO table_1 VALUES (3, FALSE, 'other test');
    -- fetch some values
    SELECT * FROM table_1 WHERE ((some_int <= 100 OR some_int = 102) AND (some_int < 100 OR some_int = 103)) OR ((some_int <= 102 AND some_int > 100) OR (some_int <= 103 AND some_int > 101));

    -- Ouput
    0|1|first line
    3|1|first line
    3|0|other test
    */

    let t1 = Table::new("table_1", simple_batch_5());
    let tables: OrderedTables = OrderedTables::new(vec![t1]).unwrap();
    let public_ordered_schemas = tables.ordered_schemas();

    let sqls = ["SELECT DISTINCT * FROM table_1 WHERE ((some_int <= 100 OR some_int = 102) AND (some_int < 100 OR some_int = 103)) OR ((some_int <= 102 AND some_int > 100) OR (some_int <= 103 AND some_int > 101))"];

    // One line : 10
    let expected_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("some_int", DataType::UInt32, false),
            Field::new("some_bool", DataType::Boolean, false),
            Field::new("some_str", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(UInt32Array::from(vec![0,3,3])),
            Arc::new(BooleanArray::from(vec![true, true, false])),
            Arc::new(StringArray::from(vec!["first line", "first line", "other test"])),
        ],
    )
    .unwrap();

    let sql_client = FheSqlClient::new(public_ordered_schemas.clone()).unwrap();
    let options = SqlResultOptions::default().with_compress(true);

    let clear_sql_query = sql_client.clear_sql(sqls[0], options).unwrap();
    let sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();

    let rb = sql_result.clone().into_record_batch().unwrap();

    assert_eq!(&rb, &expected_batch);
}
