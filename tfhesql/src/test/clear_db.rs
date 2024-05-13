use super::{simple_batch::RecordBatchBuilder, sql_client_customers, sql_client_numbers, sql_client_tiny_numbers};
use crate::{
    test::sql_client_customers_categories, FheRunSqlQuery, FheSqlServer, SqlResultFormat,
    SqlResultOptions,
};
use arrow_array::types::{Int16Type, UInt32Type};
use arrow_cast::pretty::print_batches;

#[test]
fn test_customers_1() {
    let (sql_client, tables) = sql_client_customers();

    let options = SqlResultOptions::default()
        .with_compress(true)
        .with_format(SqlResultFormat::TableBytesInColumnOrder);

    let sql = "SELECT * FROM Customers WHERE City='Berlin'";
    let clear_sql_query = sql_client.clear_sql(sql, options).unwrap();
    let clear_sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();
    let rb = clear_sql_result.clone().into_record_batch().unwrap();

    let mut expected_rb = RecordBatchBuilder::new();
    expected_rb.push_with_name::<UInt32Type>("CustomerID", vec![1]);
    expected_rb.push_str_with_name("CustomerName", vec!["Alfreds Futterkiste"]);
    expected_rb.push_str_with_name("ContactName", vec!["Maria Anders"]);
    expected_rb.push_str_with_name("Address", vec!["Obere Str. 57"]);
    expected_rb.push_str_with_name("City", vec!["Berlin"]);
    expected_rb.push_str_with_name("PostalCode", vec!["12209"]);
    expected_rb.push_str_with_name("Country", vec!["Germany"]);

    let expected_rb = expected_rb.finish();
    assert_eq!(rb.schema(), expected_rb.schema());
    assert_eq!(rb.columns(), expected_rb.columns());

    print_batches(&[rb]).unwrap();

    #[cfg(feature = "stats")]
    clear_sql_result.print_stats();
}

#[test]
fn test_customers_2() {
    let (sql_client, tables) = sql_client_customers();

    let options = SqlResultOptions::default()
        .with_compress(false)
        .with_format(SqlResultFormat::TableBytesInRowOrder);

    let sql = "SELECT CustomerID FROM Customers WHERE Country='France'";
    let clear_sql_query = sql_client.clear_sql(sql, options).unwrap();
    let clear_sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();
    let rb = clear_sql_result.clone().into_record_batch().unwrap();
    let mut expected_rb = RecordBatchBuilder::new();
    expected_rb
        .push_with_name::<UInt32Type>("CustomerID", vec![7, 9, 18, 23, 26, 40, 41, 57, 74, 84, 85]);

    let expected_rb = expected_rb.finish();
    assert_eq!(rb, expected_rb);

    print_batches(&[rb]).unwrap();

    #[cfg(feature = "stats")]
    clear_sql_result.print_stats();
}

#[test]
fn test_customers_3() {
    let (sql_client, tables) = sql_client_customers();

    let options = SqlResultOptions::default()
        .with_compress(true)
        .with_format(SqlResultFormat::TableBytesInRowOrder);

    let sql = "SELECT CustomerID,PostalCode FROM Customers WHERE Country='France'";
    let clear_sql_query = sql_client.clear_sql(sql, options).unwrap();
    let clear_sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();
    let rb = clear_sql_result.clone().into_record_batch().unwrap();
    let mut expected_rb = RecordBatchBuilder::new();
    expected_rb
        .push_with_name::<UInt32Type>("CustomerID", vec![7, 9, 18, 23, 26, 40, 41, 57, 74, 84, 85]);
    expected_rb.push_str_with_name(
        "PostalCode",
        vec![
            "67000", "13008", "44000", "59000", "44000", "78000", "31000", "75012", "75016",
            "69004", "51100",
        ],
    );

    let expected_rb = expected_rb.finish();
    assert_eq!(rb, expected_rb);

    print_batches(&[rb]).unwrap();

    #[cfg(feature = "stats")]
    clear_sql_result.print_stats();
}

#[test]
fn test_customers_4() {
    let (sql_client, tables) = sql_client_customers_categories();

    let options = SqlResultOptions::default()
        .with_compress(true)
        .with_format(crate::SqlResultFormat::RowBytes(true));

    let sql = "SELECT CustomerID,PostalCode FROM Customers WHERE Country='France'";
    let clear_sql_query = sql_client.clear_sql(sql, options).unwrap();

    let clear_sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();
    let rb = clear_sql_result.clone().into_record_batch().unwrap();

    let mut expected_rb = RecordBatchBuilder::new();
    expected_rb
        .push_with_name::<UInt32Type>("CustomerID", vec![7, 9, 18, 23, 26, 40, 41, 57, 74, 84, 85]);
    expected_rb.push_str_with_name(
        "PostalCode",
        vec![
            "67000", "13008", "44000", "59000", "44000", "78000", "31000", "75012", "75016",
            "69004", "51100",
        ],
    );

    let expected_rb = expected_rb.finish();
    assert_eq!(rb, expected_rb);

    //WARNING: stats are not valid when multiple tests are launched in parallel
    //arrow_cast::pretty::print_batches(&[rb]).unwrap();
    #[cfg(feature = "stats")]
    clear_sql_result.print_stats();
}

#[test]
fn test_numbers_1() {
    let (sql_client, tables) = sql_client_numbers();

    let options = SqlResultOptions::default()
        .with_compress(true)
        .with_format(SqlResultFormat::TableBytesInColumnOrder);

    let sql = "SELECT * FROM Numbers WHERE SomeU8=2";
    let clear_sql_query = sql_client.clear_sql(sql, options).unwrap();
    let clear_sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();
    let rb = clear_sql_result.clone().into_record_batch().unwrap();

    // let mut expected_rb = RecordBatchBuilder::new();
    // expected_rb.push_with_name::<UInt32Type>("CustomerID", vec![1]);
    // expected_rb.push_str_with_name("CustomerName", vec!["Alfreds Futterkiste"]);
    // expected_rb.push_str_with_name("ContactName", vec!["Maria Anders"]);
    // expected_rb.push_str_with_name("Address", vec!["Obere Str. 57"]);
    // expected_rb.push_str_with_name("City", vec!["Berlin"]);
    // expected_rb.push_str_with_name("PostalCode", vec!["12209"]);
    // expected_rb.push_str_with_name("Country", vec!["Germany"]);

    // let expected_rb = expected_rb.finish();
    // assert_eq!(rb.schema(), expected_rb.schema());
    // assert_eq!(rb.columns(), expected_rb.columns());

    print_batches(&[rb]).unwrap();

    #[cfg(feature = "stats")]
    clear_sql_result.print_stats();
}

#[test]
fn test_numbers_2() {
    let (sql_client, tables) = sql_client_numbers();

    let options = SqlResultOptions::default()
        .with_compress(true)
        .with_format(SqlResultFormat::TableBytesInColumnOrder);

    let sql = "SELECT * FROM Numbers WHERE (SomeU8 > 2 AND SomeBool) OR SomeI16 < 0";
    let clear_sql_query = sql_client.clear_sql(sql, options).unwrap();
    let clear_sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();
    let rb = clear_sql_result.clone().into_record_batch().unwrap();

    // let mut expected_rb = RecordBatchBuilder::new();
    // expected_rb.push_with_name::<UInt32Type>("CustomerID", vec![1]);
    // expected_rb.push_str_with_name("CustomerName", vec!["Alfreds Futterkiste"]);
    // expected_rb.push_str_with_name("ContactName", vec!["Maria Anders"]);
    // expected_rb.push_str_with_name("Address", vec!["Obere Str. 57"]);
    // expected_rb.push_str_with_name("City", vec!["Berlin"]);
    // expected_rb.push_str_with_name("PostalCode", vec!["12209"]);
    // expected_rb.push_str_with_name("Country", vec!["Germany"]);

    // let expected_rb = expected_rb.finish();
    // assert_eq!(rb.schema(), expected_rb.schema());
    // assert_eq!(rb.columns(), expected_rb.columns());

    print_batches(&[rb]).unwrap();

    #[cfg(feature = "stats")]
    clear_sql_result.print_stats();
}

#[test]
fn test_numbers_3() {
    let (sql_client, tables) = sql_client_tiny_numbers();

    let options = SqlResultOptions::default()
        .with_compress(true)
        .with_format(SqlResultFormat::TableBytesInColumnOrder);

    let sql = "SELECT SomeBool,SomeI16 FROM Numbers WHERE (SomeI16 > 2 AND SomeBool) OR SomeI16 < 0";
    let clear_sql_query = sql_client.clear_sql(sql, options).unwrap();
    let clear_sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();
    let rb = clear_sql_result.clone().into_record_batch().unwrap();

    let mut expected_rb = RecordBatchBuilder::new();
    expected_rb.push_bool_with_name("SomeBool", vec![true,true,false]);
    expected_rb.push_with_name::<Int16Type>("SomeI16", vec![-16355,27024,-4666]);

    let expected_rb = expected_rb.finish();
    assert_eq!(rb.schema(), expected_rb.schema());
    assert_eq!(rb.columns(), expected_rb.columns());

    print_batches(&[rb]).unwrap();

    #[cfg(feature = "stats")]
    clear_sql_result.print_stats();
}
