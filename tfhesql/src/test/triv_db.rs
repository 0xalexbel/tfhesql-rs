use super::{simple_batch::RecordBatchBuilder, sql_client_customers};
use crate::{
    test::sql_client_customers_categories, test_util::try_load_or_gen_test_keys, FheRunSqlQuery, FheSqlServer, SqlResultOptions
};
use crate::test_util::broadcast_set_server_key;
use arrow_array::types::UInt32Type;
use tfhe::set_server_key;

#[test]
fn test_customers_1() {
    let (_, sk) = try_load_or_gen_test_keys(false);
    broadcast_set_server_key(&sk);
    set_server_key(sk);

    let (sql_client, tables) = sql_client_customers();

    let sql = "SELECT * FROM Customers WHERE City='Berlin'";
    let enc_sql_query = sql_client.trivial_encrypt_sql(sql, SqlResultOptions::default()).unwrap();
    let enc_sql_result =
        FheSqlServer::run(&enc_sql_query, &tables, ).unwrap();
    let rb = enc_sql_result
        .try_decrypt_trivial_record_batch()
        .unwrap();

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

    // WARNING: stats are not valid when multiple tests are launched in parallel
    // arrow_cast::pretty::print_batches(&[rb]).unwrap();
    // enc_sql_result.print_stats();
}

#[test]
fn test_customers_2() {
    let (_, sk) = try_load_or_gen_test_keys(false);
    broadcast_set_server_key(&sk);
    set_server_key(sk);

    let (sql_client, tables) = sql_client_customers();

    let sql = "SELECT CustomerID FROM Customers WHERE Country='France'";
    let enc_sql_query = sql_client.trivial_encrypt_sql(sql, SqlResultOptions::default()).unwrap();
    let enc_sql_result =
        FheSqlServer::run(&enc_sql_query, &tables).unwrap();
    let rb = enc_sql_result
        .try_decrypt_trivial_record_batch()
        .unwrap();

    let mut expected_rb = RecordBatchBuilder::new();
    expected_rb
        .push_with_name::<UInt32Type>("CustomerID", vec![7, 9, 18, 23, 26, 40, 41, 57, 74, 84, 85]);

    let expected_rb = expected_rb.finish();
    assert_eq!(rb, expected_rb);

    // WARNING: stats are not valid when multiple tests are launched in parallel
    // arrow_cast::pretty::print_batches(&[rb]).unwrap();
    // enc_sql_result.print_stats();
}

#[test]
fn test_customers_3() {
    let (_, sk) = try_load_or_gen_test_keys(false);
    broadcast_set_server_key(&sk);
    set_server_key(sk);

    let (sql_client, tables) = sql_client_customers();

    let sql = "SELECT CustomerID,PostalCode FROM Customers WHERE Country='France'";
    let enc_sql_query = sql_client.trivial_encrypt_sql(sql, SqlResultOptions::default()).unwrap();
    let enc_sql_result =
        FheSqlServer::run(&enc_sql_query, &tables).unwrap();
    let rb = enc_sql_result
        .try_decrypt_trivial_record_batch()
        .unwrap();

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

    // WARNING: stats are not valid when multiple tests are launched in parallel
    // arrow_cast::pretty::print_batches(&[rb]).unwrap();
    // enc_sql_result.print_stats();
}

#[test]
fn test_customers_4() {
    let (_, sk) = try_load_or_gen_test_keys(false);
    broadcast_set_server_key(&sk);
    set_server_key(sk);

    let (sql_client, tables) = sql_client_customers_categories();

    let options = SqlResultOptions::default()
        .with_compress(true)
        .with_format(crate::SqlResultFormat::RowBytes(true));

    let sql = "SELECT CustomerID,PostalCode FROM Customers WHERE Country='France'";
    let enc_sql_query = sql_client.trivial_encrypt_sql(sql, options).unwrap();
    let enc_sql_result = FheSqlServer::run(&enc_sql_query, &tables).unwrap();
    let rb = enc_sql_result
        .try_decrypt_trivial_record_batch()
        .unwrap();

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
    enc_sql_result.print_stats();
}
