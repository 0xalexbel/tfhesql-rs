use super::{simple_batch::RecordBatchBuilder, sql_client_customers};
use crate::test_util::broadcast_set_server_key;
use crate::{
    default_into::DefaultIntoWithKey, test::sql_client_customers_small,
    test_util::try_load_or_gen_test_keys, FheRunSqlQuery, FheSqlServer, SqlResultFormat,
    SqlResultOptions,
};
use arrow_array::types::UInt32Type;
use tfhe::{prelude::FheDecrypt, set_server_key, FheBool};

#[test]
fn test_customers_encrypt() {
    let (ck, _) = try_load_or_gen_test_keys(false);
    // broadcast_set_server_key(&sk);
    // set_server_key(sk);

    let options = SqlResultOptions::default()
        .with_compress(true)
        .with_format(SqlResultFormat::TableBytesInColumnOrder);

    let (sql_client, _) = sql_client_customers_small();

    FheBool::default_into_with_key(&ck);

    let sql = "SELECT * FROM Customers WHERE City='Berlin'";
    let expected_clear_sql_query = sql_client.clear_sql(sql, options).unwrap();
    let enc_sql_query = sql_client.encrypt_sql(sql, &ck, options).unwrap();
    let clear_sql_query = enc_sql_query.decrypt(&ck);
    assert_eq!(clear_sql_query, expected_clear_sql_query);
}

#[test]
fn test_customers_1() {
    let (ck, sk) = try_load_or_gen_test_keys(false);
    broadcast_set_server_key(&sk);
    set_server_key(sk);

    let options = SqlResultOptions::default()
        .with_compress(true)
        .with_format(SqlResultFormat::TableBytesInColumnOrder);

    let (sql_client, tables) = sql_client_customers_small();

    let sql = "SELECT * FROM Customers WHERE City='Berlin'";
    let compressed_sql_query = sql_client
        .encrypt_compressed_sql(sql, &ck, options)
        .unwrap();
    let enc_sql_query = compressed_sql_query.decompress();

    //let enc_sql_query = sql_client.encrypt_sql(sql, &ck, options).unwrap();
    let enc_sql_result = FheSqlServer::run(&enc_sql_query, &tables).unwrap();
    let rb = enc_sql_result.decrypt_record_batch(&ck).unwrap();

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
    arrow_cast::pretty::print_batches(&[rb]).unwrap();

    #[cfg(feature = "stats")]
    enc_sql_result.print_detailed_stats();
}

#[test]
fn test_customers_2() {
    let (ck, sk) = try_load_or_gen_test_keys(false);
    broadcast_set_server_key(&sk);
    set_server_key(sk);

    let (sql_client, tables) = sql_client_customers();

    let options = SqlResultOptions::default()
        .with_compress(true)
        .with_format(SqlResultFormat::TableBytesInColumnOrder);

    let sql = "SELECT CustomerID FROM Customers WHERE Country='France'";
    let enc_sql_query = sql_client.encrypt_sql(sql, &ck, options).unwrap();
    let enc_sql_result = FheSqlServer::run(&enc_sql_query, &tables).unwrap();
    let rb = enc_sql_result.decrypt_record_batch(&ck).unwrap();

    let mut expected_rb = RecordBatchBuilder::new();
    expected_rb.push_with_name::<UInt32Type>("CustomerID", vec![7, 9]);

    let expected_rb = expected_rb.finish();
    assert_eq!(rb, expected_rb);

    // WARNING: stats are not valid when multiple tests are launched in parallel
    // arrow_cast::pretty::print_batches(&[rb]).unwrap();
    // enc_sql_result.print_stats();
}

#[test]
fn test_customers_3() {
    let (ck, sk) = try_load_or_gen_test_keys(false);
    broadcast_set_server_key(&sk);
    set_server_key(sk);

    let (sql_client, tables) = sql_client_customers();

    let options = SqlResultOptions::default()
        .with_compress(true)
        .with_format(SqlResultFormat::TableBytesInColumnOrder);

    let sql = "SELECT CustomerID,PostalCode FROM Customers WHERE Country='France'";
    let enc_sql_query = sql_client.encrypt_sql(sql, &ck, options).unwrap();
    let enc_sql_result = FheSqlServer::run(&enc_sql_query, &tables).unwrap();
    let rb = enc_sql_result.decrypt_record_batch(&ck).unwrap();

    let mut expected_rb = RecordBatchBuilder::new();
    expected_rb.push_with_name::<UInt32Type>("CustomerID", vec![7, 9]);
    expected_rb.push_str_with_name("PostalCode", vec!["67000", "13008"]);

    let expected_rb = expected_rb.finish();
    assert_eq!(rb, expected_rb);

    // WARNING: stats are not valid when multiple tests are launched in parallel
    // arrow_cast::pretty::print_batches(&[rb]).unwrap();
    // #[cfg(feature = "stats")]
    // enc_sql_result.print_stats();
}
