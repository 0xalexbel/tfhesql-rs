use tfhe::{ClientKey, ConfigBuilder};
use tfhesql::test_util::tfhesql_test_db_dir;
use tfhesql::bounty_api::*;

fn main() {
    let csv_dir = tfhesql_test_db_dir("tiny");

    // 1. Call default_parameters
    let params = default_parameters();

    // Generate Keys
    let config = ConfigBuilder::with_custom_parameters(params.0, None).build();
    let client_key = ClientKey::generate(config);
    let sks = client_key.generate_server_key();

    // 2. Call load_tables
    let tables = load_tables(&csv_dir);

    // 3. Call encrypt
    let query = "SELECT CustomerID,PostalCode,Country FROM Customers WHERE Country='Germany'";
    let enc_query = encrypt_query(query, &client_key);
    
    // 4. Call run_fhe_query
    let enc_result = run_fhe_query(&sks, &enc_query, &tables);

    // 5. Call decrypt_result to retrieve the clear result as a csv string
    let csv_string = decrypt_result(&client_key, &enc_result);
    
    println!("{}", csv_string);
}

