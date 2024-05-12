use tfhe::set_server_key;
use tfhesql::test_util::{broadcast_set_server_key, print_pretty_batches, tfhesql_test_db_dir, try_load_or_gen_test_keys};
use tfhesql::*;

fn main() {
    let csv_dir = tfhesql_test_db_dir("tiny");
    let (ck, sk) = try_load_or_gen_test_keys(false);

    // Client Side
    // ===========

    // 1. Load the SAME schemas in the SAME order as the server.
    //    This is critical since server and client must share
    //    the same table + schema order
    let client_ordered_schemas = OrderedSchemas::load_from_directory(&csv_dir).unwrap();

    // 2. Creates a new FheSqlClient instance
    let sql_client = FheSqlClient::new(client_ordered_schemas.clone()).unwrap();

    // 3. Generates a new SQL query with a SQL SELECT statement and the default options (compress = true, format = by rows + padding).
    let sql = "SELECT CustomerID,PostalCode,Country FROM Customers WHERE Country IN ('France', 'Germany')";
    let enc_sql_query = sql_client.encrypt_sql(sql, &ck, SqlResultOptions::best()).unwrap();

    // Server Side
    // ===========

    // 1. Load csv file located in the specified directory and stores them into an ordered list of tables.
    //    Note: Order is critical and should remain sealed since all the masking operations between the client
    //    and the server are based on it.
    let server_tables = OrderedTables::load_from_directory(&csv_dir).unwrap();

    // 2. Setup server keys
    broadcast_set_server_key(&sk);
    set_server_key(sk.clone());

    // 3. Executes the SQL query on the server
    let enc_sql_result = FheSqlServer::run(&enc_sql_query, &server_tables).unwrap();

    // Client Side
    // ===========

    // 1. Extract the RecordBatch from the SQL query result
    let rb = enc_sql_result.decrypt_record_batch(&ck).unwrap();

    // 2. Prints the RecordBatch using arrow pretty print
    print_pretty_batches(&[rb]).unwrap();

    // 3. FYI, displays the total number of Boolean + U8 gates
    //    When tfhesql lib is compiled with 'stats' feature
    #[cfg(feature = "stats")]
    enc_sql_result.print_stats();
}
