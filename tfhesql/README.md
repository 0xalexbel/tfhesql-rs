
## Quick start

### Example using the Clear API

``tfhesql/examples/api-clear`` is an example running a SQL SELECT command using clear bools and u8.

```bash
$ cd /path/to/tfhesql-rs
$ cargo run --release --example api-clear
```

```rust
use tfhesql::test_util::{print_pretty_batches, tfhesql_test_db_dir};
use tfhesql::*;

fn main() {
    let csv_dir = tfhesql_test_db_dir("medium");

    // Client Side
    // ===========

    // 1. Load the SAME schemas in the SAME order as the server.
    //    This is critical since server and client must share
    //    the same table + schema order
    let client_ordered_schemas = OrderedSchemas::load_from_directory(&csv_dir).unwrap();

    // 2. Creates a new FheSqlClient instance
    let sql_client = FheSqlClient::new(client_ordered_schemas.clone()).unwrap();

    // 3. Generates a new SQL query with a SQL SELECT statement 
    let sql = "SELECT CustomerID,PostalCode,Country FROM Customers WHERE Country IN ('France', 'Germany')";
    let clear_sql_query = sql_client.clear_sql(sql, SqlResultOptions::best()).unwrap();

    // Server Side
    // ===========

    // 1. Load csv file located in the specified directory and stores them into an ordered list of tables.
    //    Note: Order is critical and should remain sealed since all the masking operations between the client
    //    and the server are based on it.
    let server_tables = OrderedTables::load_from_directory(&csv_dir).unwrap();

    // 2. Executes the SQL query on the server
    let clear_sql_result = FheSqlServer::run(&clear_sql_query, &server_tables).unwrap();

    // Client Side
    // ===========

    // 1. Extract the RecordBatch from the SQL query result
    let rb = clear_sql_result.clone().into_record_batch().unwrap();

    // 2. Prints the RecordBatch using arrow pretty print
    print_pretty_batches(&[rb]).unwrap();

    // 3. FYI, displays the total number of Boolean + U8 gates
    //    When tfhesql lib is compiled with 'stats' feature
    #[cfg(feature = "stats")]
    clear_sql_result.print_stats();
}
```

### Example using the encrypt API (using TFHE-rs)

``tfhesql/examples/api-enc`` is an example running a SQL SELECT command using FheBool and FheUInt8

```bash
$ cd /path/to/tfhesql-rs
$ cargo run --release --example api-enc
```

### Example using the bounty API (using TFHE-rs)

``tfhesql/examples/api-bounty`` is an example running a SQL SELECT command using the bounty-specific API.

```bash
$ cd /path/to/tfhesql-rs
$ cargo run --release --example api-bounty
```

