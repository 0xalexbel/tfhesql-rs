# tfhesql-rs
A pure Rust library for executing simple FHE-encrypted SQL queries on a clear database using TFHE-rs

## APIs
the lib comes with two API flavours. The lib API and the bounty-specific API.
- The tfhesql API consists of a set of public structures allowing the user to run clear, encrypted and trivialy encrypted SQL querys. 
- The bounty-specific API is a strict implementation of the bounty requirements.

## Bounty API Example

```rust
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
```

## tfhesql API Example
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

    // 3. Generates a new SQL query with a SQL SELECT statement and the default options (compress = true, format = by rows + padding).
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

## The Problem & The Approach
1. Define a SQL query format
2. Write an SQL SELECT interpretor
3. Define a SQL result 

While progressing in solving the bounty, it became very clear that the final solution would be impracticable on real-life SQL databases. It's likely impracticable on very small SQL databases as well (kind of depressing 😩).

Thus, significant efforts were directed towards optimizing performance to enable the execution of a SQL SELECT request on a very small database. Emphasis was placed on refining both the SQL query and result formats.

The SQL interpreter was deliberately tested less for the following reasons:
- The extensive testing required.
- Possibility of using external libs to simplify the boolean ops binary tree.
- Users can optimize their queries, potentially nullifying the benefits of interpreter optimization.
- Although the bounty focused on the SQL interpreter, the underlying issue primarily lay in server-side computation strategies rather than the interpreter itself.

## SQL SELECT type comparison specs

The lib supports the MySQL SELECT type comparison specs.
- left > right <=> CastToNumber(left) > CastToNumber(right)
- -left <=> -(CastToNumber(left))
- +left <=> +(CastToNumber(left))
- CastToNumber(some_ascii) <=> parse the ascii string and convert it to a number. 0 if failed.  
- CastToNumber(some_bool) = 0 if some_bool is false, 1 if some_bool is true

Note: MSSQL type comparison specs are much more advanced.

## SQL AST Tree

1. Simplification: the AST Tree is simplified to get rid of the parenthesis, +/- signs and 'Not' unary operators.
2. Optimisation: the tree is parsed to eliminate trivial binary operations (for example: Some U8 > -1 is always true)
3. Conversion to Binary Tree: the tree is expanded to form a perfect binary tree with the following properties:
- Full binary tree
- Each node represents either a AND or a OR binary operation
- Each leaf is a Numerical or ASCII comparison with the following properties
    - **Left Operand**: is an column identifier (encoded as a boolean mask)
    - **Right Operand**: is a value (numerical or ASCII) or a column identifier stored in a structure named ``SqlQueryRightBytes256`` 
    - **Operator**: can only either  =, >, <, >=, <= or != (6 possibilities)

## Encoding the right operand

One of the major optimisation lies on the type of data sent to the server. The goal was to maximize performance at the expense of 
a bigger SQL Query size. The tradeoff appeared to be massively beneficial, and the relative large size of the request could be solved by using
``CompressedFheBool`` types plus a global zip to reduce the size of the query.

The right operand values are converted into 32xu8 values, each of these values are computed to produce 256 pre-calculated boolean pairs using the following formula:

```
With 0 <= i < 256
Pair(0,value,i) = (value == i) 
Pair(1,value,i) = (value > i) 
```
All the computed values are stored in a ``SqlQueryRightBytes256`` structure.

## Encrypted SQL Request format

```rust
struct EncryptedSqlQuery<B> {
    /// A header containing the crypted projections and table
    header: TableBoolMaskHeader<B>,
    /// A crypted boolean: True if running a SELECT DISTINCT query
    is_distinct: B,
    /// A structure defining the WHERE clause
    where_tree: SqlQueryTree<B>,
}

pub struct TableBoolMaskHeader<B> {
    /// A crypted boolean mask
    /// Len = the number of tables
    pub table_mask: BoolMask<B>,
    /// A crypted boolean mask 
    /// Len = Maximum number of columns in a single table
    pub field_mask: BoolMask<B>,
    /// A crypted boolean mask = NOT(field_mask)
    pub not_field_mask: BoolMask<B>,
}

pub struct SqlQueryTree<B> {
    /// A binary tree of BoolBinOpMask<B> nodes.
    /// Each node can be one of the following
    /// - a AND operator
    /// - a OR operator 
    /// - None
    tree: OptionalBoolTree<B>,
    /// A vector of boolean pairs (len = 2^k).
    /// One boolean pair for each leaf of `tree`.
    /// - True: the leaf is dummy
    /// - False: the leaf is a valid Binary Operation.
    pub(super) dummy_mask: Vec<EqNe<B>>,
    pub(super) compare_ops: SqlQueryBinOpArray<B>,
}

pub struct OptionalBoolTree<B> {
    // tree.len = 2^levels - 1
    tree: Vec<BoolBinOpMask<B>>,
}

pub struct BoolBinOpMask<B> {
    /// True is op is a binary AND operator
    pub is_and: B,
    /// True is op is a binary OR operator
    pub is_or: B,
}

pub struct SqlQueryBinOpArray<B> {
    pub(super) array: Vec<SqlQueryBinaryOp<B>>,
}

pub struct SqlQueryBinaryOp<B> {
    /// Leaf position is the binary tree
    /// The len is equal to 2^n = total number of leaves 
    pub position_mask: BoolMask<B>,
    /// Bool mask which encodes the operator: =, >, <, <=, >=, <>
    /// Len = 6
    pub comparator_mask: ComparatorMask<B>,
    /// The left operand column name encoded as a boolean mask 
    /// The mask len = the number of columns. The mask is full of zeros 
    /// except one 1 at the column index corresponding to the left operand column 
    /// identifier. 
    pub left_ident_mask: BoolMask<B>,
    /// The right operand (can be a value or an identifier)
    pub right: SqlQueryRightOperand<B>,
}

pub struct SqlQueryRightOperand<B> {
    /// The column mask (if not a numerical or ASCII value)
    pub ident_mask: BoolMask<B>,
    /// Right operand data encoded in structure of 4x64Bits words map
    pub bytes_256: SqlQueryRightBytes256<B>,
    /// True is the right operand is a numerical value strictly negative
    pub is_strictly_negative: EqNe<B>,
    /// True is the right operand is a value (numerical or ASCII)
    pub is_value: B,
}

/// A 4x64Bits map
pub struct SqlQueryRightBytes256<B> {
    pub word_0_eq_gt: Bytes64EqGt<B>,
    pub word_1_eq_ne: Bytes64EqNe<B>,
    pub word_2_eq_ne: Bytes64EqNe<B>,
    pub word_3_eq_ne: Bytes64EqNe<B>,
}

```

## Encrypted Result

The lib API offers 3 SQL result formats. For each format type a specific bunch of Bytes are computed. The bytes are later masked with the given table mask to produce the final encrypted result.

```rust
pub struct SqlResultOptions {
    compress: bool,
    format: SqlResultFormat,
}

pub enum SqlResultFormat {
    RowBytes(bool),
    TableBytesInRowOrder,
    TableBytesInColumnOrder,
}
```

- ``compress`` : If set to true (default), the bytes are compressed prior to table masking. 
- ``SqlResultFormat::RowBytes(padding)`` : The result is a two-dimensional array of bytes, where each entry corresponds to a row. For each row, an array of bytes is computed. A boolean padding option is available to obfuscate the result.
- ``SqlResultFormat::TableBytesInRowOrder`` : The result is a one-dimensional array of bytes, with all the rows concatenated to form a single byte array.
- ``SqlResultFormat::TableBytesInColumnOrder`` : The result is a one-dimensional array of bytes, with all the columns concatenated to form a single byte array.

### Final bytes order as stored in the SQL encrypted result structure
The following table has 4 columns and 2 rows:
| Col1 | Col2 | Col3 | Col4 |
|----|----|----|----|
| a | b | c | d | 
| e | f | g | h | 

The bytes are arranged as follow.
```rust
// SqlResultFormat::RowBytes(padding)
[
    [compress_byte_array(a, b, c, d)]
    [compress_byte_array(e, f, g, h)]
]

// SqlResultFormat::TableBytesInRowOrder
[compress_byte_array(a, b, c, d, e, f, g ,h)]

// SqlResultFormat::TableBytesInRowOrder
[compress_byte_array(a, e, b, f, c, g, d ,h)]

```


### The Rust encrypted structure

The following structure describes how the SQL result is computed. It consists of an encrypted part and a clear part. 
- The clear part contains only parameters necessary for client-side decryption of the result. It deliberately includes redundant information to facilitate self-decryption using a provided client key.
- The encrypted part contains the actual encrypted SQL result.

```rust
pub(crate) struct SqlResult<U8, B> {
    /// Encrypted part
    
    /// A boolean mask (redundant) that encodes the unique selected table
    table_mask: BoolMask<B>,
    /// A boolean mask (redundant) that encodes the multiple selected columns
    field_mask: BoolMask<B>,
    /// A boolean mask that encodes the selected rows computed from the SQL SELECT operation
    select_mask: BoolMask<B>,
    /// A two-dimentional array of encrypted bytes which encodes the table data values
    /// How this byte array is computed is explained below
    byte_arrays: Vec<ByteArray<U8>>,

    /// Clear part. Redundant, allows self-decryption.
    pub(crate) options: SqlResultOptions,
    pub(crate) ordered_schemas: OrderedSchemas,

    #[cfg(feature = "stats")]
    #[serde(skip_serializing, skip_deserializing)]
    pub(crate) stats: SqlStats,
}
```

## Benchmarks

The following results where optained running the `clear-api.rs` example located in the `tfhesql` lib examples directory. 

- The database used consists of the 2 following csv files:

| Table | Rows | Columns |
|-------|------|---------|
| Customers.csv | 91 | 1xUIn32 column and 6xASCII columns |
| Categories.csv | 8 | 1xUIn32 column and 2xASCII columns |

- The following SQL query is executed, it includes a IN clause thus doubling the numbers of '=' comparison operations.

```sql
SELECT CustomerID,PostalCode,Country FROM Customers WHERE Country IN ('France', 'Germany')
```

- The table below enumerates the total number of AND, OR and NOT operations. 

| Format                                     | Bool OR | UInt8 OR | Bool AND | UInt8 AND | Bool NOT | UInt8 NOT | IF | Total  | Gain	|
|--------------------------------------------|---------|----------|----------|-----------|----------|-----------|----|--------|---------|
| `compress=false`, `ByRow(true)`       | 18357   | 552      | 17533    | 18661     | 1029     | 0         | 0  | 113771 | 0 %     |
| `compress=false`, `TableBytesInRowOrder`    | 18357   | 569      | 17533    | 18430     | 1029     | 0         | 0  | 112915 | 1 %     |
| `compress=false`, `TableBytesInColumnOrder` | 18357   | 564      | 17533    | 18424     | 1029     | 0         | 0  | 112871 | 1 %     |
| `compress=true`, `ByRow(true)`       | 18357   | 455      | 17533    | 11102     | 1029     | 0         | 0  | 83147  | 27 %    |
| `compress=true`, `TableBytesInRowOrder`     | 18357   | 243      | 17533    | 5171      | 1029     | 0         | 0  | 58575  | 49 %    |
| `compress=true`, `TableBytesInColumnOrder`  | 18357   | 238      | 17533    | 5056      | 1029     | 0         | 0  | 58095  | 49 %    |

## Cost of result

One reason why the overhaul solution appears to always be somewhat impracticable is that a final U8 masking operation will always be performed on every single data value in every table of the database.

```
Total Number of U8 AND operations = Sum(0 <= t < n_tables; NumOfRows(t)*NumColumns(t))
```

## Where to go from here ?

- Returning the encrypted table looks interesting on paper, but makes the whole exercise impracticable. Furthermore, as pointed out in the comments, it does not bring any advantage privacy-wise since the table is clear for both the client and the server. This step really hurts.

- Performing SQL requests using row bounds may be feasable. The client would send a SQL SELECT request with predefined row bounds, thus limiting the CPU cost. The maximum row bounds could be controlled by the server.

- One area of investigation could be an encrypted protocol between the client and the server allowing the client to query specific row bounds in the db. Once the row bounds are decrypted on the client side, a concrete 'feasable' encrypted SQL query is sent by the client to the server.