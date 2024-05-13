# tfhesql-rs
A pure Rust library for executing simple FHE-encrypted SQL queries on a clear database using TFHE-rs

## API
the lib comes with two API flavours. The lib API and the bounty-specific API.

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


## Ast SQL Tree

1. Simplification: the Ast Tree is simplified to get rid of the parenthesis, +/- signs and 'Not' unary operators.
2. Optimisation: the tree is parsed to eliminate trivial binary operations (for example: Some U8 > -1 is always true)
3. Conversion to Binary Tree: the tree is expanded to form a perfect binary tree with the following properties:
- Full binary tree
- Each node represents either a AND or a OR binary operation
- Each leaf is a Numerical or ASCII comparison with the following properties
```
- Left Operand: a vector of 32 Bytes
- Right Operand: a vector of 32 Bytes
- Operator: Boolean Mask of len 6
```

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
