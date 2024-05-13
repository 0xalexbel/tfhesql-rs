# tfhesql-rs
A pure Rust library for executing simple FHE-encrypted SQL queries on a clear database using TFHE-rs

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
    /// A binary tree of OptionalBool<B> nodes.
    /// Each node can be one of the following
    /// - a AND operator
    /// - a OR operator 
    /// - None
    tree: OptionalBoolTree<B>,
    /// A vector of boolean pairs.
    /// One boolean pair for each leaf of the Ast Binary Tree
    pub(super) dummy_mask: Vec<EqNe<B>>,
    pub(super) compare_ops: SqlQueryBinOpArray<B>,
}

pub struct OptionalBool<B> {
    /// A crypted boolean
    pub value: B,
    /// A crypted boolean
    pub none_some: Option<EqNe<B>>,
}


```
