# tfhesql-rs
A pure Rust library for executing simple FHE-encrypted SQL queries on a clear database using TFHE-rs

## Ast SQL Tree

1. In a first set of steps, the Ast Tree is simplified to get rid of the parenthesis, +/- signs and 'Not' unary operators.
2. The tree is 
Since the ast tree can be ill-formed, the preferred strategy is to transform the ill-formed binary. 

## Encrypted SQL Request format

```rust
struct EncryptedSqlQuery<B> {
    /// A header containing the crypted projections and table
    header: TableBoolMaskHeader<B>,
    /// A boolean: True if SELECT DISTINCT 
    is_distinct: B,
    /// A structure defining the WHERE clause
    where_tree: SqlQueryTree<B>,
}

pub struct TableBoolMaskHeader<B> {
    /// A boolean mask
    /// Len = the number of tables
    pub table_mask: BoolMask<B>,
    /// A boolean mask 
    /// Len = Maximum number of columns in a single table
    pub field_mask: BoolMask<B>,
    /// A boolean mask = NOT(field_mask)
    pub not_field_mask: BoolMask<B>,
}

pub struct SqlQueryTree<B> {
    /// A binary tree where all the nodes is either a AND or a OR
    tree: OptionalBoolTree<B>,
    /// A vector of boolean pairs.
    /// One boolean pair for each leaf of the Ast Binary Tree
    pub(super) dummy_mask: Vec<EqNe<B>>,
    pub(super) compare_ops: SqlQueryBinOpArray<B>,
}

```
