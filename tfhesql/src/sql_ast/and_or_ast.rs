use arrow_schema::Schema;
use sqlparser::ast::BinaryOperator;
use sqlparser::ast::Expr;

use crate::error::FheSqlError;
use crate::hi_lo_tree::ClearEqNe;
use crate::hi_lo_tree::EqNe;
use crate::sql_ast::helpers::SqlExprIdentifier;
use crate::sql_ast::helpers::SqlExprValue;
use crate::uint::mask::ClearBoolMask;
use crate::uint::two_pow_n;

use super::bitop_mask::ClearBitOpMask;
use super::comparator_mask::ClearComparatorMask;
use super::data_ident::DataIdent;
use super::data_sig::DataSig;
use super::data_value::DataValue;
use super::helpers::compute_expr_tree_info;

////////////////////////////////////////////////////////////////////////////////
// AstRightValue
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub enum AstRightValue {
    Number(u64),
    Ascii(String),
}

////////////////////////////////////////////////////////////////////////////////
// AstRightValue
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct AstNumBinaryOp {
    pub is_dummy: bool,
    pub pos_mask: ClearBoolMask,
    pub op_mask: ClearComparatorMask,
    pub left_ident_mask: ClearBoolMask,
    pub right_ident_mask: ClearBoolMask,
    pub right_value: AstRightValue,
    pub right_minus_sign: bool,
}

impl AstNumBinaryOp {
    fn new(num_cols: usize) -> Self {
        Self {
            is_dummy: true,
            pos_mask: ClearBoolMask::new_empty(),
            op_mask: ClearComparatorMask::none(),
            left_ident_mask: ClearBoolMask::none(num_cols), //can all be false
            right_ident_mask: ClearBoolMask::none(num_cols),
            right_value: AstRightValue::Number(0),
            right_minus_sign: false,
        }
    }

    fn set(
        &mut self,
        op: &BinaryOperator,
        left: &DataIdent,
        right: &DataSig,
    ) -> Result<(), FheSqlError> {
        self.is_dummy = false;
        self.op_mask.set(op);
        self.left_ident_mask.set(left.column_index());
        match right {
            DataSig::Value(v_right) => match v_right {
                DataValue::Bool(b) => {
                    if *b {
                        self.right_value = AstRightValue::Number(1);
                    } else {
                        self.right_value = AstRightValue::Number(0);
                    }
                }
                DataValue::Num(num) => {
                    self.right_value = AstRightValue::Number(num.abs());
                    self.right_minus_sign = num.is_strictly_negative();
                    // -0 is NOT allowed!
                    assert!(num.abs() != 0 || !self.right_minus_sign);
                }
                DataValue::Ascii(str) => {
                    if self.right_minus_sign {
                        panic!("compilation error, unexpected minus sign")
                    }
                    self.right_value = AstRightValue::Ascii(str.clone())
                }
            },
            DataSig::Ident(i_right) => {
                self.right_ident_mask.set(i_right.column_index());
                self.right_minus_sign = i_right.minus_sign();
            }
        }
        Ok(())
    }
}

impl std::fmt::Display for AstNumBinaryOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_dummy {
            f.write_str("--")
        } else {
            f.write_str(self.op_mask.to_string().as_str())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// AstTree
////////////////////////////////////////////////////////////////////////////////

pub struct AstTree {
    bool_ops_tree_levels: u32,
    bool_ops_tree: Vec<ClearBitOpMask>,
    num_ops: Vec<AstNumBinaryOp>,
}

impl AstTree {
    fn with_levels(levels: u8, num_cols: usize) -> Self {
        assert!(num_cols <= 1024);
        // 1 operator and 2 operands minimum = 2 levels (depth=1)
        assert!(levels >= 2);

        let bool_ops_tree_levels = (levels as u32) - Self::num_ops_tree_levels();
        let bool_ops_count = (two_pow_n(bool_ops_tree_levels as u8) - 1) as usize;
        // A leaf is Tree of depth 1
        let num_ops_count = two_pow_n(bool_ops_tree_levels as u8) as usize;
        AstTree {
            bool_ops_tree_levels,
            bool_ops_tree: vec![ClearBitOpMask::new_noop(); bool_ops_count],
            num_ops: vec![AstNumBinaryOp::new(num_cols); num_ops_count],
        }
    }

    pub fn bool_ops_tree(&self) -> &Vec<ClearBitOpMask> {
        &self.bool_ops_tree
    }

    pub fn num_ops(&self) -> &Vec<AstNumBinaryOp> {
        &self.num_ops
    }

    pub fn compute_positions(&mut self) {
        let n_ops = self.num_ops.len();
        let n_dummies = self
            .num_ops
            .iter()
            .fold(0, |acc, op| if op.is_dummy { acc + 1 } else { acc });
        self.num_ops
            .iter_mut()
            .enumerate()
            .filter(|(_, op)| !op.is_dummy)
            .enumerate()
            .for_each(|(non_dummy_pos, (global_pos, op))| {
                let min = non_dummy_pos;
                let max = non_dummy_pos + n_dummies;
                assert!(max < n_ops);
                assert!(global_pos >= min);
                assert!(global_pos <= max);
                let mut mask = ClearBoolMask::none(n_dummies + 1);
                mask.set(global_pos - min);
                op.pos_mask = mask;
            });
        #[cfg(test)]
        self.test_positions(n_dummies);
    }

    pub fn dummy_not_dummy(&self) -> Vec<ClearEqNe> {
        self.num_ops
            .iter()
            .map(|x| EqNe {
                eq: x.is_dummy,
                ne: !x.is_dummy,
            })
            .collect()
    }

    #[cfg(test)]
    fn test_positions(&self, n_dummies: usize) {
        let mut count_dummies = 0;
        for i in 0..self.num_ops.len() {
            let op = &self.num_ops[i];
            if op.is_dummy {
                count_dummies += 1;
            }
        }
        assert_eq!(count_dummies, n_dummies);
        let mut i_non_dummy: i32 = -1;
        for i in 0..self.num_ops.len() {
            let op = &self.num_ops[i];
            if !op.is_dummy {
                i_non_dummy += 1;
                assert_eq!(
                    op.pos_mask.index_of_first_set().unwrap() + (i_non_dummy as usize),
                    i
                );
            }
        }
    }

    #[inline]
    fn levels(&self) -> u32 {
        self.bool_ops_tree_levels + Self::num_ops_tree_levels()
    }
    #[inline]
    fn depth(&self) -> i32 {
        (self.levels() as i32) - 1i32
    }
    #[inline]
    pub fn bool_ops_tree_levels(&self) -> u32 {
        self.bool_ops_tree_levels
    }
    #[inline]
    fn num_ops_tree_levels() -> u32 {
        // 2 levels = operand level + operator level
        2
    }
    #[inline]
    fn num_ops_depth(&self) -> u32 {
        // 2 levels = operand level + operator level
        assert!(self.depth() >= 1);
        (self.depth() as u32) - 1
    }

    #[cfg(test)]
    #[inline]
    fn min_depth(nodes: u64) -> u8 {
        use crate::uint::next_power_of_two;
        next_power_of_two(nodes + 1) - 1
    }

    #[inline]
    fn num_nodes_at_depth(depth: u8) -> usize {
        two_pow_n(depth) as usize
    }

    #[inline]
    fn index_at_depth(depth: u8) -> usize {
        (two_pow_n(depth) - 1) as usize
    }

    #[inline]
    fn node_at(&self, depth: u8, pos: usize) -> &ClearBitOpMask {
        assert!(pos < Self::num_nodes_at_depth(depth));
        let i = Self::index_at_depth(depth) + pos;
        &self.bool_ops_tree[i]
    }

    #[inline]
    fn node_at_mut(&mut self, depth: u8, pos: usize) -> &mut ClearBitOpMask {
        assert!(pos < Self::num_nodes_at_depth(depth));
        let i = Self::index_at_depth(depth) + pos;
        &mut self.bool_ops_tree[i]
    }

    #[inline]
    fn leaf_at_mut(&mut self, depth: u8, pos: usize) -> &mut AstNumBinaryOp {
        let leaf_index = self.dummy_pos_at(depth, pos);
        &mut self.num_ops[leaf_index]
    }

    fn dummy_pos_at(&self, depth: u8, pos: usize) -> usize {
        let sub_tree_depth = (self.depth() - 1 - (depth as i32)) as u8;
        (two_pow_n(sub_tree_depth) as usize) * pos
    }

    fn fill_leaf(
        &mut self,
        depth: u8,
        pos: usize,
        op: &BinaryOperator,
        left: &DataIdent,
        right: &DataSig,
    ) -> Result<(), FheSqlError> {
        let leaf = self.leaf_at_mut(depth, pos);
        leaf.set(op, left, right)
    }

    fn fill_node(&mut self, depth: u8, pos: usize, op: &BinaryOperator) -> Result<(), FheSqlError> {
        let node = self.node_at_mut(depth, pos);
        node.set(op)
    }
}

impl std::fmt::Display for AstTree {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut s = String::new();
        for i in 0..(self.bool_ops_tree_levels) {
            let d = i as u8;
            let n = AstTree::num_nodes_at_depth(d);
            for j in 0..n {
                let node = self.node_at(d, j);
                s.push_str(format!("depth={} pos={} node='{}'\n", d, j, node).as_str());
            }
        }
        for i in 0..self.num_ops.len() {
            s.push_str(
                format!(
                    "depth={} pos={} node='{}'\n",
                    self.num_ops_depth(),
                    i,
                    self.num_ops[i]
                )
                .as_str(),
            );
        }
        f.write_str(s.to_string().as_str())
    }
}

////////////////////////////////////////////////////////////////////////////////
// AstTreeResult
////////////////////////////////////////////////////////////////////////////////

pub enum AstTreeResult {
    Boolean(bool),
    Tree(AstTree),
}

impl AstTreeResult {
    pub fn is_false(&self) -> bool {
        match self {
            AstTreeResult::Boolean(b) => !*b,
            AstTreeResult::Tree(_) => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// compute_ast_tree
////////////////////////////////////////////////////////////////////////////////

pub fn compute_ast_tree(
    expr: &Expr,
    schema: &Schema,
    max_num_fields: usize,
) -> Result<AstTreeResult, FheSqlError> {
    fn fill_ast_tree(
        tree: &mut AstTree,
        expr: &Expr,
        depth: u8,
        pos: u64,
        schema: &Schema,
    ) -> Result<(), FheSqlError> {
        //f(expr, depth, pos);
        match expr {
            Expr::BinaryOp { left, op, right } => {
                assert!(!left.is_value());
                assert!(!left.is_minus_identifier());
                assert!(!right.is_plus_identifier());
                let left_is_leaf = left.is_identifier();
                let right_is_leaf =
                    right.is_value() || right.is_identifier() || right.is_minus_identifier();

                // No heterogeneous expressions
                assert_eq!(left_is_leaf, right_is_leaf);

                if left_is_leaf && right_is_leaf {
                    tree.fill_leaf(
                        depth,
                        pos as usize,
                        op,
                        DataSig::try_from_expr(left, schema)?.get_ident(),
                        &DataSig::try_from_expr(right, schema)?,
                    )?;
                    return Ok(());
                }

                tree.fill_node(depth, pos as usize, op)?;

                match fill_ast_tree(tree, left.as_ref(), depth + 1, 2 * pos, schema) {
                    Ok(_) => (),
                    Err(err) => return Err(err),
                };
                match fill_ast_tree(tree, right.as_ref(), depth + 1, 2 * pos + 1, schema) {
                    Ok(_) => (),
                    Err(err) => return Err(err),
                }

                Ok(())
            }
            _ => Err(FheSqlError::unsupported_expr(expr)),
        }
    }

    let tree_info = match compute_expr_tree_info(expr) {
        Some(info) => info,
        None => return Err(FheSqlError::unsupported_expr(expr)),
    };
    if tree_info.levels > u8::MAX as u32 {
        return Err(FheSqlError::unsupported_expr(expr));
    }
    if tree_info.levels == 1 {
        if expr.is_false_value() {
            return Ok(AstTreeResult::Boolean(false));
        }
        if expr.is_true_value() {
            return Ok(AstTreeResult::Boolean(true));
        }
        return Err(FheSqlError::unsupported_expr(expr));
    }

    assert!(tree_info.levels > 1);
    let mut tree = AstTree::with_levels(
        tree_info.levels as u8,
        max_num_fields,
    );
    fill_ast_tree(&mut tree, expr, 0, 0, schema)?;
    tree.compute_positions();

    Ok(AstTreeResult::Tree(tree))
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use arrow_schema::{Field, Schema};
    use sqlparser::{dialect::GenericDialect, parser::Parser};

    use crate::sql_ast::and_or_ast::{compute_ast_tree, AstTreeResult};
    use crate::sql_ast::to_parenthesized_string::ToParenthesizedString;
    use crate::sql_ast::{and_or_ast::AstTree, CompileWhereStatement};

    fn get_schema() -> Schema {
        Schema::new(vec![
            Field::new("a_s", arrow_schema::DataType::Utf8, false),
            Field::new("b_s", arrow_schema::DataType::Utf8, false),
            Field::new("c_s", arrow_schema::DataType::Utf8, false),
            Field::new("d_s", arrow_schema::DataType::Utf8, false),
            Field::new("a_b", arrow_schema::DataType::Boolean, false),
            Field::new("b_b", arrow_schema::DataType::Boolean, false),
            Field::new("c_b", arrow_schema::DataType::Boolean, false),
            Field::new("d_b", arrow_schema::DataType::Boolean, false),
            Field::new("a_i", arrow_schema::DataType::Int32, false),
            Field::new("b_i", arrow_schema::DataType::Int32, false),
            Field::new("c_i", arrow_schema::DataType::Int32, false),
            Field::new("d_i", arrow_schema::DataType::Int32, false),
            Field::new("a_u", arrow_schema::DataType::UInt32, false),
            Field::new("b_u", arrow_schema::DataType::UInt32, false),
            Field::new("c_u", arrow_schema::DataType::UInt32, false),
            Field::new("d_u", arrow_schema::DataType::UInt32, false),
        ])
    }

    #[test]
    fn test_fill() {
        let schema = get_schema();
        let dialect = GenericDialect {}; // or AnsiDialect
        let where_clauses = [
            //"a",
            // "a > b",
            //"(a > b) AND c",
            //"(a > b) AND (c > d) AND (a > b)",
            // "(a > b) AND ((c > d) AND e)",
            // "(a > b) = (c > d)",
            // "(a > b) = c",
            // "a = -3",
            // "a = +3",
            // "-a = 3",
            // "-a = -3",
            // "-a = +3",
            // "+a = 3",
            // "+a = +3",
            // "+a = -3",
            // "-a = b",
            // "-a = -b",
            // "-a = +b",
            // "+a = b",
            // "+a = -b",
            // "+a = +b",
            // "(a > b) AND ((c > d) AND (e > f))",
            // "((a > b) AND (c > d)) AND (e > f)",
            // "(a > (f > g)) AND ((c > d) AND e)",
            "b_u OR ((a_s > (a_b > a_u)) AND ((a_u > b_u) AND a_b))",
            // "NOT (a > b)",
            // "(a > b) AND NOT ((c > d) AND (e > f))",
            "a_i >= 52",
        ];
        let expected_result = [
            //"(a <> 0)",
            //"(a > b)",
            //"((a > b) AND (c <> 0))",
            "((b_u <> 0) OR (((a_s > 1) OR ((a_s = 1) AND (a_b <= a_u))) AND ((a_u > b_u) AND (a_b = true))))",
            "(a_i >= 52)",
            // "'AND' operator cannot be a leaf in 'a > b AND c'",
            // "",
            // "'AND' operator cannot be a leaf in 'c > d AND e'",
            // "'=' operator cannot be a parent in 'a > b = c > d'",
            // "'=' expression leaf mismatch error in 'a > b = c'",
            // "'=' expression leaf mismatch error in 'a = -3'",
            // "'=' expression leaf mismatch error in 'a = +3'",
            // "",
            // "'=' expression leaf mismatch error in '-a = -3'",
            // "'=' expression leaf mismatch error in '-a = +3'",
            // "'=' expression leaf mismatch error in '+a = 3'",
            // "'=' operator cannot be a parent in '+a = +3'",
            // "'=' operator cannot be a parent in '+a = -3'",
            // "",
            // "'=' expression leaf mismatch error in '-a = -b'",
            // "'=' expression leaf mismatch error in '-a = +b'",
            // "'=' expression leaf mismatch error in '+a = b'",
            // "'=' operator cannot be a parent in '+a = -b'",
            // "'=' operator cannot be a parent in '+a = +b'",
            // "",
            // "",
            // "'>' expression leaf mismatch error in 'a > f > g'",
            // "'OR' operator cannot be a leaf in 'h OR a > f > g AND c > d AND e'",
            // "'NOT' expression pre-processing error in 'NOT a > b'",
            // "'NOT' expression pre-processing error in 'NOT c > d AND e > f'",
        ];
        where_clauses
            .iter()
            .zip(expected_result.iter())
            .for_each(|(w, e)| {
                let sql = format!("SELECT * FROM t WHERE {}", w);
                let statements = Parser::parse_sql(&dialect, &sql).unwrap();
                assert!(!statements.is_empty());

                let compiled_where_expr = statements
                    .first()
                    .unwrap()
                    .compile_where(&schema)
                    .unwrap()
                    .unwrap();
                assert_eq!(compiled_where_expr.to_parenthesized_string(), *e);

                match compute_ast_tree(&compiled_where_expr, &schema, schema.fields().len())
                    .unwrap()
                {
                    AstTreeResult::Boolean(b) => println!("{}", b),
                    AstTreeResult::Tree(tree) => println!("{}", tree),
                };
            });
    }

    #[test]
    fn test() {
        assert_eq!(AstTree::min_depth(3), 1);
        assert_eq!(AstTree::min_depth(4), 2);
        assert_eq!(AstTree::min_depth(5), 2);
        assert_eq!(AstTree::min_depth(6), 2);
        assert_eq!(AstTree::min_depth(7), 2);
        assert_eq!(AstTree::min_depth(8), 3);
        assert_eq!(AstTree::min_depth(9), 3);
        assert_eq!(AstTree::min_depth(15), 3);
        assert_eq!(AstTree::min_depth(16), 4);
        assert_eq!(AstTree::index_at_depth(0), 0);
        assert_eq!(AstTree::index_at_depth(1), 1);
        assert_eq!(AstTree::index_at_depth(2), 3);
        assert_eq!(AstTree::index_at_depth(3), 7);
    }
}
