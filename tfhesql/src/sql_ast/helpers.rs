use super::data_ident::DataIdent;
use arrow_schema::Schema;
use sqlparser::ast::{BinaryOperator, Expr, Ident, UnaryOperator, Value, Visit, Visitor};
use std::ops::ControlFlow;

////////////////////////////////////////////////////////////////////////////////

pub(super) const fn not_binary_op(op: &BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Gt => BinaryOperator::LtEq,
        BinaryOperator::Lt => BinaryOperator::GtEq,
        BinaryOperator::GtEq => BinaryOperator::Lt,
        BinaryOperator::LtEq => BinaryOperator::Gt,
        BinaryOperator::Eq => BinaryOperator::NotEq,
        BinaryOperator::NotEq => BinaryOperator::Eq,
        BinaryOperator::And => BinaryOperator::Or,
        BinaryOperator::Or => BinaryOperator::And,
        _ => panic!("called `not_binary_op()` on an unsupported operator"),
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(super) const fn reflexive_binary_op(op: &BinaryOperator) -> BinaryOperator {
    match op {
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        BinaryOperator::Eq => BinaryOperator::Eq,
        BinaryOperator::NotEq => BinaryOperator::NotEq,
        BinaryOperator::And => BinaryOperator::And,
        BinaryOperator::Or => BinaryOperator::Or,
        _ => panic!("called `reflexive_binary_op()` on an unsupported operator"),
    }
}

////////////////////////////////////////////////////////////////////////////////

pub trait SqlExprLeaf
where
    Self: SqlExprValue + SqlExprIdentifier,
{
    #[inline]
    fn is_leaf(&self) -> bool {
        self.is_value_expr() || self.is_identifier_expr()
    }
}

impl<T> SqlExprLeaf for T where T: SqlExprIdentifier + SqlExprValue {}

pub trait SqlExprValue {
    fn is_value(&self) -> bool;
    fn is_string_value(&self) -> bool;
    #[inline]
    fn is_value_expr(&self) -> bool {
        self.try_find_value_expr().is_some()
    }
    fn try_get_value(&self) -> Option<&Value>;
    fn try_find_value_expr(&self) -> Option<&Self>;
    fn try_find_value(&self) -> Option<&Value> {
        match self.try_find_value_expr() {
            Some(e) => e.try_get_value(),
            None => None,
        }
    }
    #[inline]
    fn is_true_value(&self) -> bool {
        match self.try_get_value() {
            Some(Value::Boolean(b)) => *b,
            _ => false,
        }
        // match self.try_get_value() {
        //     Some(v) => match v {
        //         Value::Boolean(b) => *b,
        //         _ => false,
        //     },
        //     None => false,
        // }
    }
    #[inline]
    fn is_false_value(&self) -> bool {
        match self.try_get_value() {
            Some(Value::Boolean(b)) => !*b,
            _ => false,
        }
    }
}

impl SqlExprValue for Expr {
    #[inline]
    fn is_value(&self) -> bool {
        matches!(self, Expr::Value(_))
    }

    #[inline]
    fn is_string_value(&self) -> bool {
        match self {
            Expr::Value(value) => matches!(
                value,
                Value::SingleQuotedString(_)
                    | Value::DoubleQuotedString(_)
                    | Value::UnQuotedString(_)
            ),
            _ => false,
        }
    }

    #[inline]
    fn try_get_value(&self) -> Option<&Value> {
        match self {
            Expr::Value(value) => Some(value),
            _ => None,
        }
    }

    fn try_find_value_expr(&self) -> Option<&Expr> {
        let mut e = self;
        loop {
            match e {
                Expr::Value(_) => return Some(e),
                Expr::UnaryOp { op, expr } => match op {
                    UnaryOperator::Plus => e = expr.as_ref(),
                    UnaryOperator::Minus => e = expr.as_ref(),
                    UnaryOperator::Not => e = expr.as_ref(),
                    _ => return None,
                },
                _ => return None,
            }
        }
    }
}

pub trait SqlExprIdentifier {
    fn is_identifier(&self) -> bool;
    fn is_utf8_identifier(&self, schema: &Schema) -> bool;
    #[inline]
    fn is_identifier_expr(&self) -> bool {
        self.try_find_identifier_expr().is_some()
    }
    fn try_get_ident(&self) -> Option<&Ident>;
    fn try_find_identifier_expr(&self) -> Option<&Self>;
    fn try_find_ident(&self) -> Option<&Ident> {
        match self.try_find_identifier_expr() {
            Some(e) => e.try_get_ident(),
            None => None,
        }
    }
    #[inline]
    fn is_plus_minus_not_identifier(&self) -> bool
    where
        Self: SqlExprUnaryOp,
    {
        match self.try_get_unary_operand() {
            Some(e) => e.is_identifier(),
            None => false,
        }
    }
    #[inline]
    fn is_plus_identifier(&self) -> bool
    where
        Self: SqlExprUnaryOp,
    {
        match self.try_get_plus_operand() {
            Some(e) => e.is_identifier(),
            None => false,
        }
    }
    #[inline]
    fn is_minus_identifier(&self) -> bool
    where
        Self: SqlExprUnaryOp,
    {
        match self.try_get_minus_operand() {
            Some(e) => e.is_identifier(),
            None => false,
        }
    }
    #[inline]
    fn is_not_identifier(&self) -> bool
    where
        Self: SqlExprUnaryOp,
    {
        match self.try_get_not_operand() {
            Some(e) => e.is_identifier(),
            None => false,
        }
    }
}

impl SqlExprIdentifier for Expr {
    #[inline]
    fn is_identifier(&self) -> bool {
        matches!(self, Expr::Identifier(_))
    }
    fn is_utf8_identifier(&self, schema: &Schema) -> bool {
        match self.try_get_ident() {
            Some(ident) => match DataIdent::try_from_ident(ident, schema) {
                Ok(data_ident) => data_ident.is_ascii(),
                Err(_) => false,
            },
            None => false,
        }
    }

    #[inline]
    fn try_get_ident(&self) -> Option<&Ident> {
        match self {
            Expr::Identifier(ident) => Some(ident),
            _ => None,
        }
    }

    fn try_find_identifier_expr(&self) -> Option<&Expr> {
        let mut e = self;
        loop {
            match e {
                Expr::Identifier(_) => return Some(e),
                Expr::UnaryOp { op, expr } => match op {
                    UnaryOperator::Plus => e = expr.as_ref(),
                    UnaryOperator::Minus => e = expr.as_ref(),
                    UnaryOperator::Not => e = expr.as_ref(),
                    _ => return None,
                },
                _ => return None,
            }
        }
    }
}

pub(super) trait SqlExprUnaryOp {
    fn is_unary_op(&self, op: UnaryOperator) -> bool;
    #[inline]
    fn is_unary(&self) -> bool {
        self.is_minus() || self.is_plus() || self.is_not()
    }
    #[inline]
    fn is_plus_minus(&self) -> bool {
        self.is_minus() || self.is_plus()
    }
    #[inline]
    fn is_minus(&self) -> bool {
        self.is_unary_op(UnaryOperator::Minus)
    }
    #[inline]
    fn is_plus(&self) -> bool {
        self.is_unary_op(UnaryOperator::Plus)
    }
    #[inline]
    fn is_not(&self) -> bool {
        self.is_unary_op(UnaryOperator::Not)
    }
    fn try_get_unary_op_operand(&self, op: UnaryOperator) -> Option<&Self>;
    fn try_get_unary_operand(&self) -> Option<&Self>;
    #[inline]
    fn try_get_plus_minus_operand(&self) -> Option<&Self> {
        if self.is_plus() {
            self.try_get_plus_operand()
        } else if self.is_minus() {
            self.try_get_minus_operand()
        } else {
            None
        }
    }
    #[inline]
    fn try_get_minus_operand(&self) -> Option<&Self> {
        self.try_get_unary_op_operand(UnaryOperator::Minus)
    }
    #[inline]
    fn try_get_plus_operand(&self) -> Option<&Self> {
        self.try_get_unary_op_operand(UnaryOperator::Plus)
    }
    #[inline]
    fn try_get_not_operand(&self) -> Option<&Self> {
        self.try_get_unary_op_operand(UnaryOperator::Not)
    }

    #[inline]
    fn eq_minus_of(&self, rhs: &Self) -> bool
    where
        Self: Eq,
    {
        if self.is_minus() {
            if rhs.is_minus() {
                // -left, -right
                false
            } else {
                // -left, right
                let left = self.try_get_minus_operand().unwrap();
                left == rhs
            }
        } else if rhs.is_minus() {
            // left, -right
            let right = rhs.try_get_minus_operand().unwrap();
            self == right
        } else {
            false
        }
    }

    #[inline]
    fn eq_not_of(&self, rhs: &Self) -> bool
    where
        Self: Eq,
    {
        if self.is_not() {
            if rhs.is_not() {
                // !left, !right
                false
            } else {
                // !left, right
                let left = self.try_get_not_operand().unwrap();
                left == rhs
            }
        } else if rhs.is_not() {
            // left, !right
            let right = rhs.try_get_not_operand().unwrap();
            self == right
        } else {
            false
        }
    }
}

impl SqlExprUnaryOp for Expr {
    #[inline]
    fn is_unary_op(&self, op: UnaryOperator) -> bool {
        if let Expr::UnaryOp { op: u_op, .. } = self {
            matches!(
                (u_op, op),
                (UnaryOperator::Plus, UnaryOperator::Plus)
                    | (UnaryOperator::Minus, UnaryOperator::Minus)
                    | (UnaryOperator::Not, UnaryOperator::Not)
            )
            // match (u_op, op) {
            //     (UnaryOperator::Plus, UnaryOperator::Plus)
            //     | (UnaryOperator::Minus, UnaryOperator::Minus)
            //     | (UnaryOperator::Not, UnaryOperator::Not) => true,
            //     _ => false,
            // }
        } else {
            false
        }
    }
    #[inline]
    fn try_get_unary_op_operand(&self, op: UnaryOperator) -> Option<&Self> {
        if let Expr::UnaryOp { op: u_op, expr } = self {
            match (u_op, op) {
                (UnaryOperator::Plus, UnaryOperator::Plus)
                | (UnaryOperator::Minus, UnaryOperator::Minus)
                | (UnaryOperator::Not, UnaryOperator::Not) => Some(expr),
                _ => None,
            }
        } else {
            None
        }
    }
    #[inline]
    fn try_get_unary_operand(&self) -> Option<&Self> {
        if let Expr::UnaryOp { op, expr } = self {
            match op {
                UnaryOperator::Plus | UnaryOperator::Minus | UnaryOperator::Not => Some(expr),
                _ => None,
            }
        } else {
            None
        }
    }
}

#[allow(unused_macros)]
macro_rules! binary_op {
    ($left:expr, $op:expr, $right:expr) => {
        Expr::BinaryOp {
            left: Box::new($left.clone()),
            op: $op,
            right: Box::new($right.clone()),
        }
    };
}
#[allow(unused_imports)]
pub(crate) use binary_op;

#[inline]
pub(super) fn make_binary_op(left: &Expr, op: BinaryOperator, right: &Expr) -> Box<Expr> {
    Box::new(Expr::BinaryOp {
        left: Box::new(left.clone()),
        op,
        right: Box::new(right.clone()),
    })
}

#[inline]
pub(super) fn make_a_gt_b(left: &Expr, right: &Expr) -> Box<Expr> {
    make_binary_op(left, BinaryOperator::Gt, right)
}

#[inline]
pub(super) fn make_a_gteq_b(left: &Expr, right: &Expr) -> Box<Expr> {
    make_binary_op(left, BinaryOperator::GtEq, right)
}

#[inline]
pub(super) fn make_a_lt_b(left: &Expr, right: &Expr) -> Box<Expr> {
    make_binary_op(left, BinaryOperator::Lt, right)
}

#[inline]
pub(super) fn make_a_lteq_b(left: &Expr, right: &Expr) -> Box<Expr> {
    make_binary_op(left, BinaryOperator::LtEq, right)
}

#[inline]
pub(super) fn make_minus_a(a: Box<Expr>) -> Box<Expr> {
    Box::new(Expr::UnaryOp {
        op: UnaryOperator::Minus,
        expr: a,
    })
}

/// (a AND b)
#[inline]
pub(super) fn make_a_and_b(a: Box<Expr>, b: Box<Expr>) -> Box<Expr> {
    Box::new(Expr::BinaryOp {
        left: a,
        op: BinaryOperator::And,
        right: b,
    })
}

/// (a AND b)
#[inline]
pub(super) fn make_a_or_b(a: Box<Expr>, b: Box<Expr>) -> Box<Expr> {
    Box::new(Expr::BinaryOp {
        left: a,
        op: BinaryOperator::Or,
        right: b,
    })
}

/// a cmp num, for example a < 0
#[inline]
pub(super) fn make_a_cmp_to_num(a: Box<Expr>, op: BinaryOperator, num: u64) -> Box<Expr> {
    Box::new(Expr::BinaryOp {
        left: a,
        op,
        right: Box::new(Expr::Value(Value::Number(num.to_string(), false))),
    })
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct TreeInfo {
    pub levels: u32,
    pub count: u32,
    pub count_at_depth: Vec<u32>,
}

pub(super) fn compute_expr_tree_info(expr: &Expr) -> Option<TreeInfo> {
    struct V {
        info: TreeInfo,
        depth: u32,
        last_visit_was_pre: bool,
        last_visit_was_post: bool,
    }
    impl V {
        fn inc_depth(&mut self) {
            self.depth += 1;
            if self.info.levels < self.depth {
                self.info.levels = self.depth;
            }
            if self.info.count_at_depth.len() < self.depth as usize {
                assert_eq!(self.info.count_at_depth.len() + 1, self.depth as usize);
                self.info.count_at_depth.push(0);
                assert_eq!(self.info.count_at_depth.len(), self.depth as usize);
            }
            assert!(self.info.count_at_depth.len() == self.info.levels as usize);
            assert!(self.info.count_at_depth.len() >= self.depth as usize);
            assert!(self.depth > 0);
        }
        fn dec_depth(&mut self) {
            assert!(self.depth > 0);
            self.depth -= 1;
        }
        fn inc_at_depth(&mut self, d: u32) {
            self.info.count += 1;
            assert!(d > 0);
            assert!(self.info.count_at_depth.len() == self.info.levels as usize);
            let n = self.info.count_at_depth[(d - 1) as usize];
            self.info.count_at_depth[(d - 1) as usize] = n + 1;
        }
    }

    impl Visitor for V {
        type Break = ();

        fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
            if self.last_visit_was_pre {
                self.inc_depth();
            }
            assert!(self.depth > 0);
            self.inc_at_depth(self.depth);
            self.last_visit_was_pre = true;
            self.last_visit_was_post = false;
            ControlFlow::Continue(())
        }

        fn post_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
            assert!(self.depth > 0);
            if self.last_visit_was_post {
                self.dec_depth();
            }
            self.last_visit_was_post = true;
            self.last_visit_was_pre = false;
            ControlFlow::Continue(())
        }
    }

    let mut visitor = V {
        info: TreeInfo::default(),
        depth: 0,
        last_visit_was_pre: true,
        last_visit_was_post: false,
    };
    let flow = expr.visit(&mut visitor);

    match flow {
        ControlFlow::Continue(_) => Some(visitor.info),
        ControlFlow::Break(()) => None,
    }
}

pub trait ExprTreePositionPermission {
    fn allowed_as_parent(&self) -> bool;
    fn allowed_as_leaf(&self) -> bool;
}

impl ExprTreePositionPermission for BinaryOperator {
    fn allowed_as_parent(&self) -> bool {
        match self {
            BinaryOperator::Gt => false,
            BinaryOperator::Lt => false,
            BinaryOperator::GtEq => false,
            BinaryOperator::LtEq => false,
            BinaryOperator::Eq => false,
            BinaryOperator::NotEq => false,
            BinaryOperator::And => true,
            BinaryOperator::Or => true,
            BinaryOperator::Xor => false,
            _ => false,
        }
    }

    fn allowed_as_leaf(&self) -> bool {
        match self {
            BinaryOperator::Gt => true,
            BinaryOperator::Lt => true,
            BinaryOperator::GtEq => true,
            BinaryOperator::LtEq => true,
            BinaryOperator::Eq => true,
            BinaryOperator::NotEq => true,
            BinaryOperator::And => false,
            BinaryOperator::Or => false,
            BinaryOperator::Xor => false,
            _ => false,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::sql_ast::to_parenthesized_string::*;
    use crate::sql_ast::{CloneWhereStatement, SqlExprRemoveNestedInPlace};

    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn test() {
        let dialect = GenericDialect {}; // or AnsiDialect
        let where_clauses = [
            "(a > b) AND (c > d)",
            "((a > (b)) AND (c > (d)))",
            "(a > (b)) AND (c > (d)) OR (a IN ('a', 'b')) AND (-1)",
            "(a > b) AND (c > (d BETWEEN 10 AND (x OR y)))",
        ];
        let expected_result = [
            "((a > b) AND (c > d))",
            "((a > b) AND (c > d))",
            "(((a > b) AND (c > d)) OR ((a IN ('a', 'b')) AND (-1)))",
            "((a > b) AND (c > (d BETWEEN 10 AND (x OR y))))",
        ];
        where_clauses
            .iter()
            .zip(expected_result.iter())
            .for_each(|(w, e)| {
                let sql = format!("SELECT * FROM t WHERE {}", w);
                let statements = Parser::parse_sql(&dialect, &sql).unwrap();
                assert!(!statements.is_empty());
                let mut where_expr = statements[0].clone_where().unwrap();
                where_expr.as_mut().remove_nested_in_place().unwrap();
                let str = where_expr.to_parenthesized_string();
                println!("{}", str);
                assert_eq!(str, *e);
            });
    }

    #[test]
    fn test_info() {
        let dialect = GenericDialect {}; // or AnsiDialect
        let where_clauses = [
            "a",
            "a > b",
            "(a > b) AND c",
            "(a > b) AND (c > d)",
            "(a > b) AND ((c > d) AND e)",
            "(a > b) AND ((c > d) AND (e > f))",
            "((a > b) AND (c > d)) AND (e > f)",
            "(a > (f > g)) AND ((c > d) AND e)",
            "h OR ((a > (f > g)) AND ((c > d) AND e))",
        ];
        let expected_result = [
            TreeInfo {
                levels: 1,
                count: 1,
                count_at_depth: vec![1],
            },
            TreeInfo {
                levels: 2,
                count: 3,
                count_at_depth: vec![1, 2],
            },
            TreeInfo {
                levels: 3,
                count: 5,
                count_at_depth: vec![1, 2, 2],
            },
            TreeInfo {
                levels: 3,
                count: 7,
                count_at_depth: vec![1, 2, 4],
            },
            TreeInfo {
                levels: 4,
                count: 9,
                count_at_depth: vec![1, 2, 4, 2],
            },
            TreeInfo {
                levels: 4,
                count: 11,
                count_at_depth: vec![1, 2, 4, 4],
            },
            TreeInfo {
                levels: 4,
                count: 11,
                count_at_depth: vec![1, 2, 4, 4],
            },
            TreeInfo {
                levels: 4,
                count: 11,
                count_at_depth: vec![1, 2, 4, 4],
            },
            TreeInfo {
                levels: 5,
                count: 13,
                count_at_depth: vec![1, 2, 2, 4, 4],
            },
        ];
        where_clauses
            .iter()
            .zip(expected_result.iter())
            .for_each(|(w, e)| {
                let sql = format!("SELECT * FROM t WHERE {}", w);
                let statements = Parser::parse_sql(&dialect, &sql).unwrap();
                assert!(!statements.is_empty());
                let mut where_expr = statements[0].clone_where().unwrap();
                where_expr.as_mut().remove_nested_in_place().unwrap();
                let info = compute_expr_tree_info(&where_expr).unwrap();
                assert_eq!(info, *e);
            });
    }
}
