use super::helpers::*;
use sqlparser::ast::{Expr, UnaryOperator, Visit, Visitor};
use std::{
    fmt::{Display, Formatter},
    ops::ControlFlow,
};

#[derive(Debug, PartialEq)]
pub enum ValidateExprTreeError {
    PreProcessing(String),
    OperatorPreProcessing(String, String),
    LeafMismatch(String, String),
    LeafPermission(String, String),
    ParentPermission(String, String),
}
// An expression of non-boolean type specified in a context where a condition is expected, near ';'.
impl ValidateExprTreeError {
    pub fn preprocessing_error(expr: &Expr) -> Self {
        ValidateExprTreeError::PreProcessing(expr.to_string())
    }
    pub fn op_preprocessing_error(op: &str, expr: &Expr) -> Self {
        ValidateExprTreeError::OperatorPreProcessing(op.to_string(), expr.to_string())
    }
    pub fn leaf_mismatch_error(op: &str, expr: &Expr) -> Self {
        ValidateExprTreeError::LeafMismatch(op.to_string(), expr.to_string())
    }
    pub fn leaf_permission_error(op: &str, expr: &Expr) -> Self {
        ValidateExprTreeError::LeafPermission(op.to_string(), expr.to_string())
    }
    pub fn parent_permission_error(op: &str, expr: &Expr) -> Self {
        ValidateExprTreeError::ParentPermission(op.to_string(), expr.to_string())
    }
}

impl Display for ValidateExprTreeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidateExprTreeError::PreProcessing(expr) => {
                write!(f, "Pre-processing error in {expr}")
            }
            ValidateExprTreeError::OperatorPreProcessing(op, expr) => {
                write!(f, "'{op}' expression pre-processing error in '{expr}'")
            }
            ValidateExprTreeError::LeafMismatch(op, expr) => {
                write!(f, "'{op}' expression leaf mismatch error in '{expr}'")
            }
            ValidateExprTreeError::LeafPermission(op, expr) => {
                write!(f, "'{op}' operator cannot be a leaf in '{expr}'")
            }
            ValidateExprTreeError::ParentPermission(op, expr) => {
                write!(f, "'{op}' operator cannot be a parent in '{expr}'")
            }
        }
    }
}

pub fn validate_where_expr_tree(expr: &Expr) -> Result<(), ValidateExprTreeError> {
    struct V {}

    impl Visitor for V {
        type Break = ValidateExprTreeError;

        fn pre_visit_expr(&mut self, _expr: &Expr) -> ControlFlow<Self::Break> {
            match _expr {
                Expr::Identifier(_) => ControlFlow::Continue(()),
                Expr::Between { .. } => {
                    ControlFlow::Break(ValidateExprTreeError::preprocessing_error(_expr))
                }
                Expr::InList { .. } => {
                    ControlFlow::Break(ValidateExprTreeError::preprocessing_error(_expr))
                }
                Expr::BinaryOp { left, op, right } => {
                    // left can be 'column', '-column', '3'
                    let left_is_leaf = left.is_value() || left.is_identifier() || left.is_minus_identifier();
                    // left can be 'column', '3' ('-column' is forbidden)
                    let right_is_leaf = right.is_value() || right.is_identifier();

                    if (left_is_leaf || right_is_leaf) && !op.allowed_as_leaf() {
                        ControlFlow::Break(ValidateExprTreeError::leaf_permission_error(
                            &op.to_string(),
                            _expr,
                        ))
                    } else if (!left_is_leaf && !right_is_leaf) && !op.allowed_as_parent() {
                        ControlFlow::Break(ValidateExprTreeError::parent_permission_error(
                            &op.to_string(),
                            _expr,
                        ))
                    } else if left_is_leaf != right_is_leaf {
                        ControlFlow::Break(ValidateExprTreeError::leaf_mismatch_error(
                            &op.to_string(),
                            _expr,
                        ))
                    } else {
                        ControlFlow::Continue(())
                    }
                }
                Expr::UnaryOp { op, expr } => match op {
                    UnaryOperator::Minus => {
                        // '-column' is allowed
                        if matches!(expr.as_ref(), Expr::Identifier(_)) {
                            ControlFlow::Continue(())
                        } else {
                            ControlFlow::Break(ValidateExprTreeError::op_preprocessing_error(
                                "-", _expr,
                            ))
                        }
                    }
                    _ => ControlFlow::Break(ValidateExprTreeError::op_preprocessing_error(
                        &op.to_string(),
                        _expr,
                    )),
                },
                Expr::Nested(_) => {
                    ControlFlow::Break(ValidateExprTreeError::op_preprocessing_error("()", _expr))
                }
                Expr::Value(_) => ControlFlow::Continue(()),
                _ => ControlFlow::Break(ValidateExprTreeError::preprocessing_error(_expr)),
            }
        }
    }

    let mut visitor = V {};
    let flow = expr.visit(&mut visitor);

    match flow {
        ControlFlow::Continue(_) => Ok(()),
        ControlFlow::Break(err) => Err(err),
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use crate::sql_ast::{CloneWhereStatement, SqlExprRemoveNestedInPlace};

    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn test() {
        let dialect = GenericDialect {}; // or AnsiDialect
        let where_clauses = [
            "a",
            "a > b",
            "(a > b) AND c",
            "(a > b) AND (c > d)",
            "(a > b) AND ((c > d) AND e)",
            "(a > b) = (c > d)",
            "(a > b) = c",
            "a = -3",
            "a = +3",
            "-a = 3",
            "-a = -3",
            "-a = +3",
            "+a = 3",
            "+a = +3",
            "+a = -3",
            "-a = b",
            "-a = -b",
            "-a = +b",
            "+a = b",
            "+a = -b",
            "+a = +b",
            "(a > b) AND ((c > d) AND (e > f))",
            "((a > b) AND (c > d)) AND (e > f)",
            "(a > (f > g)) AND ((c > d) AND e)",
            "h OR ((a > (f > g)) AND ((c > d) AND e))",
            "NOT (a > b)",
            "(a > b) AND NOT ((c > d) AND (e > f))",
        ];
        let expected_result = [
            "",
            "",
            "'AND' operator cannot be a leaf in 'a > b AND c'",
            "",
            "'AND' operator cannot be a leaf in 'c > d AND e'",
            "'=' operator cannot be a parent in 'a > b = c > d'",
            "'=' expression leaf mismatch error in 'a > b = c'",
            "'=' expression leaf mismatch error in 'a = -3'",
            "'=' expression leaf mismatch error in 'a = +3'",
            "",
            "'=' expression leaf mismatch error in '-a = -3'",
            "'=' expression leaf mismatch error in '-a = +3'",
            "'=' expression leaf mismatch error in '+a = 3'",
            "'=' operator cannot be a parent in '+a = +3'",
            "'=' operator cannot be a parent in '+a = -3'",
            "",
            "'=' expression leaf mismatch error in '-a = -b'",
            "'=' expression leaf mismatch error in '-a = +b'",
            "'=' expression leaf mismatch error in '+a = b'",
            "'=' operator cannot be a parent in '+a = -b'",
            "'=' operator cannot be a parent in '+a = +b'",
            "",
            "",
            "'>' expression leaf mismatch error in 'a > f > g'",
            "'OR' operator cannot be a leaf in 'h OR a > f > g AND c > d AND e'",
            "'NOT' expression pre-processing error in 'NOT a > b'",
            "'NOT' expression pre-processing error in 'NOT c > d AND e > f'",
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
                let res = match validate_where_expr_tree(&where_expr) {
                    Ok(_) => "".to_string(),
                    Err(err) => err.to_string(),
                };
                assert_eq!(res, *e);
            });
    }
}
