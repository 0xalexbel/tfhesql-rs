use std::ops::ControlFlow;
use arrow_schema::Schema;
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator, Value, VisitMut, VisitorMut};
use crate::error::FheSqlError;
use super::data_sig::DataSig;

pub(super) struct RangeOptimizer<'a> {
    schema_ref: &'a Schema,
    ranges: Vec<DataSig>,
}

impl<'a> RangeOptimizer<'a> {
    pub fn new(schema_ref: &'a Schema) -> Self {
        RangeOptimizer {
            schema_ref,
            ranges: vec![],
        }
    }

    pub fn optimize(&mut self, the_expr: &mut Box<Expr>) -> Result<(), FheSqlError> {
        let flow = the_expr.as_mut().visit(self);
        match flow {
            ControlFlow::Continue(_) => Ok(()),
            ControlFlow::Break(err) => Err(err),
        }
    }
}

impl<'a> VisitorMut for RangeOptimizer<'a> {
    type Break = FheSqlError;

    fn post_visit_expr(
        &mut self,
        _expr: &mut sqlparser::ast::Expr,
    ) -> std::ops::ControlFlow<Self::Break> {
        let mut check_for_op_replace = false;
        let mut replace_expr: Option<Box<Expr>> = None;
        let _expr_clone = _expr.clone();
        match _expr {
            Expr::Identifier(ident) => {
                let sig = match DataSig::try_from_ident(ident, self.schema_ref) {
                    Ok(sig) => sig,
                    Err(err) => return ControlFlow::Break(err),
                };
                self.ranges.push(sig);
                check_for_op_replace = true;
            }
            Expr::Value(value) => {
                let sig = match DataSig::try_from_value(value) {
                    Ok(sig) => sig,
                    Err(err) => return ControlFlow::Break(err),
                };
                self.ranges.push(sig);
                check_for_op_replace = true;
            }
            Expr::InList { .. } => todo!(),
            Expr::Between { .. } => todo!(),
            Expr::BinaryOp { left, op, right } => {
                let right_sig = self.ranges.pop().unwrap();
                let left_sig = self.ranges.pop().unwrap();

                match op {
                    BinaryOperator::Xor
                    | BinaryOperator::Gt
                    | BinaryOperator::Lt
                    | BinaryOperator::GtEq
                    | BinaryOperator::LtEq
                    | BinaryOperator::Eq
                    | BinaryOperator::NotEq => {
                        let sig = left_sig.binary_op(op, &right_sig);
                        self.ranges.push(sig);
                        check_for_op_replace = true;
                    }
                    BinaryOperator::And => {
                        let sig = left_sig.binary_op(op, &right_sig);
                        if !sig.is_value() {
                            if left_sig.is_true() {
                                replace_expr = Some(right.clone());
                            } else if right_sig.is_true() {
                                replace_expr = Some(left.clone());
                            }
                        } else {
                            check_for_op_replace = true;
                        }
                        self.ranges.push(sig);
                    }
                    BinaryOperator::Or => {
                        let sig = left_sig.binary_op(op, &right_sig);
                        if !sig.is_value() {
                            if left_sig.is_false() {
                                replace_expr = Some(right.clone());
                            } else if right_sig.is_false() {
                                replace_expr = Some(left.clone());
                            }
                        } else {
                            check_for_op_replace = true;
                        }
                        self.ranges.push(sig);
                    }
                    _ => return ControlFlow::Break(FheSqlError::unsupported_expr(_expr)),
                }
            }
            Expr::UnaryOp { op, .. } => {
                let left_sig = self.ranges.pop().unwrap();
                match op {
                    UnaryOperator::Plus | UnaryOperator::Minus | UnaryOperator::Not => {
                        let sig = left_sig.unary_op(op);
                        self.ranges.push(sig);
                        check_for_op_replace = true;
                    }
                    _ => return ControlFlow::Break(FheSqlError::unsupported_expr(_expr)),
                }
            }
            _ => return ControlFlow::Break(FheSqlError::unsupported_expr(_expr)),
        }
        if check_for_op_replace {
            assert!(replace_expr.is_none());
            let last_sig = self.ranges.last().unwrap();
            if last_sig.is_value() {
                match Value::try_from(self.ranges.last().unwrap()) {
                    Ok(value) => {
                        *_expr = Expr::Value(value);
                    }
                    Err(err) => return ControlFlow::Break(err),
                }
            }
        } else if replace_expr.is_some() {
            assert!(!check_for_op_replace);
            *_expr = *replace_expr.unwrap();
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod test {
    use crate::sql_ast::*;
    use crate::sql_ast::to_parenthesized_string::*;

    use arrow_schema::Field;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

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
    fn test() {
        let schema = get_schema();
        let dialect = GenericDialect {}; // or AnsiDialect
        let where_clauses = [
            "((((a_i >= 2) AND (a_i <= 3)) AND (2 <= 3)) OR (((a_i >= 3) AND (a_i <= 2)) AND (2 >= 3)))",
            "((((a_u >= (-2)) AND (a_u <= 3)) AND ((-2) <= 3)) OR (((a_u >= 3) AND (a_u <= (-2))) AND ((-2) >= 3)))",
            "((((a_b >= (-2)) AND (a_b <= 3)) AND ((-2) <= 3)) OR (((a_b >= 3) AND (a_b <= (-2))) AND ((-2) >= 3)))",
            "((((a_b >= 4) AND (a_b <= 3)) AND (4 <= 3)) OR (((a_b >= 3) AND (a_b <= 4)) AND (4 >= 3)))",
            "(a_s >= -3)",
            "((((a_s >= 0) AND (a_s <= -3)) AND (0 <= -3)) OR (((a_s >= -3) AND (a_s <= 0)) AND (0 >= -3)))",
            "d_i >= 52"
        ];
        let expected_result = [
            "((a_i >= 2) AND (a_i <= 3))", 
            "(a_u <= 3)",
            "true",
            "false",
            "(a_s >= -3)",
            "((a_s >= -3) AND (a_s <= 0))",
            "(d_i >= 52)"
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
                let mut optimizer = RangeOptimizer::new(&schema);
                optimizer.optimize(&mut where_expr).unwrap();
                assert_eq!(where_expr.to_parenthesized_string(), *e);
            });
    }

    #[test]
    fn test_one() {
        // "SELECT ProductID FROM table3 WHERE Type >= 52"
        //let w = "((((a_s >= 0) AND (a_s <= -3)) AND (0 <= -3)) OR (((a_s >= -3) AND (a_s <= 0)) AND (0 >= -3)))";
        let w = "d_i >= 52";
        let _e = "(d_i >= 52)";

        let dialect = GenericDialect {}; // or AnsiDialect
        let schema = get_schema();
        let sql = format!("SELECT * FROM t WHERE {}", w);
        let statements = Parser::parse_sql(&dialect, &sql).unwrap();
        assert!(!statements.is_empty());
        let mut where_expr = statements[0].clone_where().unwrap();
        where_expr.as_mut().remove_nested_in_place().unwrap();
        let mut optimizer = RangeOptimizer::new(&schema);
        optimizer.optimize(&mut where_expr).unwrap();
        println!("{}", where_expr.to_parenthesized_string());
}
}
