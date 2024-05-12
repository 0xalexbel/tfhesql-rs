use arrow_schema::Schema;
use sqlparser::ast::{BinaryOperator, Expr, SetExpr, Statement, UnaryOperator};

use crate::error::FheSqlError;

use self::{
    data_ident::DataIdent, data_type::DataType, data_value::DataValue,
    num_op_rewriter::rewrite_where_expr_in_place, range_optimizer::RangeOptimizer,
    where_validator::validate_where_expr_tree,
};

pub mod and_or_ast;
pub mod bitop_mask;
mod column_ident;
mod data_ident;
mod data_sig;
mod data_type;
mod data_value;
mod helpers;
mod num_op_rewriter;
pub mod parser;
mod range_optimizer;
mod tests;
mod to_parenthesized_string;
mod where_validator;

mod comparator_mask;
pub use comparator_mask::ComparatorMask;

pub trait SqlExprRemoveNestedInPlace {
    fn remove_nested_in_place(&mut self) -> Result<(), FheSqlError>;
}

pub trait SqlExprGetChildren {
    fn get_children(&self) -> Result<Vec<&Expr>, FheSqlError>;
    fn get_children_mut(&mut self) -> Result<Vec<&mut Expr>, FheSqlError>;
}

impl SqlExprGetChildren for Expr {
    fn get_children(&self) -> Result<Vec<&Expr>, FheSqlError> {
        match self {
            Expr::Identifier(_) => Ok(vec![]),
            Expr::InList { expr, list, .. } => {
                let mut v = vec![expr.as_ref()];
                list.iter().for_each(|item| v.push(item));
                Ok(v)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Ok(vec![expr.as_ref(), low.as_ref(), high.as_ref()])
            }
            Expr::BinaryOp { left, right, .. } => {
                Ok(vec![left.as_ref(), right.as_ref()])
            }
            Expr::UnaryOp { expr, .. } => {
                Ok(vec![expr.as_ref()])
            }
            Expr::Nested(expr) => {
                Ok(vec![expr.as_ref()])
            }
            Expr::Value(_) => Ok(vec![]),
            _ => Err(FheSqlError::unsupported_expr(self)),
        }
    }

    fn get_children_mut(&mut self) -> Result<Vec<&mut Expr>, FheSqlError> {
        match self {
            Expr::Identifier(_) => Ok(vec![]),
            Expr::InList { expr, list, .. } => {
                let mut v = vec![expr.as_mut()];
                list.iter_mut().for_each(|item| v.push(item));
                Ok(v)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Ok(vec![expr.as_mut(), low.as_mut(), high.as_mut()])
            }
            Expr::BinaryOp { left, right, .. } => {
                Ok(vec![left.as_mut(), right.as_mut()])
            }
            Expr::UnaryOp { expr, .. } => {
                Ok(vec![expr.as_mut()])
            }
            Expr::Nested(expr) => {
                Ok(vec![expr.as_mut()])
            }
            Expr::Value(_) => Ok(vec![]),
            _ => Err(FheSqlError::unsupported_expr(self)),
        }
    }
}

impl SqlExprRemoveNestedInPlace for Expr {
    fn remove_nested_in_place(&mut self) -> Result<(), FheSqlError> {
        match self {
            Expr::Nested(expr) => {
                expr.as_mut().remove_nested_in_place()?;
                *self = (**expr).clone();
            }
            _ => {
                self.get_children_mut()?
                    .iter_mut()
                    .try_for_each(|item| (*item).remove_nested_in_place())?;
            }
        }
        Ok(())
    }
}

pub(super) trait SqlExprDataType {
    fn data_type(&self, schema: &Schema) -> Result<DataType, FheSqlError>;
}

impl SqlExprDataType for Expr {
    #[inline]
    fn data_type(&self, schema: &Schema) -> Result<DataType, FheSqlError> {
        match self {
            Expr::UnaryOp { op, .. } => match op {
                UnaryOperator::Plus => Ok(DataType::AnyInt),
                UnaryOperator::Minus => Ok(DataType::AnyInt),
                UnaryOperator::Not => Ok(DataType::Boolean),
                _ => Err(FheSqlError::unsupported_unary_op(op)),
            },
            Expr::BinaryOp { op, .. } => match op {
                BinaryOperator::Gt
                | BinaryOperator::Lt
                | BinaryOperator::GtEq
                | BinaryOperator::LtEq
                | BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Xor => Ok(DataType::Boolean),
                _ => Err(FheSqlError::unsupported_binary_op(op)),
            },
            Expr::InList { .. } => Ok(DataType::Boolean),
            Expr::Between { .. } => Ok(DataType::Boolean),
            Expr::Value(value) => {
                let dv = DataValue::try_from(value)?;
                Ok(dv.data_type())
            }
            Expr::Identifier(ident) => {
                let dv = DataIdent::try_from_ident(ident, schema)?;
                Ok(dv.data_type())
            }
            _ => Err(FheSqlError::unsupported_expr(self)),
        }
    }
}

pub trait CloneWhereStatement {
    fn clone_where(&self) -> Option<Box<Expr>>;
}

pub trait CompileWhereStatement
where
    Self: CloneWhereStatement,
{
    fn compile_where(&self, schema: &Schema) -> Result<Option<Box<Expr>>, FheSqlError> {
        let mut where_expr = match self.clone_where() {
            Some(w) => w,
            None => return Ok(None),
        };

        // First step : remove parentheses
        where_expr.as_mut().remove_nested_in_place()?;

        // Second step : rewrite where statement (remove minus, plus etc.)
        rewrite_where_expr_in_place(&mut where_expr, schema)?;

        // Third step : optimize tree
        let mut optimizer = RangeOptimizer::new(schema);
        optimizer.optimize(&mut where_expr)?;

        // Fourth step : check
        match validate_where_expr_tree(&where_expr) {
            Ok(_) => Ok(Some(where_expr)),
            Err(err) => Err(FheSqlError::InternalError(err.to_string())),
        }
    }
}

impl<T> CompileWhereStatement for T where T: CloneWhereStatement {}

impl CloneWhereStatement for Statement {
    fn clone_where(&self) -> Option<Box<Expr>> {
        match self {
            Statement::Query(query) => match query.as_ref().body.as_ref() {
                SetExpr::Select(s) => s.selection.as_ref().map(|selection| Box::new(selection.clone())),
                _ => None,
            },
            _ => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
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
            "a > b AND c > d",
            "a > b AND c > d",
            "a > b AND c > d OR a IN ('a', 'b') AND -1",
            "a > b AND c > d BETWEEN 10 AND x OR y",
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
                assert_eq!(where_expr.to_string(), *e);
            });
    }
}
