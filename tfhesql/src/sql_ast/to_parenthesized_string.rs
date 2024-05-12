use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

use crate::error::FheSqlError;

pub(super) trait ToParenthesizedString {
    fn try_to_parenthesized_string(&self) -> Result<String, FheSqlError>;
    fn to_parenthesized_string(&self) -> String {
        self.try_to_parenthesized_string().unwrap()
    }
}

impl ToParenthesizedString for Expr {
    fn try_to_parenthesized_string(&self) -> Result<String, FheSqlError> {
        match self {
            Expr::Identifier(ident) => Ok(ident.to_string()),
            Expr::Value(value) => Ok(value.to_string()),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr_s = expr.try_to_parenthesized_string()?;
    
                // let mut list_s: Vec<String> = vec![];
                // let n = list.len();
                // for i in 0..n {
                //     let item_s = list[i].try_to_parenthesized_string()?;
                //     list_s.push(item_s);
                // }
                let list_s = list.iter().map(|l| {
                    l.try_to_parenthesized_string()
                }).collect::<Result<Vec<String>, FheSqlError>>()?;

                if *negated {
                    Ok(format!("({} NOT IN ({}))", expr_s, list_s.join(", ")))
                } else {
                    Ok(format!("({} IN ({}))", expr_s, list_s.join(", ")))
                }
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr_s = expr.try_to_parenthesized_string()?;
                let low_s = low.try_to_parenthesized_string()?;
                let high_s = high.try_to_parenthesized_string()?;
                if *negated {
                    Ok(format!("({} NOT BETWEEN {} AND {})", expr_s, low_s, high_s))
                } else {
                    Ok(format!("({} BETWEEN {} AND {})", expr_s, low_s, high_s))
                }
            }
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Gt
                | BinaryOperator::Lt
                | BinaryOperator::GtEq
                | BinaryOperator::LtEq
                | BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Xor => {
                    let left_s = left.try_to_parenthesized_string()?;
                    let right_s = right.try_to_parenthesized_string()?;
                    Ok(format!("({} {} {})", left_s, op, right_s))
                }
                _ => Err(FheSqlError::unsupported_expr(self)),
            },
            Expr::UnaryOp { op, expr } => match op {
                UnaryOperator::Plus | UnaryOperator::Minus => {
                    let expr_s = expr.try_to_parenthesized_string()?;
                    Ok(format!("({}{})", op, expr_s))
                }
                UnaryOperator::Not => {
                    let expr_s = expr.try_to_parenthesized_string()?;
                    Ok(format!("({} {})", op, expr_s))
                }
                _ => Err(FheSqlError::unsupported_expr(self)),
            },
            _ => Err(FheSqlError::unsupported_expr(self)),
        }
    }
}
