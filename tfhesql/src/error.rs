use std::{error::Error, fmt::{Display, Formatter}};
use sqlparser::ast::{BinaryOperator, Expr, UnaryOperator};

#[derive(Debug, PartialEq)]
pub enum FheSqlError {
    ArrowError(String),
    CsvError(String),
    IoError(String),
    SyntaxError(String),
    UnsupportedSqlQuery(String),
    UnsupportedSqlStatement(String),
    UnknownColumnName(String),
    UnsupportedExpr(String),
    InternalError(String),
    DecryptError(String),
    InvalidQueryError(String),
}

impl Error for FheSqlError {}

// An expression of non-boolean type specified in a context where a condition is expected, near ';'.
impl FheSqlError {
    pub(crate) fn unsupported_expr(expr: &Expr) -> Self {
        FheSqlError::UnsupportedExpr(expr.to_string())
    }
    pub(crate) fn unsupported_arrow_data_type(data_type: &arrow_schema::DataType) -> Self {
        FheSqlError::SyntaxError(format!("Unsupported data type '{}'",data_type))
    }
    pub(crate) fn syntax_error(msg: &str) -> Self {
        FheSqlError::SyntaxError(msg.to_string())
    }
    pub(crate) fn unsupported_binary_op(op: &BinaryOperator) -> Self {
        FheSqlError::SyntaxError(format!("Unsupported binary operator '{}'",op))
    }
    pub(crate) fn unsupported_unary_op(op: &UnaryOperator) -> Self {
        FheSqlError::SyntaxError(format!("Unsupported operator '{}'",op))
    }
    pub(crate) fn parse_int_error(num: &str) -> Self {
        FheSqlError::SyntaxError(format!("Unable to parse integer argument '{}'",num))
    }
    pub(crate) fn unsupported_value(value: &str) -> Self {
        FheSqlError::SyntaxError(format!("Unsupported value format '{}'",value))
    }
}

impl From<std::io::Error> for FheSqlError {
    fn from(error: std::io::Error) -> Self {
        FheSqlError::IoError(error.to_string())
    }
}

impl Display for FheSqlError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FheSqlError::ArrowError(desc) => write!(f, "Arrow error: {desc}"),
            FheSqlError::CsvError(desc) => write!(f, "Csv error: {desc}"),
            FheSqlError::IoError(desc) => write!(f, "Io error: {desc}"),
            FheSqlError::SyntaxError(desc) => write!(f, "Syntax error: {desc}"),
            FheSqlError::UnsupportedSqlQuery(desc) => write!(f, "Unsupported SQL query: {desc}"),
            FheSqlError::UnsupportedExpr(expr) => write!(f, "Unsupported SQL expression: '{expr}'"),
            FheSqlError::UnsupportedSqlStatement(desc) => {
                write!(f, "Unsupported SQL statement: {desc}")
            }
            FheSqlError::UnknownColumnName(name) => write!(f, "Unknown table column name: {name}"),
            FheSqlError::InternalError(desc) => write!(f, "Internal error: {desc}"),
            FheSqlError::DecryptError(desc) => write!(f, "Decrypt error: {desc}"),
            FheSqlError::InvalidQueryError(desc) => write!(f, "Invalid query error: {desc}"),
        }
    }
}
