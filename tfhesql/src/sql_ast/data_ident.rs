use super::column_ident::ColumnIdent;
use super::data_type::DataType;
use crate::{
    error::FheSqlError,
    uint::{interval::ClosedInterval, signed_u64::SignedU64},
};
use arrow_schema::Schema;
use sqlparser::ast::{Expr, Ident, UnaryOperator};
use std::ops::Neg;

////////////////////////////////////////////////////////////////////////////////
// DataIdent
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DataIdent {
    column: Box<ColumnIdent>,
    cast_to: DataType,
    minus_sign: bool,
}

////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for DataIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(format!("{:?}]", self).as_str())
    }
}

impl From<DataType> for DataIdent {
    fn from(data_type: DataType) -> Self {
        let column = ColumnIdent::from(data_type);
        DataIdent {
            column: Box::new(column),
            cast_to: data_type,
            minus_sign: false,
        }
    }
}

impl From<&ColumnIdent> for DataIdent {
    fn from(column: &ColumnIdent) -> Self {
        DataIdent {
            column: Box::new(column.clone()),
            cast_to: column.data_type(),
            minus_sign: false,
        }
    }
}

impl DataIdent {
    pub fn try_from_ident(ident: &Ident, schema: &Schema) -> Result<Self, FheSqlError> {
        let column = ColumnIdent::try_from_ident(ident, schema)?;
        let data_type = column.data_type();
        Ok(DataIdent {
            column: Box::new(column),
            cast_to: data_type,
            minus_sign: false,
        })
    }

    pub fn try_from_expr(the_expr: &Expr, schema: &Schema) -> Result<Self, FheSqlError> {
        match the_expr {
            Expr::Identifier(ident) => DataIdent::try_from_ident(ident, schema),
            Expr::UnaryOp { op, expr } => {
                if matches!(op, UnaryOperator::Minus) {
                    if let Expr::Identifier(ident) = expr.as_ref() {
                        Ok(DataIdent::try_from_ident(ident, schema)?.minus())
                    } else {
                        Err(FheSqlError::unsupported_expr(the_expr))    
                    }
                } else {
                    Err(FheSqlError::unsupported_expr(the_expr))
                }

                // match op {
                //     UnaryOperator::Minus => {
                //         match expr.as_ref() {
                //             Expr::Identifier(ident) => {
                //                 Ok(DataIdent::try_from_ident(ident, schema)?.minus())
                //             }
                //             _ => Err(FheSqlError::unsupported_expr(the_expr))
                //         }
                //     },
                //     _ => Err(FheSqlError::unsupported_expr(the_expr))
                // }
            }
            _ => Err(FheSqlError::unsupported_expr(the_expr)),
        }
    }

    #[inline]
    pub fn range(&self) -> ClosedInterval<SignedU64> {
        if self.minus_sign {
            self.cast_to.min_max_range().neg()
        } else {
            self.cast_to.min_max_range()
        }
    }

    #[inline]
    pub fn column_index(&self) -> usize {
        let idx = self.column.index();
        if idx < 0 {
            panic!("Unknown column index");
        }
        idx as usize
    }

    #[inline]
    pub fn minus_sign(&self) -> bool {
        self.minus_sign
    }

    #[inline]
    pub fn is_bool(&self) -> bool {
        self.cast_to.is_bool()
    }

    #[inline]
    pub fn is_ascii(&self) -> bool {
        self.cast_to.is_ascii()
    }

    #[inline]
    pub fn cast_to_bool(&self) -> DataIdent {
        let mut di = self.clone();
        di.cast_to = di.cast_to.cast_to_bool();
        di.minus_sign = false;
        di
    }

    #[inline]
    pub fn cast_to_num(&self) -> DataIdent {
        let mut di = self.clone();
        di.cast_to = di.cast_to.cast_to_num();
        di
    }

    #[inline]
    pub fn data_type(&self) -> DataType {
        self.cast_to
    }

    #[inline]
    pub fn minus(&self) -> DataIdent {
        let mut di = self.cast_to_num();
        di.minus_sign = !di.minus_sign;
        di
    }

    #[inline]
    pub fn plus(&self) -> DataIdent {
        self.cast_to_num()
    }

    #[inline]
    pub fn not(&self) -> DataIdent {
        self.cast_to_bool()
    }
}
