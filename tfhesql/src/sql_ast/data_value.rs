use std::ops::Neg;
use sqlparser::ast::Value;
use crate::{
    error::FheSqlError,
    uint::{
        interval::ClosedInterval,
        signed_u64::{SignedU64, ZERO_SIGNED_U64},
    },
};

use super::data_type::DataType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataValue {
    Bool(bool),
    Num(SignedU64),
    Ascii(String),
}

impl std::fmt::Display for DataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataValue::Bool(b) => f.write_str(format!("{}", b).as_str()),
            DataValue::Num(n) => f.write_str(format!("{}", n).as_str()),
            DataValue::Ascii(s) => f.write_str(s.to_string().as_str()),
        }
    }
}

impl TryFrom<&Value> for DataValue {
    type Error = FheSqlError;

    fn try_from(value: &Value) -> Result<Self, FheSqlError> {
        match value {
            Value::Number(val, _) => Ok(DataValue::Num(SignedU64::try_from(val.as_str())?)),
            Value::Boolean(val) => Ok(DataValue::Bool(*val)),
            Value::SingleQuotedString(s)
            | Value::DoubleQuotedString(s)
            | Value::UnQuotedString(s) => Ok(DataValue::Ascii(s.clone())),
            _ => Err(FheSqlError::unsupported_value(value.to_string().as_str())),
        }
    }
}

impl From<&DataValue> for Value {
    fn from(value: &DataValue) -> Self {
        match value {
            DataValue::Bool(b) => Value::Boolean(*b),
            DataValue::Num(n) => Value::Number(format!("{}", n), false),
            DataValue::Ascii(s) => Value::SingleQuotedString(s.to_string()),
        }
    }
}

impl DataValue {
    #[inline]
    pub fn is_bool(&self) -> bool {
        matches!(self, DataValue::Bool(_))
    }
    #[inline]
    pub fn is_num(&self) -> bool {
        matches!(self, DataValue::Num(_))
    }
    #[inline]
    pub fn is_ascii(&self) -> bool {
        matches!(self, DataValue::Ascii(_))
    }
    #[inline]
    pub fn data_type(&self) -> DataType {
        match self {
            DataValue::Bool(_) => DataType::Boolean,
            DataValue::Num(_) => DataType::AnyInt,
            DataValue::Ascii(_) => DataType::ASCII32,
        }
    }
    #[inline]
    pub fn get_bool(&self) -> bool {
        match self {
            DataValue::Bool(val) => *val,
            _ => panic!("called `DataValue::get_bool()` on a non Bool value"),
        }
    }
    #[inline]
    pub fn get_ascii_ref(&self) -> &String {
        match self {
            DataValue::Ascii(val) => val,
            _ => panic!("called `DataValue::get_ascii_ref()` on a non ASCII value"),
        }
    }
    #[inline]
    pub fn get_ascii(&self) -> String {
        self.get_ascii_ref().clone()
    }
    #[inline]
    pub fn get_num(&self) -> SignedU64 {
        match self {
            DataValue::Num(num) => *num,
            _ => panic!("called `DataValue::get_num()` on a non Num value"),
        }
    }

    #[inline]
    pub fn range(&self) -> ClosedInterval<SignedU64> {
        let num = self.cast_to_num().get_num();
        ClosedInterval::<SignedU64>::new(num, num)
    }

    pub fn cast_to_num(&self) -> DataValue {
        match self {
            DataValue::Bool(b) => DataValue::Num(SignedU64::from(b)),
            DataValue::Num(_) => self.clone(),
            DataValue::Ascii(s) => DataValue::Num(SignedU64::from_str(s.as_str())),
        }
    }

    pub fn cast_to_bool(&self) -> DataValue {
        match self {
            DataValue::Bool(_) => self.clone(),
            DataValue::Num(num) => DataValue::Bool(*num != ZERO_SIGNED_U64),
            DataValue::Ascii(_) => self.cast_to_num().cast_to_bool(),
        }
    }

    /*

    NOT
    ===
    op(lhs: AnyType) = op(ToBool(lhs)) -> BooType

    */
    pub fn not(&self) -> DataValue {
        let b = self.cast_to_bool();
        DataValue::Bool(!b.get_bool())
    }

    /*

    AND, OR
    =======
    op(lhs: AnyType, rhs: AnyType) = op(ToBool(lhs), ToBool(rhs)) -> BooType

     */
    pub fn and(&self, rhs: &DataValue) -> DataValue {
        let b_lhs = self.cast_to_bool();
        let b_rhs = rhs.cast_to_bool();
        DataValue::Bool(b_lhs.get_bool() && b_rhs.get_bool())
    }
    pub fn or(&self, rhs: &DataValue) -> DataValue {
        let b_lhs = self.cast_to_bool();
        let b_rhs = rhs.cast_to_bool();
        DataValue::Bool(b_lhs.get_bool() || b_rhs.get_bool())
    }

    /*

    Unary: +,-
    ==========
    (lhs: AnyType) -> ToNum(lhs)

    */

    pub fn plus(&self) -> DataValue {
        self.cast_to_num()
    }

    pub fn minus(&self) -> DataValue {
        let num_value = self.cast_to_num();
        DataValue::Num(num_value.get_num().neg())
    }

    /*

    =, <, >, <=, >=, <>
    ===================
    op(lhs: AnyType, rhs: !AsciiType) = op(ToNum(lhs), ToNum(rhs)) -> BoolType
    op(lhs: AsciiType, rhs: AsciiType) = op(lhs, rhs) -> BoolType

    */

    pub fn eq_to(&self, rhs: &DataValue) -> DataValue {
        match (self, rhs) {
            (DataValue::Ascii(lhs), DataValue::Ascii(rhs)) => DataValue::Bool(lhs == rhs),
            (lhs, rhs) => {
                let n_lhs = lhs.cast_to_num();
                let n_rhs = rhs.cast_to_num();
                DataValue::Bool(n_lhs.get_num() == n_rhs.get_num())
            }
        }
    }

    pub fn noteq_to(&self, rhs: &DataValue) -> DataValue {
        self.eq_to(rhs).not()
    }

    pub fn lt_than(&self, rhs: &DataValue) -> DataValue {
        match (self, rhs) {
            (DataValue::Ascii(s_lhs), DataValue::Ascii(s_rhs)) => DataValue::Bool(s_lhs < s_rhs),
            (lhs, rhs) => {
                let num_lhs = lhs.cast_to_num().get_num();
                let num_rhs = rhs.cast_to_num().get_num();
                DataValue::Bool(num_lhs < num_rhs)
            }
        }
    }

    pub fn lteq_than(&self, rhs: &DataValue) -> DataValue {
        self.lt_than(rhs).or(&self.eq_to(rhs))
    }

    pub fn gt_than(&self, rhs: &DataValue) -> DataValue {
        self.lteq_than(rhs).not()
    }

    pub fn gteq_than(&self, rhs: &DataValue) -> DataValue {
        self.lt_than(rhs).not()
    }
}
