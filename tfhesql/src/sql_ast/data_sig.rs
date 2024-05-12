use arrow_schema::Schema;
use sqlparser::ast::{BinaryOperator, Expr, Ident, UnaryOperator, Value};
use crate::error::FheSqlError;
use super::{data_ident::DataIdent, data_type::DataType, data_value::DataValue};

////////////////////////////////////////////////////////////////////////////////
// DataSig
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq)]
pub(super) enum DataSig {
    Value(DataValue),
    Ident(DataIdent),
}

////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for DataSig {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            DataSig::Value(v) => f.write_str(format!("[Value={}]", v).as_str()),
            DataSig::Ident(i) => f.write_str(format!("[Ident={}]", i.data_type()).as_str()),
        }
    }
}

impl TryFrom<&DataSig> for Value {
    type Error = FheSqlError;

    fn try_from(sig: &DataSig) -> Result<Self, FheSqlError> {
        match &sig {
            DataSig::Value(value) => Ok(Value::from(value)),
            DataSig::Ident { .. } => Err(FheSqlError::InternalError(
                "Unable to create Value from Ident DataSig".to_string(),
            )),
        }
    }
}

impl DataSig {
    pub fn try_from_ident(ident: &Ident, schema: &Schema) -> Result<Self, FheSqlError> {
        Ok(DataSig::Ident(DataIdent::try_from_ident(ident, schema)?))
    }
    pub fn try_from_value(value: &Value) -> Result<Self, FheSqlError> {
        Ok(DataSig::Value(DataValue::try_from(value)?))
    }
    pub fn try_from_expr(the_expr: &Expr, schema: &Schema) -> Result<Self, FheSqlError> {
        match the_expr {
            Expr::Identifier(ident) => DataSig::try_from_ident(ident, schema),
            Expr::UnaryOp { .. } => Ok(DataSig::Ident(DataIdent::try_from_expr(the_expr, schema)?)),
            Expr::Value(value) => DataSig::try_from_value(value),
            _ => Err(FheSqlError::unsupported_expr(the_expr)),
        }
    }
}

impl DataSig {
    #[inline]
    pub fn is_value(&self) -> bool {
        matches!(self, DataSig::Value(_))
    }
    #[inline]
    pub fn is_true(&self) -> bool {
        if !self.is_value() {
            return false;
        }
        let v = self.get_value();
        if !v.is_bool() {
            return false;
        }
        v.get_bool()
    }
    #[inline]
    pub fn is_false(&self) -> bool {
        if !self.is_value() {
            return false;
        }
        let v = self.get_value();
        if !v.is_bool() {
            return false;
        }
        !v.get_bool()
    }
    #[inline]
    pub fn get_value(&self) -> &DataValue {
        match self {
            DataSig::Value(value) => value,
            _ => panic!("called `DataSig::get_value()` on a DataSig::Ident argument"),
        }
    }
    #[inline]
    pub fn get_ident(&self) -> &DataIdent {
        match self {
            DataSig::Ident(ident) => ident,
            _ => panic!("called `DataSig::get_ident()` on a DataSig::Value argument"),
        }
    }

    pub fn not(&self) -> DataSig {
        match self {
            DataSig::Value(value) => DataSig::Value(value.not()),
            DataSig::Ident(ident) => DataSig::Ident(ident.not()),
        }
    }

    pub fn minus(&self) -> DataSig {
        match self {
            DataSig::Value(value) => DataSig::Value(value.minus()),
            DataSig::Ident(ident) => DataSig::Ident(ident.minus()),
        }
    }

    pub fn plus(&self) -> DataSig {
        match self {
            DataSig::Value(value) => DataSig::Value(value.plus()),
            DataSig::Ident(ident) => DataSig::Ident(ident.plus()),
        }
    }

    pub fn binary_op(&self, op: &BinaryOperator, rhs: &DataSig) -> DataSig {
        match op {
            BinaryOperator::Gt => self.gt_than(rhs),
            BinaryOperator::Lt => self.lt_than(rhs),
            BinaryOperator::GtEq => self.gteq_than(rhs),
            BinaryOperator::LtEq => self.lteq_than(rhs),
            BinaryOperator::Eq => self.eq_to(rhs),
            BinaryOperator::NotEq => self.noteq_to(rhs),
            BinaryOperator::And => self.and(rhs),
            BinaryOperator::Or => self.or(rhs),
            BinaryOperator::Xor => todo!(),
            _ => panic!("Called binary_op with an unsupported operator"),
        }
    }

    pub fn unary_op(&self, op: &UnaryOperator) -> DataSig {
        match op {
            UnaryOperator::Plus => self.plus(),
            UnaryOperator::Minus => self.minus(),
            UnaryOperator::Not => self.not(),
            _ => panic!("Called unary_op() with an unsupported operator"),
        }
    }

    pub fn and(&self, rhs: &DataSig) -> DataSig {
        match (self, rhs) {
            (DataSig::Value(l), DataSig::Value(r)) => DataSig::Value(l.and(r)),
            (DataSig::Value(l), DataSig::Ident(r)) => DataSig::and_ident_value(r, l),
            (DataSig::Ident(l), DataSig::Value(r)) => DataSig::and_ident_value(l, r),
            (DataSig::Ident(l), DataSig::Ident(r)) => DataSig::and_ident_ident(l, r),
        }
    }

    pub fn or(&self, rhs: &DataSig) -> DataSig {
        match (self, rhs) {
            (DataSig::Value(l), DataSig::Value(r)) => DataSig::Value(l.or(r)),
            (DataSig::Value(l), DataSig::Ident(r)) => DataSig::or_ident_value(r, l),
            (DataSig::Ident(l), DataSig::Value(r)) => DataSig::or_ident_value(l, r),
            (DataSig::Ident(l), DataSig::Ident(r)) => DataSig::or_ident_ident(l, r),
        }
    }

    pub fn eq_to(&self, rhs: &DataSig) -> DataSig {
        match (self, rhs) {
            (DataSig::Value(l), DataSig::Value(r)) => DataSig::Value(l.eq_to(r)),
            (DataSig::Value(l), DataSig::Ident(r)) => DataSig::eq_ident_value(r, l),
            (DataSig::Ident(l), DataSig::Value(r)) => DataSig::eq_ident_value(l, r),
            (DataSig::Ident(l), DataSig::Ident(r)) => DataSig::eq_ident_ident(l, r),
        }
    }

    pub fn noteq_to(&self, rhs: &DataSig) -> DataSig {
        match (self, rhs) {
            (DataSig::Value(l), DataSig::Value(r)) => DataSig::Value(l.noteq_to(r)),
            (DataSig::Value(l), DataSig::Ident(r)) => DataSig::noteq_ident_value(r, l),
            (DataSig::Ident(l), DataSig::Value(r)) => DataSig::noteq_ident_value(l, r),
            (DataSig::Ident(l), DataSig::Ident(r)) => DataSig::noteq_ident_ident(l, r),
        }
    }

    pub fn lt_than(&self, rhs: &DataSig) -> DataSig {
        match (self, rhs) {
            (DataSig::Value(l), DataSig::Value(r)) => DataSig::Value(l.lt_than(r)),
            (DataSig::Value(l), DataSig::Ident(r)) => DataSig::lt_ident_value(r, l),
            (DataSig::Ident(l), DataSig::Value(r)) => DataSig::lt_ident_value(l, r),
            (DataSig::Ident(l), DataSig::Ident(r)) => DataSig::lt_ident_ident(l, r),
        }
    }

    pub fn lteq_than(&self, rhs: &DataSig) -> DataSig {
        match (self, rhs) {
            (DataSig::Value(l), DataSig::Value(r)) => DataSig::Value(l.lteq_than(r)),
            (DataSig::Value(l), DataSig::Ident(r)) => DataSig::lteq_ident_value(r, l),
            (DataSig::Ident(l), DataSig::Value(r)) => DataSig::lteq_ident_value(l, r),
            (DataSig::Ident(l), DataSig::Ident(r)) => DataSig::lteq_ident_ident(l, r),
        }
    }

    pub fn gt_than(&self, rhs: &DataSig) -> DataSig {
        match (self, rhs) {
            (DataSig::Value(l), DataSig::Value(r)) => DataSig::Value(l.gt_than(r)),
            (DataSig::Value(l), DataSig::Ident(r)) => DataSig::gt_ident_value(r, l),
            (DataSig::Ident(l), DataSig::Value(r)) => DataSig::gt_ident_value(l, r),
            (DataSig::Ident(l), DataSig::Ident(r)) => DataSig::gt_ident_ident(l, r),
        }
    }

    pub fn gteq_than(&self, rhs: &DataSig) -> DataSig {
        match (self, rhs) {
            (DataSig::Value(l), DataSig::Value(r)) => DataSig::Value(l.gteq_than(r)),
            (DataSig::Value(l), DataSig::Ident(r)) => DataSig::gteq_ident_value(r, l),
            (DataSig::Ident(l), DataSig::Value(r)) => DataSig::gteq_ident_value(l, r),
            (DataSig::Ident(l), DataSig::Ident(r)) => DataSig::gteq_ident_ident(l, r),
        }
    }

    /// column_1 AND column_2
    fn and_ident_ident(_i_lhs: &DataIdent, _i_rhs: &DataIdent) -> DataSig {
        DataSig::Ident(DataIdent::from(DataType::Boolean))
    }

    /// column_1 AND 123
    fn and_ident_value(i_lhs: &DataIdent, v_rhs: &DataValue) -> DataSig {
        let b_lhs = i_lhs.cast_to_bool();
        let b_rhs = v_rhs.cast_to_bool();

        let bool_rhs = b_rhs.get_bool();
        if !bool_rhs {
            DataSig::Value(DataValue::Bool(false))
        } else {
            DataSig::Ident(b_lhs)
        }
    }

    /// column_1 OR 123
    fn or_ident_value(i_lhs: &DataIdent, v_rhs: &DataValue) -> DataSig {
        let b_lhs = i_lhs.cast_to_bool();
        let b_rhs = v_rhs.cast_to_bool();

        let bool_rhs = b_rhs.get_bool();
        if bool_rhs {
            DataSig::Value(DataValue::Bool(true))
        } else {
            DataSig::Ident(b_lhs)
        }
    }

    /// column_1 OR column_2
    fn or_ident_ident(_i_lhs: &DataIdent, _i_rhs: &DataIdent) -> DataSig {
        DataSig::Ident(DataIdent::from(DataType::Boolean))
    }

    /// column_1 == column_2
    fn eq_ident_ident(_i_lhs: &DataIdent, _i_rhs: &DataIdent) -> DataSig {
        DataSig::Ident(DataIdent::from(DataType::Boolean))
    }

    /// column_1 == 1234
    fn eq_ident_value(i_lhs: &DataIdent, v_rhs: &DataValue) -> DataSig {
        let n_lhs = i_lhs.cast_to_num();
        let n_rhs = v_rhs.cast_to_num();

        let range_lhs = n_lhs.range();
        let num_rhs = n_rhs.get_num();

        if range_lhs.contains(&num_rhs) {
            DataSig::Ident(DataIdent::from(DataType::Boolean))
        } else {
            DataSig::Value(DataValue::Bool(false))
        }
    }

    /// column_1 != column_2
    fn noteq_ident_ident(_i_lhs: &DataIdent, _i_rhs: &DataIdent) -> DataSig {
        DataSig::Ident(DataIdent::from(DataType::Boolean))
    }

    /// column_1 != 1234
    fn noteq_ident_value(i_lhs: &DataIdent, v_rhs: &DataValue) -> DataSig {
        let n_lhs = i_lhs.cast_to_num();
        let n_rhs = v_rhs.cast_to_num();

        let range_lhs = n_lhs.range();
        let num_rhs = n_rhs.get_num();

        if range_lhs.contains(&num_rhs) {
            DataSig::Ident(DataIdent::from(DataType::Boolean))
        } else {
            DataSig::Value(DataValue::Bool(true))
        }
    }

    /// column_1 < column_2
    fn lt_ident_ident(_i_lhs: &DataIdent, _i_rhs: &DataIdent) -> DataSig {
        DataSig::Ident(DataIdent::from(DataType::Boolean))
    }

    /// column_1 < 1234
    fn lt_ident_value(i_lhs: &DataIdent, v_rhs: &DataValue) -> DataSig {
        let n_lhs = i_lhs.cast_to_num();
        let n_rhs = v_rhs.cast_to_num();

        let range_lhs = n_lhs.range();
        let num_rhs = n_rhs.get_num();

        if range_lhs.max() < num_rhs {
            return DataSig::Value(DataValue::Bool(true));
        }
        if range_lhs.min() >= num_rhs {
            return DataSig::Value(DataValue::Bool(false));
        }

        DataSig::Ident(DataIdent::from(DataType::Boolean))
    }

    /// column_1 <= column_2
    fn lteq_ident_ident(_i_lhs: &DataIdent, _i_rhs: &DataIdent) -> DataSig {
        DataSig::Ident(DataIdent::from(DataType::Boolean))
    }

    /// column_1 <= 1234
    fn lteq_ident_value(i_lhs: &DataIdent, v_rhs: &DataValue) -> DataSig {
        let n_lhs = i_lhs.cast_to_num();
        let n_rhs = v_rhs.cast_to_num();

        let range_lhs = n_lhs.range();
        let num_rhs = n_rhs.get_num();

        if range_lhs.max() <= num_rhs {
            return DataSig::Value(DataValue::Bool(true));
        }
        if range_lhs.min() > num_rhs {
            return DataSig::Value(DataValue::Bool(false));
        }

        DataSig::Ident(DataIdent::from(DataType::Boolean))
    }

    /// column_1 > column_2
    fn gt_ident_ident(_i_lhs: &DataIdent, _i_rhs: &DataIdent) -> DataSig {
        DataSig::Ident(DataIdent::from(DataType::Boolean))
    }

    /// column_1 > 1234
    fn gt_ident_value(i_lhs: &DataIdent, v_rhs: &DataValue) -> DataSig {
        let n_lhs = i_lhs.cast_to_num();
        let n_rhs = v_rhs.cast_to_num();

        let range_lhs = n_lhs.range();
        let num_rhs = n_rhs.get_num();

        if range_lhs.min() > num_rhs {
            return DataSig::Value(DataValue::Bool(true));
        }
        if range_lhs.max() <= num_rhs {
            return DataSig::Value(DataValue::Bool(false));
        }

        DataSig::Ident(DataIdent::from(DataType::Boolean))
    }

    /// column_1 >= column_2
    fn gteq_ident_ident(_i_lhs: &DataIdent, _i_rhs: &DataIdent) -> DataSig {
        DataSig::Ident(DataIdent::from(DataType::Boolean))
    }

    /// column_1 >= 1234
    fn gteq_ident_value(i_lhs: &DataIdent, v_rhs: &DataValue) -> DataSig {
        let n_lhs = i_lhs.cast_to_num();
        let n_rhs = v_rhs.cast_to_num();

        let range_lhs = n_lhs.range();
        let num_rhs = n_rhs.get_num();

        if range_lhs.min() >= num_rhs {
            return DataSig::Value(DataValue::Bool(true));
        }
        if range_lhs.max() < num_rhs {
            return DataSig::Value(DataValue::Bool(false));
        }

        DataSig::Ident(DataIdent::from(DataType::Boolean))
    }
}
