use crate::uint::interval::*;
use crate::uint::signed_u64::SignedU64;
use crate::{error::FheSqlError, uint::signed_u64::MinMaxRange};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    Boolean,
    I8,
    I16,
    I32,
    I64,
    U1,
    U8,
    U16,
    U32,
    U64,
    AnyInt,
    ASCII32,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl DataType {
    #[inline]
    pub fn is_integer_or_ascii(&self) -> bool {
        use DataType::*;
        self.is_signed_integer() || self.is_unsigned_integer() || matches!(self, AnyInt) || self.is_ascii()
    }
    #[inline]
    pub fn is_integer(&self) -> bool {
        use DataType::*;
        self.is_signed_integer() || self.is_unsigned_integer() || matches!(self, AnyInt)
    }
    #[inline]
    pub fn is_signed_integer(&self) -> bool {
        use DataType::*;
        matches!(
            self,
            I8 | I16 | I32 | I64
        )
    }
    #[inline]
    pub fn is_unsigned_integer(&self) -> bool {
        use DataType::*;
        matches!(self, U1 | U8 | U16 | U32 | U64)
    }
    #[inline]
    pub fn is_bool(&self) -> bool {
        use DataType::*;
        matches!(self, Boolean)
    }
    #[inline]
    pub fn is_ascii(&self) -> bool {
        use DataType::*;
        matches!(self, ASCII32)
    }
    #[inline]
    pub fn cast_to_num(&self) -> DataType {
        if self.is_bool() {
            DataType::U1
        } else if self.is_ascii() {
            DataType::AnyInt
        } else {
            *self
        }
    }
    #[inline]
    pub fn cast_to_bool(&self) -> DataType {
        DataType::Boolean
    }
}

impl TryFrom<&arrow_schema::DataType> for DataType {
    type Error = FheSqlError;

    fn try_from(data_type: &arrow_schema::DataType) -> Result<Self, Self::Error> {
        match data_type {
            arrow_schema::DataType::Boolean => Ok(DataType::Boolean),
            arrow_schema::DataType::Int8 => Ok(DataType::I8),
            arrow_schema::DataType::Int16 => Ok(DataType::I16),
            arrow_schema::DataType::Int32 => Ok(DataType::I32),
            arrow_schema::DataType::Int64 => Ok(DataType::I64),
            arrow_schema::DataType::UInt8 => Ok(DataType::U8),
            arrow_schema::DataType::UInt16 => Ok(DataType::U16),
            arrow_schema::DataType::UInt32 => Ok(DataType::U32),
            arrow_schema::DataType::UInt64 => Ok(DataType::U64),
            arrow_schema::DataType::Utf8 => Ok(DataType::ASCII32),
            _ => Err(FheSqlError::UnsupportedSqlQuery(format!(
                "Unsupported DataType {}",
                data_type
            ))),
        }
    }
}

impl DataType {
    pub fn min_max_range(&self) -> ClosedInterval<SignedU64> {
        match self {
            DataType::Boolean => bool::min_max_range(),
            DataType::I8 => i8::min_max_range(),
            DataType::I16 => i16::min_max_range(),
            DataType::I32 => i32::min_max_range(),
            DataType::I64 => i64::min_max_range(),
            DataType::U1 => bool::min_max_range(),
            DataType::U8 => u8::min_max_range(),
            DataType::U16 => u16::min_max_range(),
            DataType::U32 => u32::min_max_range(),
            DataType::U64 => u64::min_max_range(),
            DataType::AnyInt => MINUS_PLUS_INFINITY_SIGNED_U64_RANGE,
            DataType::ASCII32 => MINUS_PLUS_INFINITY_SIGNED_U64_RANGE,
        }
    }
}
