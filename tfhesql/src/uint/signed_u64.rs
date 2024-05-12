use super::interval::ClosedInterval;
use crate::error::FheSqlError;
use std::cmp::Ordering;

////////////////////////////////////////////////////////////////////////////////
// SignedU64
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SignedU64 {
    abs: u64,
    is_strictly_negative: bool,
}

////////////////////////////////////////////////////////////////////////////////

pub const MINUS_INFINITY_SIGNED_U64: SignedU64 = SignedU64 {
    abs: u64::MAX,
    is_strictly_negative: true,
};
pub const PLUS_INFINITY_SIGNED_U64: SignedU64 = SignedU64 {
    abs: u64::MAX,
    is_strictly_negative: false,
};
pub const ZERO_SIGNED_U64: SignedU64 = SignedU64 {
    abs: 0,
    is_strictly_negative: false,
};

////////////////////////////////////////////////////////////////////////////////

impl PartialOrd for SignedU64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SignedU64 {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.is_strictly_negative {
            // self < 0
            if other.is_strictly_negative {
                // rhs < 0
                match self.abs.cmp(&other.abs) {
                    Ordering::Less => Ordering::Greater,
                    Ordering::Equal => Ordering::Equal,
                    Ordering::Greater => Ordering::Less,
                }
            } else {
                // rhs >= 0
                Ordering::Less
            }
        } else {
            // self >= 0
            if other.is_strictly_negative {
                // rhs < 0
                Ordering::Greater
            } else {
                // rhs >= 0
                self.abs.cmp(&other.abs)
            }
        }
    }
}

impl std::ops::Neg for SignedU64 {
    type Output = SignedU64;

    fn neg(self) -> Self::Output {
        if self.abs == 0 {
            self
        } else {
            SignedU64 {
                abs: self.abs,
                is_strictly_negative: !self.is_strictly_negative,
            }
        }
    }
}

pub trait MinMaxRange {
    fn min_max_range() -> ClosedInterval<SignedU64>;
}

macro_rules! impl_from_uint_data_num {
    ($($uint:ty),*) => {
        $(
            impl From<$uint> for SignedU64 {
                fn from(value: $uint) -> Self {
                    SignedU64 {abs: value as u64, is_strictly_negative: false}
                }
            }
            impl From<&$uint> for SignedU64 {
                fn from(value: &$uint) -> Self {
                    SignedU64 {abs: *value as u64, is_strictly_negative: false}
                }
            }
        )*
    };
}

macro_rules! impl_from_int_data_num {
    ($($int:ty),*) => {
        $(
            impl From<$int> for SignedU64 {
                fn from(value: $int) -> Self {
                    SignedU64 {abs:(value as i128).abs() as u64, is_strictly_negative:value < 0}
                }
            }
            impl From<&$int> for SignedU64 {
                fn from(value: &$int) -> Self {
                    SignedU64 {abs:(*value as i128).abs() as u64, is_strictly_negative:value < &0}
                }
            }
        )*
    };
}

macro_rules! impl_min_max_range {
    ($($int:ty),*) => {
        $(
            impl MinMaxRange for $int {
                fn min_max_range() -> ClosedInterval<SignedU64> {
                    ClosedInterval::<SignedU64>::new(SignedU64::from(<$int>::MIN), SignedU64::from(<$int>::MAX))
                }
            }
        )*
    };
}

macro_rules! impl_try_from_signed_u64 {
    ($($int:ty),*) => {
        $(
            impl TryFrom<SignedU64> for $int {
                type Error = crate::error::FheSqlError;

                fn try_from(value: SignedU64) -> Result<Self, Self::Error> {
                    let range = <$int>::min_max_range();
                    if !range.contains(&value) {
                        Err(crate::error::FheSqlError::InternalError(String::new()))
                    } else {
                        Ok(value.abs as $int)
                    }
                }
            }
        )*
    };
}

impl From<bool> for SignedU64 {
    fn from(value: bool) -> Self {
        SignedU64 {
            abs: if value { 1 } else { 0 },
            is_strictly_negative: false,
        }
    }
}
impl From<&bool> for SignedU64 {
    fn from(value: &bool) -> Self {
        SignedU64 {
            abs: if *value { 1 } else { 0 },
            is_strictly_negative: false,
        }
    }
}

impl MinMaxRange for bool {
    fn min_max_range() -> ClosedInterval<SignedU64> {
        ClosedInterval::<SignedU64>::new(SignedU64::from(0), SignedU64::from(1))
    }
}

impl_from_uint_data_num!(u8, u16, u32, u64);
impl_from_int_data_num!(i8, i16, i32, i64);
impl_min_max_range!(u8, u16, u32, u64);
impl_min_max_range!(i8, i16, i32, i64);
impl_try_from_signed_u64!(u8, u16, u32, u64);
impl_try_from_signed_u64!(i8, i16, i32, i64);

// literal out of range for `u8`
// the literal `2345_u8` does not fit into the type `u8` whose range is `0..=255`
// `#[deny(overflowing_literals)]` on by default

impl std::fmt::Display for SignedU64 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.is_strictly_negative {
            return f.write_str(format!("-{}", self.abs).as_str());
        } else {
            return f.write_str(format!("{}", self.abs).as_str());
        }
    }
}

impl TryFrom<&str> for SignedU64 {
    type Error = FheSqlError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.parse::<u64>() {
            Ok(some_u64) => Ok(SignedU64::from(some_u64)),
            Err(_) => match value.parse::<f64>() {
                Ok(some_f64) => {
                    if some_f64.fract() != 0.0 || some_f64.abs() > u64::MAX as f64 {
                        Err(FheSqlError::parse_int_error(value))
                    } else if some_f64 >= 0.0 {
                        if some_f64 > u64::MAX as f64 {
                            Err(FheSqlError::parse_int_error(value))
                        } else {
                            Ok(SignedU64::from(some_f64 as u64))
                        }
                    } else if some_f64.abs() > i64::MAX as f64 {
                        Err(FheSqlError::parse_int_error(value))
                    } else {
                        Ok(SignedU64::from(some_f64 as i64))
                    }
                }
                Err(_) => Err(FheSqlError::parse_int_error(value)),
            },
        }
    }
}

impl SignedU64 {
    pub fn from_str(str: &str) -> Self {
        match SignedU64::try_from(str) {
            Ok(su64) => su64,
            Err(_) => SignedU64::from(0),
        }
    }
}

impl SignedU64 {
    #[inline]
    pub fn abs(&self) -> u64 {
        self.abs
    }
    pub fn is_strictly_negative(&self) -> bool {
        self.is_strictly_negative
    }
}
