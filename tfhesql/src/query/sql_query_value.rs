use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::hi_lo_tree::Bytes64EqGt;
use crate::hi_lo_tree::ClearBytes64EqGt;
use crate::hi_lo_tree::Bytes64EqNe;
use crate::hi_lo_tree::ClearBytes256EqNe;
use crate::hi_lo_tree::ClearBytes64EqNe;
use crate::hi_lo_tree::EqNe;
use crate::sql_ast::and_or_ast::AstRightValue;
use crate::uint::mask::BoolMask;
use crate::uint::mask::ClearBoolMask;
use crate::types::MemoryCastInto;

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct SqlQueryRightBytes256<B> {
    pub word_0_eq_gt: Bytes64EqGt<B>,
    pub word_1_eq_ne: Bytes64EqNe<B>,
    pub word_2_eq_ne: Bytes64EqNe<B>,
    pub word_3_eq_ne: Bytes64EqNe<B>,
}

derive4_encrypt_decrypt! { SqlQueryRightBytes256<B> {word_0_eq_gt: Bytes64EqGt<B>, word_1_eq_ne: Bytes64EqNe<B>, word_2_eq_ne: Bytes64EqNe<B>, word_3_eq_ne: Bytes64EqNe<B>} }

pub type ClearSqlQueryRightBytes256 = SqlQueryRightBytes256<bool>;

impl From<u64> for ClearSqlQueryRightBytes256 {
    fn from(value: u64) -> Self {
        ClearSqlQueryRightBytes256 {
            word_0_eq_gt: ClearBytes64EqGt::from(value),
            word_1_eq_ne: ClearBytes64EqNe::from(0),
            word_2_eq_ne: ClearBytes64EqNe::from(0),
            word_3_eq_ne: ClearBytes64EqNe::from(0),
        }
    }
}

impl From<&str> for ClearSqlQueryRightBytes256 {
    fn from(value: &str) -> Self {
        let b = ClearBytes256EqNe::from(value);
        ClearSqlQueryRightBytes256 {
            word_0_eq_gt: MemoryCastInto::<ClearBytes64EqGt>::mem_cast_into(b.word0),
            word_1_eq_ne: b.word1,
            word_2_eq_ne: b.word2,
            word_3_eq_ne: b.word3,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct SqlQueryRightOperand<B> {
    pub ident_mask: BoolMask<B>,
    pub bytes_256: SqlQueryRightBytes256<B>,
    pub is_strictly_negative: EqNe<B>,
    pub is_value: B,
}

pub type ClearSqlQueryValue = SqlQueryRightOperand<bool>;

impl ClearSqlQueryValue {
    pub(super) fn build(
        ident: &ClearBoolMask,
        value: &AstRightValue,
        has_minus_sign: bool,
    ) -> Self {
        SqlQueryRightOperand {
            ident_mask: ident.clone(),
            bytes_256: match value {
                AstRightValue::Number(num) => {
                    // if negative: must be strictly negative !
                    assert!(!has_minus_sign || *num != 0);
                    ClearSqlQueryRightBytes256::from(*num)
                }
                AstRightValue::Ascii(str) => ClearSqlQueryRightBytes256::from(str.as_str()),
            },
            is_strictly_negative: EqNe {
                eq: has_minus_sign,
                ne: !has_minus_sign,
            },
            is_value: ident.count_set() == 0,
        }
    }
}

derive4_encrypt_decrypt! { SqlQueryRightOperand<B> {ident_mask: BoolMask<B>, bytes_256: SqlQueryRightBytes256<B>, is_strictly_negative: EqNe<B>, is_value: B} }
