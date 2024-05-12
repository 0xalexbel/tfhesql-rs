use super::eq_ne::EqNe;
use crate::default_into::*;
use crate::encrypt::*;
use crate::encrypt::traits::*;
use crate::uint::maps::UIntLeBlocksPow2Array;
use crate::uint::FromN;

////////////////////////////////////////////////////////////////////////////////
// ZeroMax
////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, PartialEq, Eq, Debug, serde::Deserialize, serde::Serialize)]
pub struct ZeroMax<B> {
    pub(super) is_zero: EqNe<B>,
    pub(super) is_max: EqNe<B>,
}

derive2_encrypt_decrypt! { ZeroMax<T> {is_zero:EqNe<T>, is_max:EqNe<T>} }

pub type ClearZeroMax = ZeroMax<bool>;
pub type ZeroMaxPow2Array<B> = UIntLeBlocksPow2Array<ZeroMax<B>>;

////////////////////////////////////////////////////////////////////////////////
// ClearZeroMax
////////////////////////////////////////////////////////////////////////////////
 
impl<Num> From<Num> for ClearZeroMax
where
    Num: tfhe::core_crypto::commons::numeric::Numeric,
{
    #[inline]
    fn from(value: Num) -> Self {
        ZeroMax {
            is_zero: EqNe::from((value, Num::ZERO)),
            is_max: EqNe::from((value, Num::MAX)),
        }
    }
}

impl<Num> FromN<Num, 2> for ClearZeroMax
where
    Num: tfhe::core_crypto::commons::numeric::Numeric,
{
    #[inline]
    fn from_n(n_values: &[Num; 2]) -> Self {
        ZeroMax {
            is_zero: EqNe::from_n(&[(n_values[0], Num::ZERO), (n_values[1], Num::ZERO)]),
            is_max: EqNe::from_n(&[(n_values[0], Num::MAX), (n_values[1], Num::MAX)]),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
impl<T> ZeroMax<T> {
    #[inline]
    fn eq_max(&self) -> &T {
        &self.is_max.eq
    }
    #[inline]
    fn ne_max(&self) -> &T {
        &self.is_max.ne
    }
    #[inline]
    fn eq_zero(&self) -> &T {
        &self.is_zero.eq
    }
    #[inline]
    fn ne_zero(&self) -> &T {
        &self.is_zero.ne
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test_zero_max {
    use super::*;
    use crate::uint::maps::UIntLeBlocksPow2Array;
    use crate::uint::traits::ToU16;
    use crate::uint::traits::ToU32;

    #[test]
    fn test() {
        let zm = ZeroMax::from(12345u32);
        assert_eq!(zm.eq_max(), &false);
        assert_eq!(zm.eq_zero(), &false);
        assert_eq!(zm.ne_zero(), &true);
        assert_eq!(zm.ne_max(), &true);

        let zm = ZeroMax::from(0u32);
        assert_eq!(zm.eq_max(), &false);
        assert_eq!(zm.eq_zero(), &true);
        assert_eq!(zm.ne_zero(), &false);
        assert_eq!(zm.ne_max(), &true);

        let zm = ZeroMax::from(u32::MAX);
        assert_eq!(zm.eq_max(), &true);
        assert_eq!(zm.eq_zero(), &false);
        assert_eq!(zm.ne_zero(), &true);
        assert_eq!(zm.ne_max(), &false);

        let zm = ZeroMax::from(u128::MAX);
        assert_eq!(zm.eq_max(), &true);
        assert_eq!(zm.eq_zero(), &false);
        assert_eq!(zm.ne_zero(), &true);
        assert_eq!(zm.ne_max(), &false);

        let zm = ZeroMax::from_n(&[u128::MAX, u128::MAX]);
        assert_eq!(zm.eq_max(), &true);
        assert_eq!(zm.eq_zero(), &false);
        assert_eq!(zm.ne_zero(), &true);
        assert_eq!(zm.ne_max(), &false);

        let zm = ZeroMax::from_n(&[u128::MIN, u128::MIN]);
        assert_eq!(zm.eq_max(), &false);
        assert_eq!(zm.eq_zero(), &true);
        assert_eq!(zm.ne_zero(), &false);
        assert_eq!(zm.ne_max(), &true);
    }

    #[test]
    fn test_zero_max_le_array() {
        type ZeroMaxLeArray = UIntLeBlocksPow2Array<ZeroMax<bool>>;

        let le = [0u16, 0u16, u16::MAX, 0u16];
        let a = u64::from_le_u16(le);
        let zm_array = ZeroMaxLeArray::from(a);
        assert_eq!(zm_array.bytes_len(), 8);
        assert_eq!(zm_array.count_blocks(16 / 8), 4);
        assert!(zm_array.le_uint_at(0, 16 / 8).eq_zero());
        assert!(zm_array.le_uint_at(1, 16 / 8).eq_zero());
        assert!(zm_array.le_uint_at(2, 16 / 8).eq_max());
        assert!(zm_array.le_uint_at(3, 16 / 8).eq_zero());
        le.iter().enumerate().for_each(|(i, v)| {
            assert!(zm_array.le_uint_at(i as u8, 16 / 8).eq_zero() == &(*v == 0u16));
        });
        le.iter().enumerate().for_each(|(i, v)| {
            assert!(zm_array.le_uint_at(i as u8, 16 / 8).eq_max() == &(*v == u16::MAX));
        });

        let le = [0u32, u32::MAX];
        let a = u64::from_le_u32(le);
        let zm_array = ZeroMaxLeArray::from(a);
        assert_eq!(zm_array.bytes_len(), 8);
        assert_eq!(zm_array.count_blocks(32 / 8), 2);
        assert!(zm_array.le_uint_at(0, 16 / 8).eq_zero());
        assert!(zm_array.le_uint_at(1, 16 / 8).eq_zero());
        assert!(zm_array.le_uint_at(2, 16 / 8).eq_max());
        assert!(zm_array.le_uint_at(3, 16 / 8).eq_max());
        assert!(zm_array.le_uint_at(0, 32 / 8).eq_zero());
        assert!(zm_array.le_uint_at(1, 32 / 8).eq_max());
        le.iter().enumerate().for_each(|(i, v)| {
            assert!(zm_array.le_u32_at(i as u8).eq_zero() == &(*v == 0u32));
            assert!(zm_array.le_uint_at(i as u8, 32 / 8).eq_zero() == &(*v == 0u32));
        });
        le.iter().enumerate().for_each(|(i, v)| {
            assert!(zm_array.le_u32_at(i as u8).eq_max() == &(*v == u32::MAX));
            assert!(zm_array.le_uint_at(i as u8, 32 / 8).eq_max() == &(*v == u32::MAX));
        });
    }
}
