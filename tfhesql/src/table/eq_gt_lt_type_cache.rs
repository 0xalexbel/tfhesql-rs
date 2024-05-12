use crate::{default_into::DefaultInto, types::ThreadSafeBool};
use crate::hi_lo_tree::{EqGtLt, EqNe};
use crate::hi_lo_tree::U64EqGtTree;
use crate::hi_lo_tree::traits::CompareToSignedInteger;
use crate::hi_lo_tree::traits::CompareToUnsignedInteger;
use super::type_cache::{TypedTableValueCache, TypedValueCache};
use super::ascii_cache::AsciiCache;

////////////////////////////////////////////////////////////////////////////////

// Special case when the cache only stores an EqGtLt struct
pub type EqGtLtTypedTableValueCache<B> = TypedTableValueCache<EqGtLt<B>>;
type EqGtLtTypedValueCache<B> = TypedValueCache<EqGtLt<B>>;

////////////////////////////////////////////////////////////////////////////////
// EqGtLtTypedValueCache
////////////////////////////////////////////////////////////////////////////////

impl<B> EqGtLtTypedValueCache<B>
where
    B: ThreadSafeBool + DefaultInto<B>,
{
    /// For all cached pairs ``Cache(Column(i), EqGt(value))``,
    /// with 0 <= i < Num Cols, computes:
    /// ``Cache(Column(i), EqGtLt(value))``
    pub fn fill_from_eq_gt_cache(
        &mut self,
        right_strictly_negative: &EqNe<B>,
        eq_gt_cache: &U64EqGtTree<B>,
        ascii_cache: &AsciiCache<B>,
    ) {
        use crate::utils::rayon::rayon_join4;

        let not_right_is_strictly_negative = right_strictly_negative.ne();
        let right_is_strictly_negative = right_strictly_negative.eq();

        macro_rules! compute_unsigned {
            ($cache:tt) => {
                self.$cache.for_each(|dst, left_value| {
                    *dst = eq_gt_cache.eq_gt_lt_from_unsigned(*left_value);
                })
            };
        }

        macro_rules! compute_signed {
            ($cache:tt, $uint:ty) => {
                self.$cache.for_each(|dst, left_value| {
                    *dst = eq_gt_cache.eq_gt_lt_from_signed(
                        (left_value.abs() as $uint, *left_value < 0),
                        right_is_strictly_negative,
                        not_right_is_strictly_negative,
                    );
                })
            };
        }

        // We do not take the negative sign into account when dealing with unsigned integer
        // We assume that the Tree compiler has performed the required optimisations and simplifications
        // For example U16Column > -2 is always TRUE
        rayon_join4(
            || {
                rayon_join4(
                    || compute_unsigned!(u8_cache),
                    || compute_unsigned!(u16_cache),
                    || compute_unsigned!(u32_cache),
                    || compute_unsigned!(u64_cache),
                )
            },
            || {
                rayon_join4(
                    || compute_signed!(i8_cache, u8),
                    || compute_signed!(i16_cache, u16),
                    || compute_signed!(i32_cache, u32),
                    || compute_signed!(i64_cache, u64),
                )
            },
            || compute_unsigned!(bool_cache),
            || {
                self.str_cache.for_each(|dst, str| {
                    let eq = ascii_cache.equ(str.as_str()).unwrap();
                    // String only supports the EQ operator
                    dst.eq = eq.clone();
                });
            },
        );
    }
}

impl<B> EqGtLtTypedTableValueCache<B> 
where
    B: ThreadSafeBool + DefaultInto<B>,
{
    pub fn fill_from_eq_gt(
        &mut self,
        right_strictly_negative: &EqNe<B>,
        eq_gt_cache: &U64EqGtTree<B>,
        ascii_cache: &AsciiCache<B>,
    ) {
        assert!(!self.value_cache_dropped);

        self.value_cache.fill_from_eq_gt_cache(
            right_strictly_negative,
            eq_gt_cache,
            ascii_cache,
        );
    }
}
