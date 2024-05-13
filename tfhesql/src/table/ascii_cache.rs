use std::collections::HashMap;

#[cfg(feature = "parallel")]
use rayon::iter::*;

use crate::bitops::par_bitand_vec_ref;
use crate::hi_lo_tree::{Bytes64Equ, U64EqGtTree, U64EquTree};
use crate::types::ThreadSafeBool;
use crate::OrderedTables;
use crate::{
    ascii::ascii_to_le_u64x4,
    default_into::DefaultInto,
    query::sql_query_value::SqlQueryRightBytes256,
    uint::{block::U64x4, iter::*},
};

////////////////////////////////////////////////////////////////////////////////
// AsciiCache
////////////////////////////////////////////////////////////////////////////////

pub struct AsciiCache<B> {
    charx8: [U64EquTree<B>; 4],
    strings: HashMap<U64x4, B>,
}

impl<B> AsciiCache<B>
where
    B: Clone,
{
    pub fn new(bytes_256: &SqlQueryRightBytes256<B>) -> Self {
        AsciiCache {
            charx8: [
                U64EquTree::<B>::new(Bytes64Equ::<B>::from(&bytes_256.word_0_eq_gt)),
                U64EquTree::<B>::new(Bytes64Equ::<B>::from(&bytes_256.word_1_eq_ne)),
                U64EquTree::<B>::new(Bytes64Equ::<B>::from(&bytes_256.word_2_eq_ne)),
                U64EquTree::<B>::new(Bytes64Equ::<B>::from(&bytes_256.word_3_eq_ne)),
            ],
            strings: HashMap::<U64x4, B>::new(),
        }
    }
}

impl<B> AsciiCache<B>
where
    B: ThreadSafeBool + DefaultInto<B>,
{
    #[cfg(feature = "parallel")]
    pub fn fill(
        &mut self,
        tables: &OrderedTables,
        chunck_size: usize,
        num_cache: Option<&U64EqGtTree<B>>,
    ) {
        self.charx8
            .par_iter_mut()
            .enumerate()
            .for_each(|(word_index, tree)| {
                if word_index == 0 {
                    tree.fill_with_iter_and_secondary_tree(
                        tables.iter_ascii_u16(word_index as u8),
                        chunck_size,
                        num_cache,
                    )
                    .fill16();
                } else {
                    tree.fill_with_iter(tables.iter_ascii_u16(word_index as u8), chunck_size)
                        .fill16();
                }
                tree.fill_with_iter(tables.iter_ascii_u32(word_index as u8), chunck_size)
                    .fill32();
                tree.fill_with_iter(tables.iter_ascii_u64(word_index as u8), chunck_size)
                    .fill64();
            });

        // Collect all [u64x4] in all tables
        tables.iter_le_u64x4().for_each(|u64x4| {
            if self.strings.contains_key(&u64x4) {
                return;
            }
            self.strings.insert(u64x4, B::get_false());
        });

        // Parallel compute
        self.strings.par_iter_mut().for_each(|(u64x4, dst)| {
            *dst = Self::par_ascii_u64x4_eq(&self.charx8, u64x4);
        });

        // Clear unecessary memory
        self.charx8.iter_mut().for_each(|x| x.clear())
    }

    #[cfg(not(feature = "parallel"))]
    pub fn fill(
        &mut self,
        tables: &OrderedTables,
        chunck_size: usize,
        num_cache: Option<&U64EqGtTree<B>>,
    ) {
        use crate::bitops::par_bitand_10;

        self.charx8
            .iter_mut()
            .enumerate()
            .for_each(|(word_index, tree)| {
                if word_index == 0 {
                    tree.fill_with_iter_and_secondary_tree(
                        tables.iter_ascii_u16(word_index as u8),
                        chunck_size,
                        num_cache,
                    )
                    .fill16();
                } else {
                    tree.fill_with_iter(tables.iter_ascii_u16(word_index as u8), chunck_size)
                        .fill16();
                }
                tree.fill_with_iter(tables.iter_ascii_u32(word_index as u8), chunck_size)
                    .fill32();
                tree.fill_with_iter(tables.iter_ascii_u64(word_index as u8), chunck_size)
                    .fill64();
            });

        // Collect all [u64x4] in all tables
        tables.iter_le_u64x4().for_each(|u64x4| {
            if self.strings.contains_key(&u64x4) {
                return;
            }
            self.strings.insert(u64x4, B::get_false());
        });

        // Serial compute
        self.strings.iter_mut().for_each(|(u64x4, dst)| {
            *dst = Self::par_ascii_u64x4_eq(&self.charx8, u64x4);
        });

        // Clear unecessary memory
        self.charx8.iter_mut().for_each(|x| x.clear())
    }

    fn par_ascii_u64x4_eq(charx8: &[U64EquTree<B>; 4], u64x4: &[u64; 4]) -> B {
        let mut stack = vec![];
        if u64x4[3] == 0 {
            if u64x4[2] == 0 {
                if u64x4[1] == 0 {
                    charx8[0].ascii_eq_to_u64(u64x4[0], &mut stack);
                } else {
                    assert_ne!(u64x4[0], 0);
                    charx8[0].ascii_eq_to_u64(u64x4[0], &mut stack);
                    charx8[1].ascii_eq_to_u64(u64x4[1], &mut stack);
                }
            } else {
                assert_ne!(u64x4[0], 0);
                assert_ne!(u64x4[1], 0);
                charx8[0].ascii_eq_to_u64(u64x4[0], &mut stack);
                charx8[1].ascii_eq_to_u64(u64x4[1], &mut stack);
                charx8[2].ascii_eq_to_u64(u64x4[2], &mut stack);
            }
        } else {
            assert_ne!(u64x4[0], 0);
            assert_ne!(u64x4[1], 0);
            assert_ne!(u64x4[2], 0);
            charx8[0].ascii_eq_to_u64(u64x4[0], &mut stack);
            charx8[1].ascii_eq_to_u64(u64x4[1], &mut stack);
            charx8[2].ascii_eq_to_u64(u64x4[2], &mut stack);
            charx8[3].ascii_eq_to_u64(u64x4[3], &mut stack);
        }

        par_bitand_vec_ref(stack).unwrap()
    }
}

impl<B> AsciiCache<B> {
    pub fn equ(&self, other: &str) -> Option<&B> {
        let u64x4 = ascii_to_le_u64x4(other);
        self.strings.get(&u64x4)
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::AsciiCache;
    use crate::{query::sql_query_value::ClearSqlQueryRightBytes256, OrderedTables, Table};
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test() {
        let schema = Schema::new(vec![Field::new("String1", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(vec![
                "ab",
                // "abcd",
                // "1234567890abcdefghijkl",
                // "1234567890abcdefghijkl",
                // "1234567890abcdefghijkl",
                // "1234567890abcdefghijkl",
                // "1234567890abcdefghijkl",
                // "1234567890abcdefghijkl",
                // "1234567890abcdefghijkl",
                // "1234567890abcdefghijkl",
                // "1234567890abcdefghijkl",
                // "1234567890abcdefghijkl",
                // "1234567890abcdefghijkl",
                // "1234567890abcdefghijklmnopqrstuv",
                // "Hello!",
            ]))],
        )
        .unwrap();

        let t1 = Table::new("table1", batch);
        let tables: OrderedTables = OrderedTables::new(vec![t1]).unwrap();

        //let secret_str = "Hello!";
        let secret_str = "ab";
        let secret_str_bytes = ClearSqlQueryRightBytes256::from(secret_str);
        let mut c = AsciiCache::<bool>::new(&secret_str_bytes);

        c.fill(&tables, 100, None);

        assert_eq!(c.equ("ab").unwrap(), &true);
        // assert_eq!(c.equ("ab").unwrap(), &false);
        // assert_eq!(c.equ("1234567890abcdefghijkl").unwrap(), &false);
        // assert_eq!(c.equ("1234567890abcdefghijklmnopqrstuv").unwrap(), &false);
        // assert_eq!(c.equ("Hello!").unwrap(), &true);
    }
}
