use arrow_array::cast::*;
use arrow_array::types::*;
use arrow_array::*;
use arrow_schema::DataType;
use std::ops::ControlFlow;

use super::row_visitor::TableVisitor;
use crate::maps::IndexedMap;
use crate::bitops::{par_bitor_vec_ref, RefBitAnd, RefBitOr};
use crate::types::ThreadSafeBool;
use crate::OrderedTables;
use crate::Table;
use crate::{default_into::DefaultInto, uint::mask::BoolMask};

////////////////////////////////////////////////////////////////////////////////

type BoolCache<T, B> = IndexedMap<(bool, T), B>;
type I8Cache<T, B> = IndexedMap<(i8, T), B>;
type U8Cache<T, B> = IndexedMap<(u8, T), B>;
type I16Cache<T, B> = IndexedMap<(i16, T), B>;
type U16Cache<T, B> = IndexedMap<(u16, T), B>;
type I32Cache<T, B> = IndexedMap<(i32, T), B>;
type U32Cache<T, B> = IndexedMap<(u32, T), B>;
type I64Cache<T, B> = IndexedMap<(i64, T), B>;
type U64Cache<T, B> = IndexedMap<(u64, T), B>;
type StringCache<T, B> = IndexedMap<(String, T), B>;

////////////////////////////////////////////////////////////////////////////////
// TypeCache
////////////////////////////////////////////////////////////////////////////////

pub struct TypeCache<T, B> {
    pub(super) bool_cache: BoolCache<T, B>,
    pub(super) i8_cache: I8Cache<T, B>,
    pub(super) u8_cache: U8Cache<T, B>,
    pub(super) i16_cache: I16Cache<T, B>,
    pub(super) u16_cache: U16Cache<T, B>,
    pub(super) i32_cache: I32Cache<T, B>,
    pub(super) u32_cache: U32Cache<T, B>,
    pub(super) i64_cache: I64Cache<T, B>,
    pub(super) u64_cache: U64Cache<T, B>,
    pub(super) str_cache: StringCache<T, B>,
}

pub(super) type TypedValueCache<T> = TypeCache<(), T>;
type TypedColumnValueCache<T> = TypeCache<usize, T>;

////////////////////////////////////////////////////////////////////////////////

impl<T, B> Default for TypeCache<T, B>
where
    T: Default,
{
    fn default() -> Self {
        Self {
            bool_cache: Default::default(),
            i8_cache: Default::default(),
            u8_cache: Default::default(),
            i16_cache: Default::default(),
            u16_cache: Default::default(),
            i32_cache: Default::default(),
            u32_cache: Default::default(),
            i64_cache: Default::default(),
            u64_cache: Default::default(),
            str_cache: Default::default(),
        }
    }
}

pub trait TypedMap<I, T, B> {
    fn map(&self) -> &IndexedMap<(I, T), B>;
    fn map_mut(&mut self) -> &mut IndexedMap<(I, T), B>;
}

macro_rules! impl_typed_map {
    ($ty:ty, $cache:tt) => {
        impl<T, B> TypedMap<$ty, T, B> for TypeCache<T, B> {
            #[inline]
            fn map(&self) -> &IndexedMap<($ty, T), B> {
                &self.$cache
            }
            #[inline]
            fn map_mut(&mut self) -> &mut IndexedMap<($ty, T), B> {
                &mut self.$cache
            }
        }
    };
}

impl_typed_map!(bool, bool_cache);
impl_typed_map!(i8, i8_cache);
impl_typed_map!(i16, i16_cache);
impl_typed_map!(i32, i32_cache);
impl_typed_map!(i64, i64_cache);
impl_typed_map!(u8, u8_cache);
impl_typed_map!(u16, u16_cache);
impl_typed_map!(u32, u32_cache);
impl_typed_map!(u64, u64_cache);
impl_typed_map!(String, str_cache);

impl<KP, V> TypeCache<KP, V>
where
    KP: std::hash::Hash + std::cmp::Eq + Clone,
    V: Clone,
{
    #[inline]
    fn cache_mut<I>(&mut self) -> &mut IndexedMap<(I, KP), V>
    where
        Self: TypedMap<I, KP, V>,
    {
        self.map_mut()
    }

    #[inline]
    fn insert_key<I>(&mut self, key: I, key_params: KP, default: &V)
    where
        Self: TypedMap<I, KP, V>,
        I: std::hash::Hash + std::cmp::Eq + Clone,
    {
        self.cache_mut().insert_key((key, key_params), default);
    }
}

////////////////////////////////////////////////////////////////////////////////
// TypedColumnValueCache
////////////////////////////////////////////////////////////////////////////////

impl<T> TypedColumnValueCache<T> {
    pub fn get_table_row(&self, table: &Table, row_index: usize) -> Vec<&T> {
        let mut v: Vec<&T> = vec![];
        if table.num_rows() <= row_index {
            return v;
        }
        table
            .batch()
            .columns()
            .iter()
            .enumerate()
            .for_each(|(column_index, column)| {
                macro_rules! push_primitive {
                    ($cache:tt, $ttype:tt) => {
                        let a = as_primitive_array::<$ttype>(column).value(row_index);
                        let b = self.$cache.get(a, column_index);
                        v.push(b);
                    };
                }
                match column.data_type() {
                    DataType::Boolean => {
                        let a = as_boolean_array(column).value(row_index);
                        let b = self.bool_cache.get(a, column_index);
                        v.push(b);
                    }
                    DataType::Int8 => {
                        push_primitive!(i8_cache, Int8Type);
                    }
                    DataType::Int16 => {
                        push_primitive!(i16_cache, Int16Type);
                    }
                    DataType::Int32 => {
                        push_primitive!(i32_cache, Int32Type);
                    }
                    DataType::Int64 => {
                        push_primitive!(i64_cache, Int64Type);
                    }
                    DataType::UInt8 => {
                        push_primitive!(u8_cache, UInt8Type);
                    }
                    DataType::UInt16 => {
                        push_primitive!(u16_cache, UInt16Type);
                    }
                    DataType::UInt32 => {
                        push_primitive!(u32_cache, UInt32Type);
                    }
                    DataType::UInt64 => {
                        push_primitive!(u64_cache, UInt64Type);
                    }
                    DataType::Utf8 => {
                        let a = as_string_array(column).value(row_index);
                        let b = self.str_cache.get(a.to_string(), column_index);
                        v.push(b);
                    }
                    _ => panic!("Invalid data type"),
                };
            });
        v
    }
}

////////////////////////////////////////////////////////////////////////////////
// TypedTableValueCache
////////////////////////////////////////////////////////////////////////////////

pub struct TypedTableValueCache<T> {
    pub(super) value_cache_dropped: bool,
    pub(super) value_cache: TypedValueCache<T>,
    pub(super) column_value_cache: TypedColumnValueCache<T>,
}

impl<T> Default for TypedTableValueCache<T> {
    fn default() -> Self {
        Self {
            value_cache_dropped: false,
            value_cache: Default::default(),
            column_value_cache: Default::default(),
        }
    }
}

impl<T> TypedTableValueCache<T>
where
    T: DefaultInto<T> + Clone,
{
    /// For all values in Tables[k] with 0 <= k < NumTables,
    /// fill value cache with default value.
    /// - key = Tables[k][i]
    /// - value = Default
    /// - Cache(Tables[k][i]) = Default
    #[inline]
    pub fn default_into_from_ordered_tables(&mut self, tables: &OrderedTables) {
        assert!(!self.value_cache_dropped);
        let mut c = Fill::<T> { cache: self };
        c.fill_default_into(tables);
    }
}

impl<T> TypedTableValueCache<T>
where
    T: Send + Sync + RefBitOr<Output = T> + DefaultInto<T> + Clone,
{
    /// ``OR { 0 <= i < NumCols; Cache(Column(i), Table[row_index]) }``
    pub fn par_or_row(&self, table: &Table, row_index: usize) -> T {
        if table.num_rows() <= row_index {
            return T::default_into();
        }
        let v = self.column_value_cache.get_table_row(table, row_index);
        par_bitor_vec_ref(v).unwrap()
    }
}

impl<T> TypedTableValueCache<T>
where
    T: Send + Sync,
{
    /// For all pairs ``(Column(i), value)`` with 0 <= i < Num Cols, computes:
    /// ``Cache(Column(i), value) = ColumnMask(i) & Cache(value)``
    #[cfg(feature = "parallel")]
    pub fn bitand_col_value<B>(&mut self, column_mask: &BoolMask<B>)
    where
        T: RefBitAnd<B, Output = T> + Clone,
        B: ThreadSafeBool,
    {
        use crate::utils::rayon::rayon_join4;
        assert!(!self.value_cache_dropped);

        macro_rules! col_mask_and {
            ($cache:tt) => {
                self.column_value_cache
                    .$cache
                    .for_each(|dst, column_value, column_index| {
                        let t = self.value_cache.$cache.get(column_value.clone());
                        *dst = t.refref_bitand(&column_mask.mask[*column_index]);
                    })
            };
        }

        rayon_join4(
            || {
                rayon_join4(
                    || col_mask_and!(u8_cache),
                    || col_mask_and!(u16_cache),
                    || col_mask_and!(u32_cache),
                    || col_mask_and!(u64_cache),
                )
            },
            || {
                rayon_join4(
                    || col_mask_and!(i8_cache),
                    || col_mask_and!(i16_cache),
                    || col_mask_and!(i32_cache),
                    || col_mask_and!(i64_cache),
                )
            },
            || col_mask_and!(bool_cache),
            || col_mask_and!(str_cache),
        );
    }

    /// For all pairs ``(Column(i), value)`` with 0 <= i < Num Cols, computes:
    /// ``Cache(Column(i), value) = ColumnMask(i) & Cache(value)``
    #[cfg(not(feature = "parallel"))]
    pub fn bitand_col_value<B>(&mut self, column_mask: &BoolMask<B>)
    where
        T: RefBitAnd<B, Output = T> + Clone,
        B: ThreadSafeBool,
    {
        assert!(!self.value_cache_dropped);

        macro_rules! col_mask_and {
            ($cache:tt) => {
                self.column_value_cache
                    .$cache
                    .for_each(|dst, column_value, column_index| {
                        let t = self.value_cache.$cache.get(column_value.clone());
                        *dst = t.refref_bitand(&column_mask.mask[*column_index]);
                    })
            };
        }

        col_mask_and!(u8_cache);
        col_mask_and!(u16_cache);
        col_mask_and!(u32_cache);
        col_mask_and!(u64_cache);

        col_mask_and!(i8_cache);
        col_mask_and!(i16_cache);
        col_mask_and!(i32_cache);
        col_mask_and!(i64_cache);

        col_mask_and!(bool_cache);
        col_mask_and!(str_cache);
    }
}

////////////////////////////////////////////////////////////////////////////////
// Fill
////////////////////////////////////////////////////////////////////////////////

struct Fill<'a, T> {
    cache: &'a mut TypedTableValueCache<T>,
}

impl<'a, T> Fill<'a, T>
where
    T: DefaultInto<T> + Clone,
{
    #[inline]
    pub fn fill_default_into(&'a mut self, tables: &OrderedTables) {
        tables.visit(self);
    }
}

impl<'a, T> TableVisitor for Fill<'a, T>
where
    T: DefaultInto<T> + Clone,
{
    type Break = ();

    fn visit_bool(
        &mut self,
        _column: &arrow_array::ArrayRef,
        column_index: usize,
        _row_index: usize,
        value: bool,
    ) -> ControlFlow<Self::Break> {
        self.cache
            .value_cache
            .insert_key::<bool>(value, (), &T::default_into());
        self.cache
            .column_value_cache
            .insert_key::<bool>(value, column_index, &T::default_into());
        ControlFlow::Continue(())
    }

    fn visit_i8(
        &mut self,
        _column: &arrow_array::ArrayRef,
        column_index: usize,
        _row_index: usize,
        value: i8,
    ) -> ControlFlow<Self::Break> {
        self.cache
            .value_cache
            .insert_key::<i8>(value, (), &T::default_into());
        self.cache
            .column_value_cache
            .insert_key::<i8>(value, column_index, &T::default_into());
        ControlFlow::Continue(())
    }

    fn visit_i16(
        &mut self,
        _column: &arrow_array::ArrayRef,
        column_index: usize,
        _row_index: usize,
        value: i16,
    ) -> ControlFlow<Self::Break> {
        self.cache
            .value_cache
            .insert_key::<i16>(value, (), &T::default_into());
        self.cache
            .column_value_cache
            .insert_key::<i16>(value, column_index, &T::default_into());
        ControlFlow::Continue(())
    }

    fn visit_i32(
        &mut self,
        _column: &arrow_array::ArrayRef,
        column_index: usize,
        _row_index: usize,
        value: i32,
    ) -> ControlFlow<Self::Break> {
        self.cache
            .value_cache
            .insert_key::<i32>(value, (), &T::default_into());
        self.cache
            .column_value_cache
            .insert_key::<i32>(value, column_index, &T::default_into());
        ControlFlow::Continue(())
    }

    fn visit_i64(
        &mut self,
        _column: &arrow_array::ArrayRef,
        column_index: usize,
        _row_index: usize,
        value: i64,
    ) -> ControlFlow<Self::Break> {
        self.cache
            .value_cache
            .insert_key::<i64>(value, (), &T::default_into());
        self.cache
            .column_value_cache
            .insert_key::<i64>(value, column_index, &T::default_into());
        ControlFlow::Continue(())
    }

    fn visit_u8(
        &mut self,
        _column: &arrow_array::ArrayRef,
        column_index: usize,
        _row_index: usize,
        value: u8,
    ) -> ControlFlow<Self::Break> {
        self.cache
            .value_cache
            .insert_key::<u8>(value, (), &T::default_into());
        self.cache
            .column_value_cache
            .insert_key::<u8>(value, column_index, &T::default_into());
        ControlFlow::Continue(())
    }

    fn visit_u16(
        &mut self,
        _column: &arrow_array::ArrayRef,
        column_index: usize,
        _row_index: usize,
        value: u16,
    ) -> ControlFlow<Self::Break> {
        self.cache
            .value_cache
            .insert_key::<u16>(value, (), &T::default_into());
        self.cache
            .column_value_cache
            .insert_key::<u16>(value, column_index, &T::default_into());
        ControlFlow::Continue(())
    }

    fn visit_u32(
        &mut self,
        _column: &arrow_array::ArrayRef,
        column_index: usize,
        _row_index: usize,
        value: u32,
    ) -> ControlFlow<Self::Break> {
        self.cache
            .value_cache
            .insert_key::<u32>(value, (), &T::default_into());
        self.cache
            .column_value_cache
            .insert_key::<u32>(value, column_index, &T::default_into());
        ControlFlow::Continue(())
    }

    fn visit_u64(
        &mut self,
        _column: &arrow_array::ArrayRef,
        column_index: usize,
        _row_index: usize,
        value: u64,
    ) -> ControlFlow<Self::Break> {
        self.cache
            .value_cache
            .insert_key::<u64>(value, (), &T::default_into());
        self.cache
            .column_value_cache
            .insert_key::<u64>(value, column_index, &T::default_into());
        ControlFlow::Continue(())
    }

    fn visit_str(
        &mut self,
        _column: &arrow_array::ArrayRef,
        column_index: usize,
        _row_index: usize,
        value: &str,
    ) -> ControlFlow<Self::Break> {
        self.cache
            .value_cache
            .insert_key::<String>(value.to_string(), (), &T::default_into());
        self.cache.column_value_cache.insert_key::<String>(
            value.to_string(),
            column_index,
            &T::default_into(),
        );
        ControlFlow::Continue(())
    }
}
