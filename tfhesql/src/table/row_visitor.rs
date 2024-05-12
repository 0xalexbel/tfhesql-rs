use std::ops::ControlFlow;

use arrow_array::{
    cast::{as_boolean_array, as_primitive_array, as_string_array},
    types::{
        Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    ArrayRef,
};
use arrow_schema::DataType;

use super::{OrderedTables, Table};

pub trait TableVisitor {
    type Break;

    fn visit_bool(
        &mut self,
        _column: &ArrayRef,
        _column_index: usize,
        _row_index: usize,
        _value: bool,
    ) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn visit_i8(
        &mut self,
        _column: &ArrayRef,
        _column_index: usize,
        _row_index: usize,
        _value: i8,
    ) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn visit_i16(
        &mut self,
        _column: &ArrayRef,
        _column_index: usize,
        _row_index: usize,
        _value: i16,
    ) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn visit_i32(
        &mut self,
        _column: &ArrayRef,
        _column_index: usize,
        _row_index: usize,
        _value: i32,
    ) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn visit_i64(
        &mut self,
        _column: &ArrayRef,
        _column_index: usize,
        _row_index: usize,
        _value: i64,
    ) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn visit_u8(
        &mut self,
        _column: &ArrayRef,
        _column_index: usize,
        _row_index: usize,
        _value: u8,
    ) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn visit_u16(
        &mut self,
        _column: &ArrayRef,
        _column_index: usize,
        _row_index: usize,
        _value: u16,
    ) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn visit_u32(
        &mut self,
        _column: &ArrayRef,
        _column_index: usize,
        _row_index: usize,
        _value: u32,
    ) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn visit_u64(
        &mut self,
        _column: &ArrayRef,
        _column_index: usize,
        _row_index: usize,
        _value: u64,
    ) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
    fn visit_str(
        &mut self,
        _column: &ArrayRef,
        _column_index: usize,
        _row_index: usize,
        _value: &str,
    ) -> ControlFlow<Self::Break> {
        ControlFlow::Continue(())
    }
}

impl OrderedTables {
    pub fn visit<V>(&self, visitor: &mut V) -> ControlFlow<V::Break>
    where
        V: TableVisitor,
    {
        self.tables()
            .iter()
            .try_for_each(|table| table.visit(visitor))
    }
    pub fn visit_str<V>(&self, visitor: &mut V) -> ControlFlow<V::Break>
    where
        V: TableVisitor,
    {
        self.tables()
            .iter()
            .try_for_each(|table| table.visit_str(visitor))
    }
}

impl Table {
    pub fn visit_str<V>(&self, visitor: &mut V) -> ControlFlow<V::Break>
    where
        V: TableVisitor,
    {
        let n = self.batch().num_rows();
        (0..n).try_for_each(|i| self.visit_row_str(visitor, i))
    }
    pub fn visit<V>(&self, visitor: &mut V) -> ControlFlow<V::Break>
    where
        V: TableVisitor,
    {
        let n = self.batch().num_rows();
        (0..n).try_for_each(|i| self.visit_row(visitor, i))
    }
    pub fn visit_row<V>(&self, visitor: &mut V, row_index: usize) -> ControlFlow<V::Break>
    where
        V: TableVisitor,
    {
        self.batch().columns().iter().enumerate().try_for_each(
            |(column_index, column)| match column.data_type() {
                DataType::Boolean => visitor.visit_bool(
                    column,
                    column_index,
                    row_index,
                    as_boolean_array(column).value(row_index),
                ),
                DataType::Int8 => visitor.visit_i8(
                    column,
                    column_index,
                    row_index,
                    as_primitive_array::<Int8Type>(column).value(row_index),
                ),
                DataType::Int16 => visitor.visit_i16(
                    column,
                    column_index,
                    row_index,
                    as_primitive_array::<Int16Type>(column).value(row_index),
                ),
                DataType::Int32 => visitor.visit_i32(
                    column,
                    column_index,
                    row_index,
                    as_primitive_array::<Int32Type>(column).value(row_index),
                ),
                DataType::Int64 => visitor.visit_i64(
                    column,
                    column_index,
                    row_index,
                    as_primitive_array::<Int64Type>(column).value(row_index),
                ),
                DataType::UInt8 => visitor.visit_u8(
                    column,
                    column_index,
                    row_index,
                    as_primitive_array::<UInt8Type>(column).value(row_index),
                ),
                DataType::UInt16 => visitor.visit_u16(
                    column,
                    column_index,
                    row_index,
                    as_primitive_array::<UInt16Type>(column).value(row_index),
                ),
                DataType::UInt32 => visitor.visit_u32(
                    column,
                    column_index,
                    row_index,
                    as_primitive_array::<UInt32Type>(column).value(row_index),
                ),
                DataType::UInt64 => visitor.visit_u64(
                    column,
                    column_index,
                    row_index,
                    as_primitive_array::<UInt64Type>(column).value(row_index),
                ),
                DataType::Utf8 => visitor.visit_str(
                    column,
                    column_index,
                    row_index,
                    as_string_array(column).value(row_index),
                ),
                _ => ControlFlow::Continue(()),
            },
        )
    }

    pub fn visit_row_str<V>(&self, visitor: &mut V, row_index: usize) -> ControlFlow<V::Break>
    where
        V: TableVisitor,
    {
        self.batch().columns().iter().enumerate().try_for_each(
            |(column_index, column)| match column.data_type() {
                DataType::Utf8 => visitor.visit_str(
                    column,
                    column_index,
                    row_index,
                    as_string_array(column).value(row_index),
                ),
                _ => ControlFlow::Continue(()),
            },
        )
    }
}
