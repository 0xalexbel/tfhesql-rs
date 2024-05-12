use arrow_array::cast::*;
use arrow_array::*;
use arrow_schema::DataType;

use crate::uint::block::*;
use crate::uint::iter::*;
use crate::uint::traits::*;

// use crate::ascii::ascii_chars_to_pos_words;
use crate::ascii::ascii_to_le_u64x4;
use crate::OrderedTables;
use crate::Table;

////////////////////////////////////////////////////////////////////////////////

macro_rules! next_column {
    ($s:tt) => {
        $s.index = 0;
        $s.block_index = 0;
        $s.column_index += 1;
    };
}

macro_rules! next_row {
    ($s:tt) => {
        $s.index += 1;
        $s.block_index = 0;
    };
}

macro_rules! continue_and_goto_next_column_if_ascii {
    ($s:tt) => {
        if $s.is_ascii {
            next_column!($s);
            continue;
        }
    };
}

macro_rules! continue_and_goto_next_column_if_not_ascii {
    ($s:tt) => {
        if !$s.is_ascii {
            next_column!($s);
            continue;
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
// LeU16BlockIterator
////////////////////////////////////////////////////////////////////////////////

impl LeU16BlockIterator for Table {
    fn iter_le_u16(&self) -> impl Iterator<Item = U16Block> {
        TableLeU16BlockIterator::new_num(self)
    }
}

impl AsciiU16BlockIterator for Table {
    fn iter_ascii_u16(&self, block_offset: UIntBlockIndex) -> impl Iterator<Item = U16Block> {
        TableLeU16BlockIterator::new_ascii(self, block_offset)
    }
}

impl LeU16BlockIterator for OrderedTables {
    fn iter_le_u16(&self) -> impl Iterator<Item = U16Block> {
        self.tables.iter().flat_map(move |t| t.iter_le_u16())
    }
}

impl AsciiU16BlockIterator for OrderedTables {
    fn iter_ascii_u16(&self, block_offset: UIntBlockIndex) -> impl Iterator<Item = U16Block> {
        self.tables
            .iter()
            .flat_map(move |t| t.iter_ascii_u16(block_offset))
    }
}

pub struct TableLeU16BlockIterator<'a> {
    table: &'a Table,
    column_index: usize,
    index: usize,
    block_index: UIntBlockIndex,
    block_offset: UIntBlockIndex,
    ascii_u64x4: Option<[u64; 4]>,
    is_ascii: bool,
}

impl<'a> TableLeU16BlockIterator<'a> {
    fn new_num(table: &'a Table) -> Self {
        TableLeU16BlockIterator {
            table,
            column_index: 0,
            index: 0,
            block_index: 0,
            ascii_u64x4: None,
            block_offset: 0,
            is_ascii: false,
        }
    }
    fn new_ascii(table: &'a Table, block_offset: UIntBlockIndex) -> Self {
        assert!(block_offset < 4);
        TableLeU16BlockIterator {
            table,
            column_index: 0,
            index: 0,
            block_index: 0,
            ascii_u64x4: None,
            block_offset,
            is_ascii: true,
        }
    }
}

impl<'a> Iterator for TableLeU16BlockIterator<'a> {
    type Item = U16Block;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.table.batch();
        let num_columns = batch.num_columns();

        while self.column_index < num_columns {
            let column_ref = batch.column(self.column_index);
            let column_len = column_ref.as_ref().len();

            if self.index >= column_len {
                next_column!(self);
                continue;
            }

            let data_type = column_ref.as_ref().data_type();
            let result: Option<Self::Item>;
            match *data_type {
                DataType::Int16 => {
                    continue_and_goto_next_column_if_ascii!(self);
                    let a: &Int16Array = as_primitive_array(column_ref.as_ref());
                    let v = a.value(self.index).unsigned_abs();
                    result = Some(v.le_u16_block(0));
                    next_row!(self);
                }
                DataType::UInt16 => {
                    continue_and_goto_next_column_if_ascii!(self);
                    let a: &UInt16Array = as_primitive_array(column_ref.as_ref());
                    let v = a.value(self.index);
                    result = Some(v.le_u16_block(0));
                    next_row!(self);
                }
                DataType::Int32 => {
                    continue_and_goto_next_column_if_ascii!(self);
                    let a: &Int32Array = as_primitive_array(column_ref.as_ref());
                    let v = a.value(self.index).unsigned_abs();
                    result = Some(v.le_u16_block(self.block_index));
                    self.block_index += 1;
                    if self.block_index == (u32::BITS / u16::BITS) as UIntBlockIndex {
                        next_row!(self);
                    }
                }
                DataType::UInt32 => {
                    continue_and_goto_next_column_if_ascii!(self);
                    let a: &UInt32Array = as_primitive_array(column_ref.as_ref());
                    let v = a.value(self.index);
                    result = Some(v.le_u16_block(self.block_index));
                    self.block_index += 1;
                    if self.block_index == (u32::BITS / u16::BITS) as UIntBlockIndex {
                        next_row!(self);
                    }
                }
                DataType::Int64 => {
                    continue_and_goto_next_column_if_ascii!(self);
                    let a: &Int64Array = as_primitive_array(column_ref.as_ref());
                    let v = a.value(self.index).unsigned_abs();
                    result = Some(v.le_u16_block(self.block_index));
                    self.block_index += 1;
                    if self.block_index == (u64::BITS / u16::BITS) as u8 {
                        next_row!(self);
                    }
                }
                DataType::UInt64 => {
                    continue_and_goto_next_column_if_ascii!(self);
                    let a: &UInt64Array = as_primitive_array(column_ref.as_ref());
                    let v = a.value(self.index);
                    result = Some(v.le_u16_block(self.block_index));
                    self.block_index += 1;
                    if self.block_index == (u64::BITS / u16::BITS) as UIntBlockIndex {
                        next_row!(self);
                    }
                }
                DataType::Utf8 => {
                    continue_and_goto_next_column_if_not_ascii!(self);
                    let b: U16Block;
                    match self.ascii_u64x4 {
                        Some(u64x4) => {
                            assert!(self.block_index > 0);
                            assert!(self.block_index < ((u64::BITS / u16::BITS) as UIntBlockIndex));
                            // b can be ZERO!
                            b = u64x4[self.block_offset as usize].le_u16_block(self.block_index);
                        }
                        None => {
                            assert_eq!(self.block_index, 0);
                            let a = as_string_array(column_ref.as_ref());
                            let s = a.value(self.index);
                            let u64x4 = ascii_to_le_u64x4(s);
                            self.ascii_u64x4 = Some(u64x4);
                            // b can be ZERO!, empty string
                            b = u64x4[self.block_offset as usize].le_u16_block(self.block_index);
                        }
                    }

                    // Stop if end of string
                    let b_u8 = blk_value!(b).to_le_bytes();
                    if b_u8[0] == 0 || b_u8[1] == 0 {
                        self.ascii_u64x4 = None;
                        next_row!(self);
                        continue;
                    }

                    result = Some(b);

                    self.block_index += 1;
                    if self.block_index == (u64::BITS / u16::BITS) as UIntBlockIndex {
                        self.ascii_u64x4 = None;
                        next_row!(self);
                    }
                }
                _ => {
                    next_column!(self);
                    continue;
                }
            }
            return result;
        }
        None
    }
}

////////////////////////////////////////////////////////////////////////////////
// LeU32BlockIterator
////////////////////////////////////////////////////////////////////////////////

impl LeU32BlockIterator for Table {
    fn iter_le_u32(&self) -> impl Iterator<Item = U32Block> {
        TableLeU32BlockIterator::new_num(self)
    }
}

impl AsciiU32BlockIterator for Table {
    fn iter_ascii_u32(&self, block_offset: UIntBlockIndex) -> impl Iterator<Item = U32Block> {
        TableLeU32BlockIterator::new_ascii(self, block_offset)
    }
}

impl LeU32BlockIterator for OrderedTables {
    fn iter_le_u32(&self) -> impl Iterator<Item = U32Block> {
        self.tables.iter().flat_map(move |t| t.iter_le_u32())
    }
}

impl AsciiU32BlockIterator for OrderedTables {
    fn iter_ascii_u32(&self, block_offset: UIntBlockIndex) -> impl Iterator<Item = U32Block> {
        self.tables
            .iter()
            .flat_map(move |t| t.iter_ascii_u32(block_offset))
    }
}

pub struct TableLeU32BlockIterator<'a> {
    table: &'a Table,
    column_index: usize,
    index: usize,
    block_index: UIntBlockIndex,
    block_offset: UIntBlockIndex,
    ascii_u64x4: Option<[u64; 4]>,
    is_ascii: bool,
}

impl<'a> TableLeU32BlockIterator<'a> {
    fn new_num(table: &'a Table) -> Self {
        TableLeU32BlockIterator {
            table,
            column_index: 0,
            index: 0,
            block_index: 0,
            block_offset: 0,
            ascii_u64x4: None,
            is_ascii: false,
        }
    }
    fn new_ascii(table: &'a Table, block_offset: UIntBlockIndex) -> Self {
        TableLeU32BlockIterator {
            table,
            column_index: 0,
            index: 0,
            block_index: 0,
            block_offset,
            ascii_u64x4: None,
            is_ascii: true,
        }
    }
}

impl<'a> Iterator for TableLeU32BlockIterator<'a> {
    type Item = U32Block;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.table.batch();
        let num_columns = batch.num_columns();

        while self.column_index < num_columns {
            let column_ref = batch.column(self.column_index);
            let column_len = column_ref.as_ref().len();

            if self.index >= column_len {
                next_column!(self);
                continue;
            }

            let data_type = column_ref.as_ref().data_type();
            let result: Option<Self::Item>;
            match *data_type {
                DataType::Int32 => {
                    continue_and_goto_next_column_if_ascii!(self);
                    let a: &Int32Array = as_primitive_array(column_ref.as_ref());
                    let v = a.value(self.index).unsigned_abs();
                    result = Some(v.le_u32_block(self.block_index));
                    self.block_index += 1;
                    if self.block_index == 1 {
                        next_row!(self);
                    }
                }
                DataType::UInt32 => {
                    continue_and_goto_next_column_if_ascii!(self);
                    let a: &UInt32Array = as_primitive_array(column_ref.as_ref());
                    let v = a.value(self.index);
                    result = Some(v.le_u32_block(self.block_index));
                    self.block_index += 1;
                    if self.block_index == 1 {
                        next_row!(self);
                    }
                }
                DataType::Int64 => {
                    continue_and_goto_next_column_if_ascii!(self);
                    let a: &Int64Array = as_primitive_array(column_ref.as_ref());
                    let v = a.value(self.index).unsigned_abs();
                    result = Some(v.le_u32_block(self.block_index));
                    self.block_index += 1;
                    if self.block_index == (u64::BITS / u32::BITS) as u8 {
                        next_row!(self);
                    }
                }
                DataType::UInt64 => {
                    continue_and_goto_next_column_if_ascii!(self);
                    let a: &UInt64Array = as_primitive_array(column_ref.as_ref());
                    let v = a.value(self.index);
                    result = Some(v.le_u32_block(self.block_index));
                    self.block_index += 1;
                    if self.block_index == (u64::BITS / u32::BITS) as UIntBlockIndex {
                        next_row!(self);
                    }
                }
                DataType::Utf8 => {
                    continue_and_goto_next_column_if_not_ascii!(self);
                    let b: U32Block;
                    match self.ascii_u64x4 {
                        Some(u64x4) => {
                            assert!(self.block_index > 0);
                            assert!(self.block_index < ((u64::BITS / u32::BITS) as UIntBlockIndex));
                            // b can be ZERO!
                            b = u64x4[self.block_offset as usize].le_u32_block(self.block_index);
                        }
                        None => {
                            assert_eq!(self.block_index, 0);
                            let a = as_string_array(column_ref.as_ref());
                            let s = a.value(self.index);
                            let u64x4 = ascii_to_le_u64x4(s);
                            self.ascii_u64x4 = Some(u64x4);
                            // b can be ZERO!, empty string
                            b = u64x4[self.block_offset as usize].le_u32_block(self.block_index);
                        }
                    }

                    // Stop if end of string
                    let b_u8 = blk_value!(b).to_le_bytes();
                    if b_u8[0] == 0 || b_u8[1] == 0 || b_u8[2] == 0 || b_u8[3] == 0 {
                        if b_u8[1] != 0 {
                            assert_ne!(b_u8[0], 0);
                        }
                        if b_u8[2] != 0 {
                            assert_ne!(b_u8[1], 0);
                            assert_ne!(b_u8[0], 0);
                        }
                        if b_u8[3] != 0 {
                            assert_ne!(b_u8[2], 0);
                            assert_ne!(b_u8[1], 0);
                            assert_ne!(b_u8[0], 0);
                        }
                        self.ascii_u64x4 = None;
                        next_row!(self);
                        continue;
                    }

                    result = Some(b);

                    self.block_index += 1;
                    if self.block_index == (u64::BITS / u32::BITS) as UIntBlockIndex {
                        self.ascii_u64x4 = None;
                        next_row!(self);
                    }
                }
                _ => {
                    next_column!(self);
                    continue;
                }
            }
            return result;
        }
        None
    }
}

////////////////////////////////////////////////////////////////////////////////
// LeU64BlockIterator
////////////////////////////////////////////////////////////////////////////////

impl LeU64BlockIterator for Table {
    fn iter_le_u64(&self) -> impl Iterator<Item = U64Block> {
        TableLeU64BlockIterator::new_num(self)
    }
}

impl AsciiU64BlockIterator for Table {
    fn iter_ascii_u64(&self, block_offset: UIntBlockIndex) -> impl Iterator<Item = U64Block> {
        TableLeU64BlockIterator::new_ascii(self, block_offset)
    }
}

impl LeU64BlockIterator for OrderedTables {
    fn iter_le_u64(&self) -> impl Iterator<Item = U64Block> {
        self.tables.iter().flat_map(move |t| t.iter_le_u64())
    }
}

impl AsciiU64BlockIterator for OrderedTables {
    fn iter_ascii_u64(&self, block_offset: UIntBlockIndex) -> impl Iterator<Item = U64Block> {
        self.tables
            .iter()
            .flat_map(move |t| t.iter_ascii_u64(block_offset))
    }
}

pub struct TableLeU64BlockIterator<'a> {
    table: &'a Table,
    column_index: usize,
    index: usize,
    block_index: UIntBlockIndex,
    block_offset: UIntBlockIndex,
    is_ascii: bool,
}

impl<'a> TableLeU64BlockIterator<'a> {
    fn new_num(table: &'a Table) -> Self {
        TableLeU64BlockIterator {
            table,
            column_index: 0,
            index: 0,
            block_index: 0,
            block_offset: 0,
            is_ascii: false,
        }
    }

    fn new_ascii(table: &'a Table, block_offset: UIntBlockIndex) -> Self {
        TableLeU64BlockIterator {
            table,
            column_index: 0,
            index: 0,
            block_index: 0,
            block_offset,
            is_ascii: true,
        }
    }
}

impl<'a> Iterator for TableLeU64BlockIterator<'a> {
    type Item = U64Block;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.table.batch();
        let num_columns = batch.num_columns();

        while self.column_index < num_columns {
            let column_ref = batch.column(self.column_index);
            let column_len = column_ref.as_ref().len();

            if self.index >= column_len {
                next_column!(self);
                continue;
            }

            let data_type = column_ref.as_ref().data_type();
            let result: Option<Self::Item>;
            match *data_type {
                DataType::Int64 => {
                    continue_and_goto_next_column_if_ascii!(self);
                    let a: &Int64Array = as_primitive_array(column_ref.as_ref());
                    let v = a.value(self.index).unsigned_abs();
                    result = Some(v.le_u64_block(self.block_index));
                    self.block_index += 1;
                    if self.block_index == 1 {
                        next_row!(self);
                    }
                }
                DataType::UInt64 => {
                    continue_and_goto_next_column_if_ascii!(self);
                    let a: &UInt64Array = as_primitive_array(column_ref.as_ref());
                    let v = a.value(self.index);
                    result = Some(v.le_u64_block(self.block_index));
                    self.block_index += 1;
                    if self.block_index == 1 {
                        next_row!(self);
                    }
                }
                DataType::Utf8 => {
                    continue_and_goto_next_column_if_not_ascii!(self);
                    assert_eq!(self.block_index, 0);
                    let a = as_string_array(column_ref.as_ref());
                    let s = a.value(self.index);
                    let u64x4 = ascii_to_le_u64x4(s);
                    let b: U64Block = (u64x4[self.block_offset as usize], 0);

                    let b_u8 = blk_value!(b).to_le_bytes();
                    if b_u8[0] == 0
                        || b_u8[1] == 0
                        || b_u8[2] == 0
                        || b_u8[3] == 0
                        || b_u8[4] == 0
                        || b_u8[5] == 0
                        || b_u8[6] == 0
                        || b_u8[7] == 0
                    {
                        if b_u8[1] != 0 {
                            assert_ne!(b_u8[0], 0);
                        }
                        if b_u8[2] != 0 {
                            assert_ne!(b_u8[1], 0);
                            assert_ne!(b_u8[0], 0);
                        }
                        if b_u8[3] != 0 {
                            assert_ne!(b_u8[2], 0);
                            assert_ne!(b_u8[1], 0);
                            assert_ne!(b_u8[0], 0);
                        }
                        if b_u8[4] != 0 {
                            assert_ne!(b_u8[3], 0);
                            assert_ne!(b_u8[2], 0);
                            assert_ne!(b_u8[1], 0);
                            assert_ne!(b_u8[0], 0);
                        }
                        if b_u8[5] != 0 {
                            assert_ne!(b_u8[4], 0);
                            assert_ne!(b_u8[3], 0);
                            assert_ne!(b_u8[2], 0);
                            assert_ne!(b_u8[1], 0);
                            assert_ne!(b_u8[0], 0);
                        }
                        if b_u8[6] != 0 {
                            assert_ne!(b_u8[5], 0);
                            assert_ne!(b_u8[4], 0);
                            assert_ne!(b_u8[3], 0);
                            assert_ne!(b_u8[2], 0);
                            assert_ne!(b_u8[1], 0);
                            assert_ne!(b_u8[0], 0);
                        }
                        if b_u8[7] != 0 {
                            assert_ne!(b_u8[6], 0);
                            assert_ne!(b_u8[5], 0);
                            assert_ne!(b_u8[4], 0);
                            assert_ne!(b_u8[3], 0);
                            assert_ne!(b_u8[2], 0);
                            assert_ne!(b_u8[1], 0);
                            assert_ne!(b_u8[0], 0);
                        }

                        next_row!(self);
                        continue;
                    }

                    result = Some(b);
                    next_row!(self);
                }
                _ => {
                    next_column!(self);
                    continue;
                }
            }
            return result;
        }
        None
    }
}

////////////////////////////////////////////////////////////////////////////////
// LeU64x4Iterator
////////////////////////////////////////////////////////////////////////////////

impl LeU64x4Iterator for Table {
    fn iter_le_u64x4(&self) -> impl Iterator<Item = U64x4> {
        TableLeU64x4Iterator::new(self)
    }
}

impl LeU64x4Iterator for OrderedTables {
    fn iter_le_u64x4(&self) -> impl Iterator<Item = U64x4> {
        self.tables.iter().flat_map(|t| t.iter_le_u64x4())
    }
}

pub struct TableLeU64x4Iterator<'a> {
    table: &'a Table,
    column_index: usize,
    index: usize,
}

impl<'a> TableLeU64x4Iterator<'a> {
    fn new(table: &'a Table) -> Self {
        TableLeU64x4Iterator {
            table,
            column_index: 0,
            index: 0,
        }
    }
}

impl<'a> Iterator for TableLeU64x4Iterator<'a> {
    type Item = U64x4;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.table.batch();
        let num_columns = batch.num_columns();

        macro_rules! next_column {
            ($s:tt) => {
                $s.index = 0;
                $s.column_index += 1;
            };
        }
        macro_rules! next_row {
            ($s:tt) => {
                $s.index += 1;
            };
        }

        while self.column_index < num_columns {
            let column_ref = batch.column(self.column_index);
            let column_len = column_ref.as_ref().len();

            if self.index >= column_len {
                next_column!(self);
                continue;
            }

            let data_type = column_ref.as_ref().data_type();
            match data_type {
                DataType::Utf8 => {
                    let a: &StringArray = as_string_array(column_ref.as_ref());
                    let v = a.value(self.index);
                    next_row!(self);
                    return Some(ascii_to_le_u64x4(v));
                }
                _ => {
                    next_column!(self);
                    continue;
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    use crate::ascii::ascii_to_le_u64x4;
    use crate::table::block_iter::*;
    use crate::Table;
    use arrow_schema::*;
    use std::sync::Arc;

    #[test]
    fn test_u16_str() {
        let schema = Schema::new(vec![Field::new("String1", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from(vec![
                "ab",
                "cde",
                "fghi",
                "abcdefghijklmnopqrstuvwxyzABCDEF",
            ]))],
        )
        .unwrap();

        let w = "ab".as_bytes();

        println!("0={} (97='a')", w[0]);
        println!("1={} (98='b')", w[1]);

        let v = ascii_to_le_u64x4("ab");
        let d1 = u64::from_le_bytes([97, 98, 0, 0, 0, 0, 0, 0]);
        let d2 = u64::from_le_bytes([w[0], w[1], 0, 0, 0, 0, 0, 0]);
        assert_eq!(d1, d2);
        assert_eq!(v[0], d1);
        assert_eq!(v[1], 0);
        assert_eq!(v[2], 0);
        assert_eq!(v[3], 0);

        let t1 = Table::new("table1", batch);
        let mut iter = TableLeU16BlockIterator::new_ascii(&t1, 0);

        //ab
        let ab = u16::from_le_bytes([97, 98]);
        assert_eq!(iter.next().unwrap(), (ab, 0));

        //cd
        let cd = u16::from_le_bytes([99, 100]);
        assert_eq!(iter.next().unwrap(), (cd, 0));
        
        // e
        // is ignored

        //fg
        let fg = u16::from_le_bytes([102, 103]);
        assert_eq!(iter.next().unwrap(), (fg, 0));
        //hi
        let hi = u16::from_le_bytes([104, 105]);
        assert_eq!(iter.next().unwrap(), (hi, 1));

        //ab
        let ab = u16::from_le_bytes([97, 98]);
        assert_eq!(iter.next().unwrap(), (ab, 0));
        //cd
        let cd = u16::from_le_bytes([99, 100]);
        assert_eq!(iter.next().unwrap(), (cd, 1));
        //ef
        let ef = u16::from_le_bytes([101, 102]);
        assert_eq!(iter.next().unwrap(), (ef, 2));
        //gh
        let gh = u16::from_le_bytes([103, 104]);
        assert_eq!(iter.next().unwrap(), (gh, 3));
        assert!(iter.next().is_none());
    }
}
