use std::sync::Arc;

use arrow_array::cast::*;
use arrow_array::*;
use arrow_schema::{FieldRef, SchemaRef};

use crate::ascii::{ascii_to_le_u8x32, le_u8x32_to_string};
use crate::error::FheSqlError;
use crate::uint::mask::ClearBoolMask;
use crate::uint::ClearByteArray;

#[inline]
pub fn arrow_shema_data_type_width(
    data_type: &arrow_schema::DataType,
) -> Result<usize, FheSqlError> {
    use arrow_schema::DataType;
    match data_type {
        DataType::Boolean => Ok(1),
        DataType::Int8 => Ok(1),
        DataType::Int16 => Ok(2),
        DataType::Int32 => Ok(4),
        DataType::Int64 => Ok(8),
        DataType::UInt8 => Ok(1),
        DataType::UInt16 => Ok(2),
        DataType::UInt32 => Ok(4),
        DataType::UInt64 => Ok(8),
        DataType::Utf8 => Ok(32),
        _ => Err(FheSqlError::unsupported_arrow_data_type(data_type)),
    }
}

#[inline]
pub fn array_column_cell_eq(column: &ArrayRef, row_index1: usize, row_index2: usize) -> bool {
    let data_type = column.data_type();
    match *data_type {
        arrow_schema::DataType::Boolean => {
            let a: &BooleanArray = as_boolean_array(&column);
            a.value(row_index1) == a.value(row_index2)
        }
        arrow_schema::DataType::Int8 => {
            let a: &Int8Array = as_primitive_array(&column);
            a.value(row_index1) == a.value(row_index2)
        }
        arrow_schema::DataType::Int16 => {
            let a: &Int16Array = as_primitive_array(&column);
            a.value(row_index1) == a.value(row_index2)
        }
        arrow_schema::DataType::Int32 => {
            let a: &Int32Array = as_primitive_array(&column);
            a.value(row_index1) == a.value(row_index2)
        }
        arrow_schema::DataType::Int64 => {
            let a: &Int64Array = as_primitive_array(&column);
            a.value(row_index1) == a.value(row_index2)
        }
        arrow_schema::DataType::UInt8 => {
            let a: &UInt8Array = as_primitive_array(&column);
            a.value(row_index1) == a.value(row_index2)
        }
        arrow_schema::DataType::UInt16 => {
            let a: &UInt16Array = as_primitive_array(&column);
            a.value(row_index1) == a.value(row_index2)
        }
        arrow_schema::DataType::UInt32 => {
            let a: &UInt32Array = as_primitive_array(&column);
            a.value(row_index1) == a.value(row_index2)
        }
        arrow_schema::DataType::UInt64 => {
            let a: &UInt64Array = as_primitive_array(&column);
            a.value(row_index1) == a.value(row_index2)
        }
        arrow_schema::DataType::Utf8 => {
            let a: &StringArray = as_string_array(&column);
            a.value(row_index1) == a.value(row_index2)
        }
        _ => todo!(),
    }
}

pub fn write_row_le_bytes(column: &ArrayRef, row_index: usize, buffer: &mut ClearByteArray) {
    // WARNING!!
    // u8_index can overflow!
    let data_type = column.data_type();

    match *data_type {
        arrow_schema::DataType::Boolean => {
            let a: &BooleanArray = as_boolean_array(&column);
            let b = a.value(row_index);
            if b {
                buffer.push(1_u8);
            } else {
                buffer.push(0_u8);
            }
        }
        arrow_schema::DataType::Int8 => {
            let a: &Int8Array = as_primitive_array(&column);
            a.value(row_index)
                .to_le_bytes()
                .iter()
                .for_each(|x| buffer.push(*x));
        }
        arrow_schema::DataType::Int16 => {
            let a: &Int16Array = as_primitive_array(&column);
            a.value(row_index)
                .to_le_bytes()
                .iter()
                .for_each(|x| buffer.push(*x));
        }
        arrow_schema::DataType::Int32 => {
            let a: &Int32Array = as_primitive_array(&column);
            a.value(row_index)
                .to_le_bytes()
                .iter()
                .for_each(|x| buffer.push(*x));
        }
        arrow_schema::DataType::Int64 => {
            let a: &Int64Array = as_primitive_array(&column);
            a.value(row_index)
                .to_le_bytes()
                .iter()
                .for_each(|x| buffer.push(*x));
        }
        arrow_schema::DataType::UInt8 => {
            let a: &UInt8Array = as_primitive_array(&column);
            a.value(row_index)
                .to_le_bytes()
                .iter()
                .for_each(|x| buffer.push(*x));
        }
        arrow_schema::DataType::UInt16 => {
            let a: &UInt16Array = as_primitive_array(&column);
            a.value(row_index)
                .to_le_bytes()
                .iter()
                .for_each(|x| buffer.push(*x));
        }
        arrow_schema::DataType::UInt32 => {
            let a: &UInt32Array = as_primitive_array(&column);
            a.value(row_index)
                .to_le_bytes()
                .iter()
                .for_each(|x| buffer.push(*x));
        }
        arrow_schema::DataType::UInt64 => {
            let a: &UInt64Array = as_primitive_array(&column);
            a.value(row_index)
                .to_le_bytes()
                .iter()
                .for_each(|x| buffer.push(*x));
        }
        arrow_schema::DataType::Utf8 => {
            let a: &StringArray = as_string_array(&column);
            ascii_to_le_u8x32(a.value(row_index))
                .iter()
                .for_each(|x| buffer.push(*x));
        }
        _ => todo!(),
    }
}

pub fn write_column_le_bytes(column: &ArrayRef, buffer: &mut ClearByteArray) {
    macro_rules! primite_array_to_le_byte {
        ($at:ident) => {
            let a: &$at = as_primitive_array(&column);
            a.iter().for_each(|o| {
                o.unwrap()
                    .to_le_bytes()
                    .iter()
                    .for_each(|x| buffer.push(*x));
            });
        };
    }

    let data_type = column.data_type();

    // push column width
    buffer.push(arrow_shema_data_type_width(data_type).unwrap() as u8);

    match *data_type {
        arrow_schema::DataType::Boolean => {
            let a: &BooleanArray = as_boolean_array(&column);
            a.iter().for_each(|b| {
                if b.unwrap() {
                    buffer.push(1_u8);
                } else {
                    buffer.push(0_u8);
                }
            });
        }
        arrow_schema::DataType::Int8 => {
            primite_array_to_le_byte!(Int8Array);
        }
        arrow_schema::DataType::Int16 => {
            primite_array_to_le_byte!(Int16Array);
        }
        arrow_schema::DataType::Int32 => {
            primite_array_to_le_byte!(Int32Array);
        }
        arrow_schema::DataType::Int64 => {
            primite_array_to_le_byte!(Int64Array);
        }
        arrow_schema::DataType::UInt8 => {
            primite_array_to_le_byte!(UInt8Array);
        }
        arrow_schema::DataType::UInt16 => {
            primite_array_to_le_byte!(UInt16Array);
        }
        arrow_schema::DataType::UInt32 => {
            primite_array_to_le_byte!(UInt32Array);
        }
        arrow_schema::DataType::UInt64 => {
            primite_array_to_le_byte!(UInt64Array);
        }
        arrow_schema::DataType::Utf8 => {
            let a: &StringArray = as_string_array(&column);
            a.iter().for_each(|s| {
                ascii_to_le_u8x32(s.unwrap())
                .iter()
                .for_each(|x| buffer.push(*x));
            });
        }
        _ => panic!("Invalid DataType"),
    }
}

pub fn array_read_columns_from_rows(
    schema_ref: &SchemaRef,
    field_mask: &ClearBoolMask,
    select_mask: &ClearBoolMask,
    byte_rows: Vec<ClearByteArray>,
) -> (Vec<ArrayRef>, Vec<FieldRef>) {
    let mut offset: usize = 0;
    let mut arrays: Vec<ArrayRef> = vec![];
    let mut out_fields: Vec<FieldRef> = vec![];

    let fields = &schema_ref.as_ref().fields;
    for i in 0..fields.len() {
        let f = &fields[i];

        match f.data_type() {
            arrow_schema::DataType::Boolean => {
                if field_mask.is_set(i) {
                    let v = read_column_from_rows_bool(offset, &byte_rows, select_mask);
                    arrays.push(Arc::new(BooleanArray::from(v)));
                    out_fields.push(f.clone());
                }
                offset += 1;
            }
            arrow_schema::DataType::Int8 => {
                if field_mask.is_set(i) {
                    let v = read_column_from_rows_i8(offset, &byte_rows, select_mask);
                    arrays.push(Arc::new(Int8Array::from(v)));
                    out_fields.push(f.clone());
                }
                offset += 1;
            }
            arrow_schema::DataType::Int16 => {
                if field_mask.is_set(i) {
                    let v = read_column_from_rows_i16(offset, &byte_rows, select_mask);
                    arrays.push(Arc::new(Int16Array::from(v)));
                    out_fields.push(f.clone());
                }
                offset += 2;
            }
            arrow_schema::DataType::Int32 => {
                if field_mask.is_set(i) {
                    let v = read_column_from_rows_i32(offset, &byte_rows, select_mask);
                    arrays.push(Arc::new(Int32Array::from(v)));
                    out_fields.push(f.clone());
                }
                offset += 4;
            }
            arrow_schema::DataType::Int64 => {
                if field_mask.is_set(i) {
                    let v = read_column_from_rows_i64(offset, &byte_rows, select_mask);
                    arrays.push(Arc::new(Int64Array::from(v)));
                    out_fields.push(f.clone());
                }
                offset += 8;
            }
            arrow_schema::DataType::UInt8 => {
                if field_mask.is_set(i) {
                    let v = read_column_from_rows_u8(offset, &byte_rows, select_mask);
                    arrays.push(Arc::new(UInt8Array::from(v)));
                    out_fields.push(f.clone());
                }
                offset += 1;
            }
            arrow_schema::DataType::UInt16 => {
                if field_mask.is_set(i) {
                    let v = read_column_from_rows_u16(offset, &byte_rows, select_mask);
                    arrays.push(Arc::new(UInt16Array::from(v)));
                    out_fields.push(f.clone());
                }
                offset += 2;
            }
            arrow_schema::DataType::UInt32 => {
                if field_mask.is_set(i) {
                    let v = read_column_from_rows_u32(offset, &byte_rows, select_mask);
                    arrays.push(Arc::new(UInt32Array::from(v)));
                    out_fields.push(f.clone());
                }
                offset += 4;
            }
            arrow_schema::DataType::UInt64 => {
                if field_mask.is_set(i) {
                    let v = read_column_from_rows_u64(offset, &byte_rows, select_mask);
                    arrays.push(Arc::new(UInt64Array::from(v)));
                    out_fields.push(f.clone());
                }
                offset += 8;
            }
            arrow_schema::DataType::Utf8 => {
                if field_mask.is_set(i) {
                    let v = read_column_from_rows_str(offset, &byte_rows, select_mask);
                    arrays.push(Arc::new(StringArray::from(v)));
                    out_fields.push(f.clone());
                }
                offset += 32;
            }
            _ => panic!("Invalid data type"),
        }
    }
    (arrays, out_fields)
}

pub fn read_column_from_rows_bool(
    offset: usize,
    byte_rows: &[ClearByteArray],
    select_mask: &ClearBoolMask,
) -> Vec<bool> {
    assert!(select_mask.len() >= byte_rows.len());
    byte_rows
        .iter()
        .enumerate()
        .filter(|(row_index, row)| select_mask.is_set(*row_index) && offset < row.len())
        .map(|(_, row)| {
            let b = u8::from_le_bytes([row.get(offset)]);
            b != 0
        })
        .collect()
}

pub fn read_column_from_rows_i8(offset: usize, byte_rows: &[ClearByteArray], select_mask: &ClearBoolMask) -> Vec<i8> {
    assert!(select_mask.len() >= byte_rows.len());
    byte_rows
        .iter()
        .enumerate()
        .filter(|(row_index, row)| select_mask.is_set(*row_index) && offset < row.len())
        .map(|(_, row)| i8::from_le_bytes([row.get(offset)]))
        .collect()
}

pub fn read_column_from_rows_i16(
    offset: usize,
    byte_rows: &[ClearByteArray],
    select_mask: &ClearBoolMask,
) -> Vec<i16> {
    assert!(select_mask.len() >= byte_rows.len());
    byte_rows
        .iter()
        .enumerate()
        .filter(|(row_index, row)| select_mask.is_set(*row_index) && offset < row.len())
        .map(|(_, row)| i16::from_le_bytes([row.get(offset), row.get(offset + 1)]))
        .collect()
}

pub fn read_column_from_rows_i32(
    offset: usize,
    byte_rows: &[ClearByteArray],
    select_mask: &ClearBoolMask,
) -> Vec<i32> {
    assert!(select_mask.len() >= byte_rows.len());
    byte_rows
        .iter()
        .enumerate()
        .filter(|(row_index, row)| select_mask.is_set(*row_index) && offset < row.len())
        .map(|(_, row)| {
            i32::from_le_bytes([
                row.get(offset),
                row.get(offset + 1),
                row.get(offset + 2),
                row.get(offset + 3),
            ])
        })
        .collect()
}

pub fn read_column_from_rows_i64(
    offset: usize,
    byte_rows: &[ClearByteArray],
    select_mask: &ClearBoolMask,
) -> Vec<i64> {
    assert!(select_mask.len() >= byte_rows.len());
    byte_rows
        .iter()
        .enumerate()
        .filter(|(row_index, row)| select_mask.is_set(*row_index) && offset < row.len())
        .map(|(_, row)| {
            i64::from_le_bytes([
                row.get(offset),
                row.get(offset + 1),
                row.get(offset + 2),
                row.get(offset + 3),
                row.get(offset + 4),
                row.get(offset + 5),
                row.get(offset + 6),
                row.get(offset + 7),
            ])
        })
        .collect()
}

pub fn read_column_from_rows_u8(offset: usize, byte_rows: &[ClearByteArray], select_mask: &ClearBoolMask) -> Vec<u8> {
    assert!(select_mask.len() >= byte_rows.len());
    byte_rows
        .iter()
        .enumerate()
        .filter(|(row_index, row)| select_mask.is_set(*row_index) && offset < row.len())
        .map(|(_, row)| u8::from_le_bytes([row.get(offset)]))
        .collect()
}

pub fn read_column_from_rows_u16(
    offset: usize,
    byte_rows: &[ClearByteArray],
    select_mask: &ClearBoolMask,
) -> Vec<u16> {
    assert!(select_mask.len() >= byte_rows.len());
    byte_rows
        .iter()
        .enumerate()
        .filter(|(row_index, row)| select_mask.is_set(*row_index) && offset < row.len())
        .map(|(_, row)| u16::from_le_bytes([row.get(offset), row.get(offset + 1)]))
        .collect()
}

pub fn read_column_from_rows_u32(
    offset: usize,
    byte_rows: &[ClearByteArray],
    select_mask: &ClearBoolMask,
) -> Vec<u32> {
    assert!(select_mask.len() >= byte_rows.len());
    byte_rows
        .iter()
        .enumerate()
        .filter(|(row_index, row)| select_mask.is_set(*row_index) && offset < row.len())
        .map(|(_, row)| {
            u32::from_le_bytes([
                row.get(offset),
                row.get(offset + 1),
                row.get(offset + 2),
                row.get(offset + 3),
            ])
        })
        .collect()
}

pub fn read_column_from_rows_u64(
    offset: usize,
    byte_rows: &[ClearByteArray],
    select_mask: &ClearBoolMask,
) -> Vec<u64> {
    assert!(select_mask.len() >= byte_rows.len());
    byte_rows
        .iter()
        .enumerate()
        .filter(|(row_index, row)| select_mask.is_set(*row_index) && offset < row.len())
        .map(|(_, row)| {
            u64::from_le_bytes([
                row.get(offset),
                row.get(offset + 1),
                row.get(offset + 2),
                row.get(offset + 3),
                row.get(offset + 4),
                row.get(offset + 5),
                row.get(offset + 6),
                row.get(offset + 7),
            ])
        })
        .collect()
}

pub fn read_column_from_rows_str(
    offset: usize,
    byte_rows: &[ClearByteArray],
    select_mask: &ClearBoolMask,
) -> Vec<String> {
    assert!(select_mask.len() >= byte_rows.len());
    byte_rows
        .iter()
        .enumerate()
        .filter(|(row_index, row)| select_mask.is_set(*row_index) && offset < row.len())
        .map(|(_, row)| {
            let u8x32: [u8; 32] = std::array::from_fn(|i| row.get(offset + i));
            le_u8x32_to_string(&u8x32)
        })
        .collect()
}

pub fn array_read_columns_from_columns(
    schema_ref: &SchemaRef,
    field_mask: &ClearBoolMask,
    select_mask: &ClearBoolMask,
    mut byte_columns: Vec<ClearByteArray>,
) -> (Vec<ArrayRef>, Vec<FieldRef>) {
    let mut arrays: Vec<ArrayRef> = vec![];
    let mut out_fields: Vec<FieldRef> = vec![];

    let fields = &schema_ref.as_ref().fields;
    for i in 0..fields.len() {
        let f = &fields[i];

        match f.data_type() {
            arrow_schema::DataType::Boolean => {
                if field_mask.is_set(i) {
                    let v = std::mem::take(&mut byte_columns[i]).into_bool_vec(select_mask);
                    arrays.push(Arc::new(BooleanArray::from(v)));
                    out_fields.push(f.clone());
                }
            }
            arrow_schema::DataType::Int8 => {
                if field_mask.is_set(i) {
                    let v = std::mem::take(&mut byte_columns[i]).into_i8_vec(select_mask);
                    arrays.push(Arc::new(Int8Array::from(v)));
                    out_fields.push(f.clone());
                }
            }
            arrow_schema::DataType::Int16 => {
                if field_mask.is_set(i) {
                    let v = std::mem::take(&mut byte_columns[i]).into_i16_vec(select_mask);
                    arrays.push(Arc::new(Int16Array::from(v)));
                    out_fields.push(f.clone());
                }
            }
            arrow_schema::DataType::Int32 => {
                if field_mask.is_set(i) {
                    let v = std::mem::take(&mut byte_columns[i]).into_i32_vec(select_mask);
                    arrays.push(Arc::new(Int32Array::from(v)));
                    out_fields.push(f.clone());
                }
            }
            arrow_schema::DataType::Int64 => {
                if field_mask.is_set(i) {
                    let v = std::mem::take(&mut byte_columns[i]).into_i64_vec(select_mask);
                    arrays.push(Arc::new(Int64Array::from(v)));
                    out_fields.push(f.clone());
                }
            }
            arrow_schema::DataType::UInt8 => {
                if field_mask.is_set(i) {
                    let v = std::mem::take(&mut byte_columns[i]).into_u8_vec(select_mask);
                    arrays.push(Arc::new(UInt8Array::from(v)));
                    out_fields.push(f.clone());
                }
            }
            arrow_schema::DataType::UInt16 => {
                if field_mask.is_set(i) {
                    let v = std::mem::take(&mut byte_columns[i]).into_u16_vec(select_mask);
                    arrays.push(Arc::new(UInt16Array::from(v)));
                    out_fields.push(f.clone());
                }
            }
            arrow_schema::DataType::UInt32 => {
                if field_mask.is_set(i) {
                    let v = std::mem::take(&mut byte_columns[i]).into_u32_vec(select_mask);
                    arrays.push(Arc::new(UInt32Array::from(v)));
                    out_fields.push(f.clone());
                }
            }
            arrow_schema::DataType::UInt64 => {
                if field_mask.is_set(i) {
                    let v = std::mem::take(&mut byte_columns[i]).into_u64_vec(select_mask);
                    arrays.push(Arc::new(UInt64Array::from(v)));
                    out_fields.push(f.clone());
                }
            }
            arrow_schema::DataType::Utf8 => {
                if field_mask.is_set(i) {
                    let v = std::mem::take(&mut byte_columns[i]).into_ascii32_vec(select_mask);
                    arrays.push(Arc::new(StringArray::from(v)));
                    out_fields.push(f.clone());
                }
            }
            _ => panic!("Invalid data type"),
        }
    }
    (arrays, out_fields)
}
