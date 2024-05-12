use super::Table;
use crate::uint::triangular_matrix::TriangularMatrix;
use arrow_array::cast::*;
use arrow_array::types::*;
use arrow_array::*;

/// [
///    (0,0), (0,1), (0,2), (0,3) ...   (0, p-1)  -> p items
///           (1,1), (1,2), (1,3) ...   (1, p-1)  -> p-1 items
///                  (2,2), (2,3) ...   (2, p-1)  -> p-2 items
///                         (3,3) ...   (3, p-1)  -> p-3 items
///                                .
///                                .
///                                .
///                       (p-2, p-1), (p-2, p-1) -> 2 items
///                                   (p-1, p-1) -> 1 item  
/// ]
pub fn table_row_col_cmp_col(table: &Table, row_index: usize) -> TriangularMatrix<[bool; 3]> {
    let mut col_cmp_col: Vec<[bool; 3]> = vec![];
    table
        .batch()
        .columns()
        .iter()
        .enumerate()
        .for_each(|(left_index, left_column)| {
            col_cmp_col.push([true, false, false]);
            let left_type = left_column.data_type();
            match left_type {
                arrow_schema::DataType::Boolean => {
                    let left_value = as_boolean_array(left_column).value(row_index);
                    table
                        .batch()
                        .columns()
                        .iter()
                        .enumerate()
                        .skip(left_index + 1)
                        .for_each(|(right_index, right_col)| {
                            let v = cmp_bool_value(
                                row_index,
                                left_index,
                                left_value,
                                right_col,
                                right_index,
                            );
                            col_cmp_col.push(v);
                        })
                }
                arrow_schema::DataType::Int8 => {
                    let left_value = as_primitive_array::<Int8Type>(left_column).value(row_index);
                    table
                        .batch()
                        .columns()
                        .iter()
                        .enumerate()
                        .skip(left_index + 1)
                        .for_each(|(right_index, right_col)| {
                            let v = cmp_i8_value(
                                row_index,
                                left_index,
                                left_value,
                                right_col,
                                right_index,
                            );
                            col_cmp_col.push(v);
                        })
                }
                arrow_schema::DataType::Int16 => {
                    let left_value = as_primitive_array::<Int16Type>(left_column).value(row_index);
                    table
                        .batch()
                        .columns()
                        .iter()
                        .enumerate()
                        .skip(left_index + 1)
                        .for_each(|(right_index, right_col)| {
                            let v = cmp_i16_value(
                                row_index,
                                left_index,
                                left_value,
                                right_col,
                                right_index,
                            );
                            col_cmp_col.push(v);
                        })
                }
                arrow_schema::DataType::Int32 => {
                    let left_value = as_primitive_array::<Int32Type>(left_column).value(row_index);
                    table
                        .batch()
                        .columns()
                        .iter()
                        .enumerate()
                        .skip(left_index + 1)
                        .for_each(|(right_index, right_col)| {
                            let v = cmp_i32_value(
                                row_index,
                                left_index,
                                left_value,
                                right_col,
                                right_index,
                            );
                            col_cmp_col.push(v);
                        })
                }
                arrow_schema::DataType::Int64 => {
                    let left_value = as_primitive_array::<Int64Type>(left_column).value(row_index);
                    table
                        .batch()
                        .columns()
                        .iter()
                        .enumerate()
                        .skip(left_index + 1)
                        .for_each(|(right_index, right_col)| {
                            let v = cmp_i64_value(
                                row_index,
                                left_index,
                                left_value,
                                right_col,
                                right_index,
                            );
                            col_cmp_col.push(v);
                        })
                }
                arrow_schema::DataType::UInt8 => {
                    let left_value = as_primitive_array::<UInt8Type>(left_column).value(row_index);
                    table
                        .batch()
                        .columns()
                        .iter()
                        .enumerate()
                        .skip(left_index + 1)
                        .for_each(|(right_index, right_col)| {
                            let v = cmp_u8_value(
                                row_index,
                                left_index,
                                left_value,
                                right_col,
                                right_index,
                            );
                            col_cmp_col.push(v);
                        })
                }
                arrow_schema::DataType::UInt16 => {
                    let left_value = as_primitive_array::<UInt16Type>(left_column).value(row_index);
                    table
                        .batch()
                        .columns()
                        .iter()
                        .enumerate()
                        .skip(left_index + 1)
                        .for_each(|(right_index, right_col)| {
                            let v = cmp_u16_value(
                                row_index,
                                left_index,
                                left_value,
                                right_col,
                                right_index,
                            );
                            col_cmp_col.push(v);
                        })
                }
                arrow_schema::DataType::UInt32 => {
                    let left_value = as_primitive_array::<UInt32Type>(left_column).value(row_index);
                    table
                        .batch()
                        .columns()
                        .iter()
                        .enumerate()
                        .skip(left_index + 1)
                        .for_each(|(right_index, right_col)| {
                            let v = cmp_u32_value(
                                row_index,
                                left_index,
                                left_value,
                                right_col,
                                right_index,
                            );
                            col_cmp_col.push(v);
                        })
                }
                arrow_schema::DataType::UInt64 => {
                    let left_value = as_primitive_array::<UInt64Type>(left_column).value(row_index);
                    table
                        .batch()
                        .columns()
                        .iter()
                        .enumerate()
                        .skip(left_index + 1)
                        .for_each(|(right_index, right_col)| {
                            let v = cmp_u64_value(
                                row_index,
                                left_index,
                                left_value,
                                right_col,
                                right_index,
                            );
                            col_cmp_col.push(v);
                        })
                }
                arrow_schema::DataType::Utf8 => {
                    let left_value = as_string_array(left_column).value(row_index);
                    table
                        .batch()
                        .columns()
                        .iter()
                        .enumerate()
                        .skip(left_index + 1)
                        .for_each(|(right_index, right_col)| {
                            let v = cmp_str_value(
                                row_index,
                                left_index,
                                left_value,
                                right_col,
                                right_index,
                            );
                            col_cmp_col.push(v);
                        })
                }
                _ => panic!("cmp_row failed"),
            };
        });

    assert_eq!(
        table.num_columns() * (table.num_columns() + 1) / 2,
        col_cmp_col.len()
    );
    TriangularMatrix::<[bool; 3]>::from_vec(col_cmp_col, table.num_columns())
}

macro_rules! cast_cmp_primitive {
    ($ArrayType:tt, $int:ty, $left_value:tt, $right_col:tt, $row_index:tt) => {{
        let right_arr: &$ArrayType = as_primitive_array(&$right_col);
        let right_value = right_arr.value($row_index) as $int;
        let left_value = $left_value as $int;
        [
            left_value == right_value,
            left_value > right_value,
            left_value < right_value,
        ]
    }};
}

macro_rules! cmp_primitive {
    ($ArrayType:tt, $int:ty, $left_value:tt, $right_col:tt, $row_index:tt) => {{
        let right_arr: &$ArrayType = as_primitive_array(&$right_col);
        let right_value = right_arr.value($row_index) as $int;
        let left_value = $left_value;
        [
            left_value == right_value,
            left_value > right_value,
            left_value < right_value,
        ]
    }};
}

macro_rules! cmp_bool {
    ($int:ty, $left_value:tt, $right_col:tt, $row_index:tt) => {{
        let right_value = as_boolean_array(&$right_col).value($row_index) as $int;
        [
            $left_value == right_value,
            $left_value > right_value,
            $left_value < right_value,
        ]
    }};
}

macro_rules! cmp_signed_string {
    ($left_value:tt, $right_col:tt, $row_index:tt, $zero:tt) => {{
        let right_str = as_string_array(&$right_col).value($row_index);
        match right_str.parse::<i128>() {
            Ok(right_value) => {
                let left_value = $left_value as i128;
                [
                    left_value == right_value,
                    left_value > right_value,
                    left_value < right_value,
                ]
            }
            Err(_) => [
                $left_value == $zero,
                $left_value > $zero,
                $left_value < $zero,
            ],
        }
    }};
}

macro_rules! cmp_unsigned_string {
    ($left_value:tt, $right_col:tt, $row_index:tt, $zero:tt) => {{
        let right_str = as_string_array(&$right_col).value($row_index);
        match right_str.parse::<i128>() {
            Ok(right_value) => {
                let left_value = $left_value as i128;
                [
                    left_value == right_value,
                    left_value > right_value,
                    left_value < right_value,
                ]
            }
            Err(_) => [$left_value == $zero, $left_value > $zero, false],
        }
    }};
}

macro_rules! cmp_string_integer {
    ($ArrayType:tt, $left_str:tt, $right_col:tt, $row_index:tt, $zero:tt) => {{
        let right_arr: &$ArrayType = as_primitive_array(&$right_col);
        let right_value = right_arr.value($row_index) as i128;
        match $left_str.parse::<i128>() {
            Ok(left_value) => {
                let left_value = left_value as i128;
                [
                    left_value == right_value,
                    left_value > right_value,
                    left_value < right_value,
                ]
            }
            Err(_) => [
                $zero == right_value,
                $zero > right_value,
                $zero < right_value,
            ],
        }
    }};
}

macro_rules! cmp_primitive_signed_unsigned {
    ($ArrayType:tt, $int:ty, $left_value:tt, $right_col:tt, $row_index:tt) => {{
        let right_arr: &$ArrayType = as_primitive_array(&$right_col);
        let right_value = right_arr.value($row_index) as $int;
        if $left_value < 0 {
            [false, false, true]
        } else {
            let left_value = $left_value as $int;
            [
                left_value == right_value,
                left_value > right_value,
                left_value < right_value,
            ]
        }
    }};
}

macro_rules! cast_cmp_primitive_unsigned_signed {
    ($ArrayType:tt, $int:ty, $left_value:tt, $right_col:tt, $row_index:tt) => {{
        let right_arr: &$ArrayType = as_primitive_array(&$right_col);
        let right_value = right_arr.value($row_index);
        if right_value < 0 {
            [false, true, false]
        } else {
            let left_value = $left_value as $int;
            let right_value = right_value as $int;
            [
                left_value == right_value,
                left_value > right_value,
                left_value < right_value,
            ]
        }
    }};
}

macro_rules! cmp_primitive_unsigned_signed {
    ($ArrayType:tt, $int:ty, $left_value:tt, $right_col:tt, $row_index:tt) => {{
        let right_arr: &$ArrayType = as_primitive_array(&$right_col);
        let right_value = right_arr.value($row_index);
        if right_value < 0 {
            [false, true, false]
        } else {
            let left_value = $left_value;
            let right_value = right_value as $int;
            [
                left_value == right_value,
                left_value > right_value,
                left_value < right_value,
            ]
        }
    }};
}

fn cmp_bool_value(
    row_index: usize,
    left_index: usize,
    left_value: bool,
    right_col: &ArrayRef,
    right_index: usize,
) -> [bool; 3] {
    assert!(left_index < right_index);
    let right_type = right_col.data_type();
    match right_type {
        arrow_schema::DataType::Boolean => {
            let right_value = as_boolean_array(&right_col).value(row_index);
            println!(
                "left_value={}, right_value={}, row_index={}",
                left_value, right_value, row_index
            );
            [
                left_value == right_value,
                left_value & !right_value,
                !left_value & right_value,
            ]
        }
        arrow_schema::DataType::Int8 => {
            cast_cmp_primitive!(Int8Array, i8, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int16 => {
            cast_cmp_primitive!(Int16Array, i16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int32 => {
            cast_cmp_primitive!(Int32Array, i32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int64 => {
            cast_cmp_primitive!(Int64Array, i64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt8 => {
            cast_cmp_primitive!(UInt8Array, u8, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt16 => {
            cast_cmp_primitive!(UInt16Array, u16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt32 => {
            cast_cmp_primitive!(UInt32Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt64 => {
            cast_cmp_primitive!(UInt64Array, u64, left_value, right_col, row_index)
        }
        //arrow_schema::DataType::Utf8 => cmp_signed_string!(left_value, right_col, row_index, false),
        arrow_schema::DataType::Utf8 => {
            let right_str = as_string_array(&right_col).value(row_index);
            match right_str.parse::<i128>() {
                Ok(right_value) => {
                    let left_value = left_value as i128;
                    [
                        left_value == right_value,
                        left_value > right_value,
                        left_value < right_value,
                    ]
                }
                Err(_) => [
                    left_value as u8 == 0, 
                    left_value as u8 > 0, 
                    false
                ],
            }
        }
        _ => panic!("called cmp_columns with wrong parameters"),
    }
}

fn cmp_i8_value(
    row_index: usize,
    left_index: usize,
    left_value: i8,
    right_col: &ArrayRef,
    right_index: usize,
) -> [bool; 3] {
    assert!(left_index < right_index);
    let right_type = right_col.data_type();
    match right_type {
        arrow_schema::DataType::Boolean => cmp_bool!(i8, left_value, right_col, row_index),
        arrow_schema::DataType::Int8 => {
            cmp_primitive!(Int8Array, i8, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int16 => {
            cast_cmp_primitive!(Int16Array, i16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int32 => {
            cast_cmp_primitive!(Int32Array, i32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int64 => {
            cast_cmp_primitive!(Int64Array, i64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt8 => {
            cmp_primitive_signed_unsigned!(UInt8Array, u8, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt16 => {
            cmp_primitive_signed_unsigned!(UInt16Array, u16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt32 => {
            cmp_primitive_signed_unsigned!(UInt32Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt64 => {
            cmp_primitive_signed_unsigned!(UInt64Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Utf8 => cmp_signed_string!(left_value, right_col, row_index, 0),
        _ => panic!("called cmp_columns with wrong parameters"),
    }
}

fn cmp_u8_value(
    row_index: usize,
    left_index: usize,
    left_value: u8,
    right_col: &ArrayRef,
    right_index: usize,
) -> [bool; 3] {
    assert!(left_index < right_index);
    let right_type = right_col.data_type();
    match right_type {
        arrow_schema::DataType::Boolean => cmp_bool!(u8, left_value, right_col, row_index),
        arrow_schema::DataType::Int8 => {
            cmp_primitive_unsigned_signed!(Int8Array, u8, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int16 => {
            cast_cmp_primitive_unsigned_signed!(Int16Array, u16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int32 => {
            cast_cmp_primitive_unsigned_signed!(Int32Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int64 => {
            cast_cmp_primitive_unsigned_signed!(Int64Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt8 => {
            cmp_primitive!(UInt8Array, u8, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt16 => {
            cast_cmp_primitive!(UInt16Array, u16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt32 => {
            cast_cmp_primitive!(UInt32Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt64 => {
            cast_cmp_primitive!(UInt64Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Utf8 => cmp_unsigned_string!(left_value, right_col, row_index, 0),
        _ => panic!("called cmp_columns with wrong parameters"),
    }
}

fn cmp_i16_value(
    row_index: usize,
    left_index: usize,
    left_value: i16,
    right_col: &ArrayRef,
    right_index: usize,
) -> [bool; 3] {
    assert!(left_index < right_index);
    let right_type = right_col.data_type();
    match right_type {
        arrow_schema::DataType::Boolean => cmp_bool!(i16, left_value, right_col, row_index),
        arrow_schema::DataType::Int8 => {
            cmp_primitive!(Int8Array, i16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int16 => {
            cmp_primitive!(Int16Array, i16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int32 => {
            cast_cmp_primitive!(Int32Array, i32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int64 => {
            cast_cmp_primitive!(Int64Array, i64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt8 => {
            cmp_primitive_signed_unsigned!(UInt8Array, u16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt16 => {
            cmp_primitive_signed_unsigned!(UInt16Array, u16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt32 => {
            cmp_primitive_signed_unsigned!(UInt32Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt64 => {
            cmp_primitive_signed_unsigned!(UInt64Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Utf8 => cmp_signed_string!(left_value, right_col, row_index, 0),
        _ => panic!("called cmp_columns with wrong parameters"),
    }
}

fn cmp_u16_value(
    row_index: usize,
    left_index: usize,
    left_value: u16,
    right_col: &ArrayRef,
    right_index: usize,
) -> [bool; 3] {
    assert!(left_index < right_index);
    let right_type = right_col.data_type();
    match right_type {
        arrow_schema::DataType::Boolean => cmp_bool!(u16, left_value, right_col, row_index),
        arrow_schema::DataType::Int8 => {
            cmp_primitive_unsigned_signed!(Int8Array, u16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int16 => {
            cmp_primitive_unsigned_signed!(Int16Array, u16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int32 => {
            cast_cmp_primitive_unsigned_signed!(Int32Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int64 => {
            cast_cmp_primitive_unsigned_signed!(Int64Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt8 => {
            cmp_primitive!(UInt8Array, u16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt16 => {
            cmp_primitive!(UInt16Array, u16, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt32 => {
            cast_cmp_primitive!(UInt32Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt64 => {
            cast_cmp_primitive!(UInt64Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Utf8 => cmp_unsigned_string!(left_value, right_col, row_index, 0),
        _ => panic!("called cmp_columns with wrong parameters"),
    }
}

fn cmp_i32_value(
    row_index: usize,
    left_index: usize,
    left_value: i32,
    right_col: &ArrayRef,
    right_index: usize,
) -> [bool; 3] {
    assert!(left_index < right_index);
    let right_type = right_col.data_type();
    match right_type {
        arrow_schema::DataType::Boolean => cmp_bool!(i32, left_value, right_col, row_index),
        arrow_schema::DataType::Int8 => {
            cmp_primitive!(Int8Array, i32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int16 => {
            cmp_primitive!(Int16Array, i32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int32 => {
            cmp_primitive!(Int32Array, i32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int64 => {
            cast_cmp_primitive!(Int64Array, i64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt8 => {
            cmp_primitive_signed_unsigned!(UInt8Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt16 => {
            cmp_primitive_signed_unsigned!(UInt16Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt32 => {
            cmp_primitive_signed_unsigned!(UInt32Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt64 => {
            cmp_primitive_signed_unsigned!(UInt64Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Utf8 => cmp_signed_string!(left_value, right_col, row_index, 0),
        _ => panic!("called cmp_columns with wrong parameters"),
    }
}

fn cmp_u32_value(
    row_index: usize,
    left_index: usize,
    left_value: u32,
    right_col: &ArrayRef,
    right_index: usize,
) -> [bool; 3] {
    assert!(left_index < right_index);
    let right_type = right_col.data_type();
    match right_type {
        arrow_schema::DataType::Boolean => cmp_bool!(u32, left_value, right_col, row_index),
        arrow_schema::DataType::Int8 => {
            cmp_primitive_unsigned_signed!(Int8Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int16 => {
            cmp_primitive_unsigned_signed!(Int16Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int32 => {
            cmp_primitive_unsigned_signed!(Int32Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int64 => {
            cast_cmp_primitive_unsigned_signed!(Int64Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt8 => {
            cmp_primitive!(UInt8Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt16 => {
            cmp_primitive!(UInt16Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt32 => {
            cmp_primitive!(UInt32Array, u32, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt64 => {
            cast_cmp_primitive!(UInt64Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Utf8 => cmp_unsigned_string!(left_value, right_col, row_index, 0),
        _ => panic!("called cmp_columns with wrong parameters"),
    }
}

fn cmp_i64_value(
    row_index: usize,
    left_index: usize,
    left_value: i64,
    right_col: &ArrayRef,
    right_index: usize,
) -> [bool; 3] {
    assert!(left_index < right_index);
    let right_type = right_col.data_type();
    match right_type {
        arrow_schema::DataType::Boolean => cmp_bool!(i64, left_value, right_col, row_index),
        arrow_schema::DataType::Int8 => {
            cmp_primitive!(Int8Array, i64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int16 => {
            cmp_primitive!(Int16Array, i64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int32 => {
            cmp_primitive!(Int32Array, i64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int64 => {
            cmp_primitive!(Int64Array, i64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt8 => {
            cmp_primitive_signed_unsigned!(UInt8Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt16 => {
            cmp_primitive_signed_unsigned!(UInt16Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt32 => {
            cmp_primitive_signed_unsigned!(UInt32Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt64 => {
            cmp_primitive_signed_unsigned!(UInt64Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Utf8 => cmp_signed_string!(left_value, right_col, row_index, 0),
        _ => panic!("called cmp_columns with wrong parameters"),
    }
}

fn cmp_u64_value(
    row_index: usize,
    left_index: usize,
    left_value: u64,
    right_col: &ArrayRef,
    right_index: usize,
) -> [bool; 3] {
    assert!(left_index < right_index);
    let right_type = right_col.data_type();
    match right_type {
        arrow_schema::DataType::Boolean => cmp_bool!(u64, left_value, right_col, row_index),
        arrow_schema::DataType::Int8 => {
            cmp_primitive_unsigned_signed!(Int8Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int16 => {
            cmp_primitive_unsigned_signed!(Int16Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int32 => {
            cmp_primitive_unsigned_signed!(Int32Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Int64 => {
            cmp_primitive_unsigned_signed!(Int64Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt8 => {
            cmp_primitive!(UInt8Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt16 => {
            cmp_primitive!(UInt16Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt32 => {
            cmp_primitive!(UInt32Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::UInt64 => {
            cmp_primitive!(UInt64Array, u64, left_value, right_col, row_index)
        }
        arrow_schema::DataType::Utf8 => cmp_unsigned_string!(left_value, right_col, row_index, 0),
        _ => panic!("called cmp_columns with wrong parameters"),
    }
}

fn cmp_str_value(
    row_index: usize,
    left_index: usize,
    left_value: &str,
    right_col: &ArrayRef,
    right_index: usize,
) -> [bool; 3] {
    assert!(left_index < right_index);
    let right_type = right_col.data_type();
    match right_type {
        arrow_schema::DataType::Boolean => {
            let right_arr: &BooleanArray = as_boolean_array(&right_col);
            let right_value = right_arr.value(row_index) as i128;
            match left_value.parse::<i128>() {
                Ok(left_value) => [
                    left_value == right_value,
                    left_value > right_value,
                    left_value < right_value,
                ],
                Err(_) => [0 == right_value, 0 > right_value, 0 < right_value],
            }
        }
        arrow_schema::DataType::Int8 => {
            cmp_string_integer!(Int8Array, left_value, right_col, row_index, 0)
        }
        arrow_schema::DataType::Int16 => {
            cmp_string_integer!(Int16Array, left_value, right_col, row_index, 0)
        }
        arrow_schema::DataType::Int32 => {
            cmp_string_integer!(Int32Array, left_value, right_col, row_index, 0)
        }
        arrow_schema::DataType::Int64 => {
            cmp_string_integer!(Int64Array, left_value, right_col, row_index, 0)
        }
        arrow_schema::DataType::UInt8 => {
            cmp_string_integer!(UInt8Array, left_value, right_col, row_index, 0)
        }
        arrow_schema::DataType::UInt16 => {
            cmp_string_integer!(UInt16Array, left_value, right_col, row_index, 0)
        }
        arrow_schema::DataType::UInt32 => {
            cmp_string_integer!(UInt32Array, left_value, right_col, row_index, 0)
        }
        arrow_schema::DataType::UInt64 => {
            cmp_string_integer!(UInt64Array, left_value, right_col, row_index, 0)
        }
        arrow_schema::DataType::Utf8 => {
            let right_str = as_string_array(&right_col).value(row_index);
            [
                left_value == right_str,
                left_value > right_str,
                left_value < right_str,
            ]
        }
        _ => panic!("called cmp_columns with wrong parameters"),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::*;
    use arrow_schema::{DataType, Field, Schema};

    use crate::Table;

    use super::table_row_col_cmp_col;

    #[test]
    fn test_bool_bool() {
        let schema = Schema::new(vec![
            Field::new("Boolean1", DataType::Boolean, false),
            Field::new("Boolean2", DataType::Boolean, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(BooleanArray::from(vec![true, false, true, false])),
                Arc::new(BooleanArray::from(vec![true, false, false, true])),
            ],
        )
        .unwrap();

        let t1 = Table::new("table1", batch);
        let tr = table_row_col_cmp_col(&t1, 0);
        let e = vec![
            [true, false, false], // 0,0
            [true, false, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 1);
        let e = vec![
            [true, false, false], // 0,0
            [true, false, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 2);
        let e = vec![
            [true, false, false], // 0,0
            [false, true, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 3);
        let e = vec![
            [true, false, false], // 0,0
            [false, false, true], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());
    }

    #[test]
    fn test_true_i8() {
        let schema = Schema::new(vec![
            Field::new("Boolean1", DataType::Boolean, false),
            Field::new("Int8", DataType::Int8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(BooleanArray::from(vec![true, true, true, true, true])),
                Arc::new(Int8Array::from(vec![0, 1, 123, -1, -123])),
            ],
        )
        .unwrap();

        let t1 = Table::new("table1", batch);

        let tr = table_row_col_cmp_col(&t1, 0);
        let e = vec![
            [true, false, false], // 0,0
            [false, true, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 1);
        let e = vec![
            [true, false, false], // 0,0
            [true, false, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 2);
        let e = vec![
            [true, false, false], // 0,0
            [false, false, true], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 3);
        let e = vec![
            [true, false, false], // 0,0
            [false, true, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 4);
        let e = vec![
            [true, false, false], // 0,0
            [false, true, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());
    }

    #[test]
    fn test_false_i8() {
        let schema = Schema::new(vec![
            Field::new("Boolean1", DataType::Boolean, false),
            Field::new("Int8", DataType::Int8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(BooleanArray::from(vec![false, false, false, false, false])),
                Arc::new(Int8Array::from(vec![0, 1, 123, -1, -123])),
            ],
        )
        .unwrap();

        let t1 = Table::new("table1", batch);

        let tr = table_row_col_cmp_col(&t1, 0);
        let e = vec![
            [true, false, false], // 0,0
            [true, false, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 1);
        let e = vec![
            [true, false, false], // 0,0
            [false, false, true], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 2);
        let e = vec![
            [true, false, false], // 0,0
            [false, false, true], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 3);
        let e = vec![
            [true, false, false], // 0,0
            [false, true, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 4);
        let e = vec![
            [true, false, false], // 0,0
            [false, true, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());
    }

    #[test]
    fn test_true_str() {
        let schema = Schema::new(vec![
            Field::new("Boolean", DataType::Boolean, false),
            Field::new("Utf8", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(BooleanArray::from(vec![true, true, true, true])),
                Arc::new(StringArray::from(vec!["abc", "0", "1", "-1"])),
            ],
        )
        .unwrap();

        let t1 = Table::new("table1", batch);

        let tr = table_row_col_cmp_col(&t1, 0);
        let e = vec![
            [true, false, false], // 0,0
            [false, true, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 1);
        let e = vec![
            [true, false, false], // 0,0
            [false, true, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 2);
        let e = vec![
            [true, false, false], // 0,0
            [true, false, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 3);
        let e = vec![
            [true, false, false], // 0,0
            [false, true, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());
    }

    #[test]
    fn test_false_str() {
        let schema = Schema::new(vec![
            Field::new("Boolean", DataType::Boolean, false),
            Field::new("Utf8", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(BooleanArray::from(vec![false, false, false, false])),
                Arc::new(StringArray::from(vec!["abc", "0", "1", "-1"])),
            ],
        )
        .unwrap();

        let t1 = Table::new("table1", batch);

        let tr = table_row_col_cmp_col(&t1, 0);
        let e = vec![
            [true, false, false], // 0,0
            [true, false, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 1);
        let e = vec![
            [true, false, false], // 0,0
            [true, false, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 2);
        let e = vec![
            [true, false, false], // 0,0
            [false, false, true], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 3);
        let e = vec![
            [true, false, false], // 0,0
            [false, true, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());
    }

    #[test]
    fn test_str_str() {
        let schema = Schema::new(vec![
            Field::new("Utf81", DataType::Utf8, false),
            Field::new("Utf82", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec!["abc", "abc", "12", "-12"])),
                Arc::new(StringArray::from(vec!["abc", "z", "1", "-1"])),
            ],
        )
        .unwrap();

        let t1 = Table::new("table1", batch);

        let tr = table_row_col_cmp_col(&t1, 0);
        let e = vec![
            [true, false, false], // 0,0
            [true, false, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 1);
        let e = vec![
            [true, false, false], // 0,0
            [false, false, true], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 2);
        let e = vec![
            [true, false, false], // 0,0
            [false, true, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());

        let tr = table_row_col_cmp_col(&t1, 3);
        let e = vec![
            [true, false, false], // 0,0
            [false, true, false], // 0,1
            [true, false, false], // 1,1
        ];
        assert_eq!(&e, tr.elements());
    }

    #[test]
    fn test_signed() {
        let schema = Schema::new(vec![
            Field::new("Int8", DataType::Int8, false),
            Field::new("Int16", DataType::Int16, false),
            Field::new("Int32", DataType::Int32, false),
            Field::new("Int64", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int8Array::from(vec![i8::MIN, i8::MAX])),
                Arc::new(Int16Array::from(vec![i16::MIN, i16::MAX])),
                Arc::new(Int32Array::from(vec![i32::MIN, i32::MAX])),
                Arc::new(Int64Array::from(vec![i64::MIN, i64::MAX])),
            ],
        )
        .unwrap();

        let t1 = Table::new("table1", batch);

        let tr = table_row_col_cmp_col(&t1, 0);
        assert_eq!(tr.elements().len(), 10);
        assert_eq!(tr.dim(), 4);
        let mut count = 0;
        for i in 0..tr.dim() {
            for j in i..tr.dim() {
                let a = tr.get(i, j);
                if i == j {
                    assert_eq!(a, &[true, false, false]);
                } else {
                    assert_eq!(a, &[false, true, false]);
                }
                count += 1;
            }
        }
        assert_eq!(count, 10);

        let tr = table_row_col_cmp_col(&t1, 1);
        assert_eq!(tr.elements().len(), 10);
        assert_eq!(tr.dim(), 4);
        let mut count = 0;
        for i in 0..tr.dim() {
            for j in i..tr.dim() {
                let a = tr.get(i, j);
                if i == j {
                    assert_eq!(a, &[true, false, false]);
                } else {
                    assert_eq!(a, &[false, false, true]);
                }
                count += 1;
            }
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_unsigned() {
        let schema = Schema::new(vec![
            Field::new("Int8", DataType::Int8, false),
            Field::new("UInt8", DataType::UInt8, false),
            Field::new("Int16", DataType::Int16, false),
            Field::new("UInt16", DataType::UInt16, false),
            Field::new("Int32", DataType::Int32, false),
            Field::new("UInt32", DataType::UInt32, false),
            Field::new("Int64", DataType::Int64, false),
            Field::new("UInt64", DataType::UInt64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int8Array::from(vec![i8::MAX])),
                Arc::new(UInt8Array::from(vec![u8::MAX])),
                Arc::new(Int16Array::from(vec![i16::MAX])),
                Arc::new(UInt16Array::from(vec![u16::MAX])),
                Arc::new(Int32Array::from(vec![i32::MAX])),
                Arc::new(UInt32Array::from(vec![u32::MAX])),
                Arc::new(Int64Array::from(vec![i64::MAX])),
                Arc::new(UInt64Array::from(vec![u64::MAX])),
            ],
        )
        .unwrap();

        let t1 = Table::new("table1", batch);

        let tr = table_row_col_cmp_col(&t1, 0);
        assert_eq!(tr.elements().len(), 36);
        assert_eq!(tr.dim(), 8);
        let mut count = 0;
        for i in 0..tr.dim() {
            for j in i..tr.dim() {
                let a = tr.get(i, j);
                if i == j {
                    assert_eq!(a, &[true, false, false]);
                } else {
                    assert_eq!(a, &[false, false, true]);
                }
                count += 1;
            }
        }
        assert_eq!(count, 36);

        let schema = Schema::new(vec![
            Field::new("Int64", DataType::Int64, false),
            Field::new("Int32", DataType::Int32, false),
            Field::new("Int16", DataType::Int16, false),
            Field::new("Int8", DataType::Int8, false),
            Field::new("UInt8", DataType::UInt8, false),
            Field::new("UInt16", DataType::UInt16, false),
            Field::new("UInt32", DataType::UInt32, false),
            Field::new("UInt64", DataType::UInt64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(vec![i64::MIN])),
                Arc::new(Int32Array::from(vec![i32::MIN])),
                Arc::new(Int16Array::from(vec![i16::MIN])),
                Arc::new(Int8Array::from(vec![i8::MIN])),
                Arc::new(UInt8Array::from(vec![u8::MIN])),
                Arc::new(UInt16Array::from(vec![u16::MAX])),
                Arc::new(UInt32Array::from(vec![u32::MAX])),
                Arc::new(UInt64Array::from(vec![u64::MAX])),
            ],
        )
        .unwrap();

        let t1 = Table::new("table1", batch);

        let tr = table_row_col_cmp_col(&t1, 0);
        assert_eq!(tr.elements().len(), 36);
        assert_eq!(tr.dim(), 8);
        let mut count = 0;
        for i in 0..tr.dim() {
            for j in i..tr.dim() {
                let a = tr.get(i, j);
                if i == j {
                    assert_eq!(a, &[true, false, false]);
                } else {
                    assert_eq!(a, &[false, false, true]);
                }
                count += 1;
            }
        }
        assert_eq!(count, 36);
    }
}
