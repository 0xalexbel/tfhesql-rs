use crate::utils::path::extract_filename_without_ext;
use crate::FheSqlError;
use crate::Table;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use std::{fs::File, io::Seek, sync::Arc};

////////////////////////////////////////////////////////////////////////////////

const DELIMITER: u8 = b","[0];

pub fn load_schema(path: &str) -> Result<Schema, FheSqlError> {
    let mut file = file_open(path)?;
    let (schema, _) = parse_schema(&mut file, Some(1))?;
    Ok(schema)
}

pub fn load(path: &str, bounds: Option<(usize, usize)>) -> Result<Table, FheSqlError> {
    let filename = match extract_filename_without_ext(path) {
        Some(s) => s,
        None => return Err(FheSqlError::CsvError(format!("Invalid pathname {}", path))),
    };

    let mut file = file_open(path)?;

    let (schema, count_lines) = parse_schema(&mut file, None)?;

    let mut buf_reader = new_buf_reader(file, schema, count_lines, bounds)?;

    match buf_reader.next() {
        None => Err(FheSqlError::CsvError("Empty CSV file".to_string())),
        Some(res) => match res {
            Err(err) => Err(FheSqlError::CsvError(err.to_string())),
            Ok(batch) => Ok(Table::new(&filename, batch)),
        },
    }
}

////////////////////////////////////////////////////////////////////////////////

macro_rules! res_or_csv_error {
    ($result:tt) => {
        match $result {
            Err(err) => Err(FheSqlError::CsvError(err.to_string())),
            Ok(ok) => Ok(ok),
        }
    };
}

fn string_to_data_type(str: &str) -> Result<DataType, String> {
    match str {
        "bool" => Ok(DataType::Boolean),
        "uint8" => Ok(DataType::UInt8),
        "uint16" => Ok(DataType::UInt16),
        "uint32" => Ok(DataType::UInt32),
        "uint64" => Ok(DataType::UInt64),
        "int8" => Ok(DataType::Int8),
        "int16" => Ok(DataType::Int16),
        "int32" => Ok(DataType::Int32),
        "int64" => Ok(DataType::Int64),
        "string" => Ok(DataType::Utf8),
        _ => Err(format!("Unknown data type {}", str)),
    }
}

fn data_type_to_string(data_type: &DataType) -> Result<String, String> {
    match data_type {
        DataType::Boolean => Ok("bool".to_string()),
        DataType::Int8 => Ok("int8".to_string()),
        DataType::Int16 => Ok("int16".to_string()),
        DataType::Int32 => Ok("int32".to_string()),
        DataType::Int64 => Ok("int64".to_string()),
        DataType::UInt8 => Ok("uint8".to_string()),
        DataType::UInt16 => Ok("uint16".to_string()),
        DataType::UInt32 => Ok("uint32".to_string()),
        DataType::UInt64 => Ok("uint64".to_string()),
        DataType::Utf8 => Ok("string".to_string()),
        _ => Err("Unsupported DataType".to_string()),
    }
}

fn file_open(path: &str) -> Result<File, FheSqlError> {
    match File::open(path) {
        Err(_) => Err(FheSqlError::IoError(
            format!("Unable to open file {}", path).to_string(),
        )),
        Ok(file) => Ok(file),
    }
}

fn infer_schema(
    file: &mut File,
    max_records: Option<usize>,
) -> Result<(Schema, usize), FheSqlError> {
    let fmt_res = arrow_csv::reader::Format::default()
        .with_header(true)
        .with_delimiter(DELIMITER)
        .infer_schema(file, max_records);

    res_or_csv_error!(fmt_res)
}

fn write_schema(schema: &Schema) -> String {
    let v: Vec<String> = schema
        .fields()
        .iter()
        .map(|field_ref| {
            let column_name_ref = field_ref.as_ref().name();
            let data_type_str = data_type_to_string(field_ref.data_type()).unwrap();
            format!("{}:{}", column_name_ref, data_type_str)
        })
        .collect();
    v.join(",")
}

fn parse_schema(
    file: &mut File,
    max_records: Option<usize>,
) -> Result<(Schema, usize), FheSqlError> {
    let (infer_schema, count_lines) = infer_schema(file, max_records)?;

    file.rewind().unwrap();

    let mut fields: Vec<Field> = vec![];
    infer_schema.fields().iter().try_for_each(|field_ref| {
        let column_name_ref = field_ref.as_ref().name();
        let name_and_type: Vec<&str> = column_name_ref.split(':').collect();
        if name_and_type.len() != 2 {
            return Err(FheSqlError::CsvError(
                format!("Invalid column name {}", column_name_ref).to_string(),
            ));
        }

        let data_type: DataType;
        let data_type_res = string_to_data_type(name_and_type[1]);
        match data_type_res {
            Err(err) => {
                return Err(FheSqlError::CsvError(err));
            }
            Ok(t) => {
                data_type = t;
            }
        }

        fields.push(Field::new(name_and_type[0], data_type, false));
        Ok(())
    })?;

    Ok((Schema::new(fields), count_lines))
}

fn new_buf_reader(
    file: File,
    schema: Schema,
    lines: usize,
    bounds: Option<(usize, usize)>,
) -> Result<arrow_csv::reader::BufReader<std::io::BufReader<File>>, FheSqlError> {
    // create a builder
    let mut reader_builder = arrow_csv::ReaderBuilder::new(Arc::new(schema))
        .with_header(true)
        .with_delimiter(DELIMITER)
        .with_batch_size(lines);

    if let Some((start, end)) = bounds {
        reader_builder = reader_builder.with_bounds(start, end)
    }

    let result = reader_builder.build(file);

    res_or_csv_error!(result)
}

pub fn record_batch_to_csv_string(batch: &arrow_array::RecordBatch) -> Result<String, FheSqlError> {
    let mut header = write_schema(&batch.schema());
    header.push('\n');
    let mut buffer: Vec<u8> = header.as_bytes().to_vec();

    let mut writer = arrow_csv::WriterBuilder::new()
        .with_delimiter(DELIMITER)
        .with_header(false)
        .build(&mut buffer);

    if let Err(err) = writer.write(batch) {
        return Err(FheSqlError::CsvError(err.to_string()));
    }

    drop(writer);

    Ok(String::from_utf8(buffer)
        .unwrap()
        //.trim_matches(|c| c == '\n' || c == '\r' || c == char::from(0))
        .trim_matches(char::from(0))
        .to_string())
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use crate::csv::{load, record_batch_to_csv_string};
    use arrow_array::cast::*;
    use arrow_array::*;

    #[test]
    fn test_table_load() {
        let v_bool = vec![true, false, true];
        let v_u32 = vec![12, 27, 32];
        let v_u64 = vec![12300, 1400, 256900];
        let v_i16: Vec<i16> = vec![-12, -13, 0];
        let t = load("./test/data.csv", None).unwrap();
        let a: &BooleanArray = as_boolean_array(t.batch().column(0).as_ref());
        assert_eq!(a, &BooleanArray::from(v_bool.clone()));
        let a: &StringArray = as_string_array(t.batch().column(1).as_ref());
        assert_eq!(a, &StringArray::from(vec!["Jean", "Bernard", "DuLoft"]));
        let a: &UInt32Array = as_primitive_array(t.batch().column(2).as_ref());
        assert_eq!(a, &UInt32Array::from(v_u32.clone()));
        let a: &UInt64Array = as_primitive_array(t.batch().column(3).as_ref());
        assert_eq!(a, &UInt64Array::from(v_u64.clone()));
        let a: &Int16Array = as_primitive_array(t.batch().column(4).as_ref());
        assert_eq!(a, &Int16Array::from(v_i16.clone()));

        let mut s1 = std::fs::read_to_string("./test/data.csv").unwrap();
        s1.retain(|c| c != '\r');
        let s2 = record_batch_to_csv_string(t.batch()).unwrap();
        assert_eq!(s1, s2);

        let t = load("./test/data.csv", Some((0, 1))).unwrap();
        let a: &BooleanArray = as_boolean_array(t.batch().column(0).as_ref());
        assert_eq!(a, &BooleanArray::from(v_bool[0..1].to_vec()));
        let a: &StringArray = as_string_array(t.batch().column(1).as_ref());
        assert_eq!(a, &StringArray::from(vec!["Jean"]));
        let a: &UInt32Array = as_primitive_array(t.batch().column(2).as_ref());
        assert_eq!(a, &UInt32Array::from(v_u32[0..1].to_vec()));
        let a: &UInt64Array = as_primitive_array(t.batch().column(3).as_ref());
        assert_eq!(a, &UInt64Array::from(v_u64[0..1].to_vec()));
        let a: &Int16Array = as_primitive_array(t.batch().column(4).as_ref());
        assert_eq!(a, &Int16Array::from(v_i16[0..1].to_vec()));
    }
}
