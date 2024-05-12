use arrow_array::*;
use arrow_array::types::*;
use arrow_schema::*;
use std::sync::Arc;

////////////////////////////////////////////////////////////////////////////////
// Batch
////////////////////////////////////////////////////////////////////////////////

pub fn simple_batch_0() -> RecordBatch {
    let mut rb = RecordBatchBuilder::new();
    rb.push_with_name::<UInt8Type>("Column1", vec![123]);
    rb.finish()
}

pub fn simple_batch_1() -> RecordBatch {
    let mut rb = RecordBatchBuilder::new();
    rb.push_with_name::<Int8Type>("CustomerID", vec![21, 22, 23, 24, 25]);
    rb.push_with_name::<Int16Type>("PostalCode", vec![-5, -6, -7, -8, -9]);
    rb.push_with_name::<Int32Type>("Preferences", vec![33, 34, 35, 36, 37]);
    rb.finish()
}

pub fn simple_batch_2() -> RecordBatch {
    let mut rb = RecordBatchBuilder::new();
    rb.push_with_name::<Int8Type>("ProductID", vec![5, 2, 3]);
    rb.push_with_name::<Int16Type>("Type", vec![5, 6, 7]);
    rb.push_with_name::<Int32Type>("Style", vec![9, 10, 11]);
    rb.push_with_name::<Int32Type>("Category", vec![13, 14, 15]);
    rb.push_str_with_name("Name", vec!["ab", "cd", "ef"]);
    rb.finish()
}

pub fn simple_batch_3() -> RecordBatch {
    let mut rb = RecordBatchBuilder::new();
    rb.push_with_name::<Int16Type>("ProductID", vec![50, 100, 100]);
    rb.push_with_name::<Int16Type>("Type", vec![50, 500, 600]);
    rb.finish()
}

pub struct RecordBatchBuilder {
    schema_builder: SchemaBuilder,
    arrays: Vec<ArrayRef>,
}

impl RecordBatchBuilder {
    pub fn new() -> Self {
        RecordBatchBuilder {
            schema_builder: SchemaBuilder::new(),
            arrays: vec![],
        }
    }
    pub fn one_string_with_name(name: impl Into<String>, v: Vec<String>) -> RecordBatch {
        let mut rb = RecordBatchBuilder::new();
        rb.push_string_with_name(name, v);
        rb.finish()
    }
    pub fn one_string(v: Vec<String>) -> RecordBatch {
        let mut rb = RecordBatchBuilder::new();
        rb.push_string(v);
        rb.finish()
    }
    pub fn one_bool(v: Vec<bool>) -> RecordBatch {
        let mut rb = RecordBatchBuilder::new();
        rb.push_bool(v);
        rb.finish()
    }
    pub fn one<IType>(v: Vec<IType::Native>) -> RecordBatch
    where
        IType: ArrowPrimitiveType,
        PrimitiveArray<IType>: From<Vec<<IType as ArrowPrimitiveType>::Native>>,
    {
        let mut rb = RecordBatchBuilder::new();
        rb.push::<IType>(v);
        rb.finish()
    }

    pub fn push_string_with_name(&mut self, name: impl Into<String>, v: Vec<String>) {
        self.schema_builder
            .push(Field::new(name, DataType::Utf8, false));
        self.arrays.push(Arc::new(StringArray::from(v)));
    }

    pub fn next_column_name(&self) -> String {
        format!("Column{}", self.arrays.len() + 1)
    } 

    pub fn push_string(&mut self, v: Vec<String>) {
        self.push_string_with_name(self.next_column_name(), v);
    }

    pub fn push_str_with_name(&mut self, name: impl Into<String>, v: Vec<&str>) {
        self.schema_builder
            .push(Field::new(name, DataType::Utf8, false));
        self.arrays.push(Arc::new(StringArray::from(v)));
    }

    pub fn push_str(&mut self, v: Vec<&str>) {
        self.push_str_with_name(self.next_column_name(), v);
    }

    pub fn push_bool_with_name(&mut self, name: impl Into<String>, v: Vec<bool>) {
        self.schema_builder
            .push(Field::new(name, DataType::Boolean, false));
        self.arrays.push(Arc::new(BooleanArray::from(v)));
    }

    pub fn push_bool(&mut self, v: Vec<bool>) {
        self.push_bool_with_name(self.next_column_name(), v);
    }

    pub fn push_with_name<IType>(&mut self, name: impl Into<String>, v: Vec<IType::Native>)
    where
        IType: ArrowPrimitiveType,
        PrimitiveArray<IType>: From<Vec<<IType as ArrowPrimitiveType>::Native>>,
    {
        self.schema_builder
            .push(Field::new(name, IType::DATA_TYPE, false));
        self.arrays.push(Arc::new(PrimitiveArray::<IType>::from(v)));
    }

    pub fn push<IType>(&mut self, v: Vec<IType::Native>)
    where
        IType: ArrowPrimitiveType,
        PrimitiveArray<IType>: From<Vec<<IType as ArrowPrimitiveType>::Native>>,
    {
        self.push_with_name::<IType>(self.next_column_name(), v);
    }

    pub fn finish(self) -> RecordBatch {
        RecordBatch::try_new(Arc::new(self.schema_builder.finish()), self.arrays).unwrap()
    }
}

