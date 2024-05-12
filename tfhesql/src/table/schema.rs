////////////////////////////////////////////////////////////////////////////////
// OrderedSchemas
////////////////////////////////////////////////////////////////////////////////

use std::{io, sync::Arc};

use crate::{
    csv::load_schema,
    error::FheSqlError,
    types::UIntType,
    uint::mask::Mask,
    utils::{
        arrow::arrow_shema_data_type_width,
        path::{absolute_path, csv_sorted_list_in_dir, extract_filename_without_ext},
    },
};
use arrow_schema::SchemaRef;

////////////////////////////////////////////////////////////////////////////////
// OrderedSchemas
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct NamedSchema {
    name: String,
    schema: SchemaRef,
}

/// A fixed-order list of [Schemas](arrow_schema::Schema). The fixed-order nature is critical
/// and should be preserved between the [FheSqlClient](crate::FheSqlClient) and
/// the [FheSqlServer](crate::FheSqlServer) as it is used to generate tables and columns
/// boolean masks.  
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct OrderedSchemas {
    ordered_schemas: Vec<NamedSchema>,
    row_max_width: usize,
    fields_max_width: Vec<usize>,
}

impl OrderedSchemas {
    pub fn new_empty() -> Self {
        OrderedSchemas {
            ordered_schemas: vec![],
            row_max_width: 0,
            fields_max_width: vec![],
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ordered_schemas.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ordered_schemas.is_empty()
    }

    #[inline]
    pub fn schema(&self, index: usize) -> &SchemaRef {
        &self.ordered_schemas[index].schema
    }

    #[inline]
    pub fn name(&self, schema_index: usize) -> &String {
        &self.ordered_schemas[schema_index].name
    }

    /// Creates a new OrderedSchemas structure by parsing all the .csv files located in the specified directory.
    ///
    /// Note: Schemas are sorted by their corresponding csv filename in Rust string comparison order.
    pub fn load_from_directory<P: AsRef<std::path::Path>>(dir: P) -> Result<Self, FheSqlError> {
        let abs_dir = absolute_path(dir)?;
        if !abs_dir.is_dir() {
            return Err(FheSqlError::IoError(format!(
                "Directory does not exist: {}",
                abs_dir.display()
            )));
        }
        let v = csv_sorted_list_in_dir(abs_dir);
        let mut named_ordered_schemas = vec![];
        v.iter().for_each(|f| {
            let s = match load_schema(f) {
                Ok(s) => s,
                Err(_) => return,
            };
            named_ordered_schemas.push((Arc::new(s), extract_filename_without_ext(f).unwrap()));
        });
        OrderedSchemas::from_schemas(named_ordered_schemas)
    }

    pub fn save_as_json<P: AsRef<std::path::Path>>(&self, path: P) -> io::Result<()> {
        let file = std::fs::File::create(path)?;
        let mut writer = std::io::BufWriter::new(file);
        serde_json::to_writer(&mut writer, self)?;
        use std::io::Write;
        writer.flush()?;
        Ok(())
    }

    pub fn load_json<P: AsRef<std::path::Path>>(path: P) -> io::Result<Self> {
        let p = path.as_ref();
        if !p.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Path does not exist: {}", p.display()),
            ));
        }

        let file = std::fs::File::open(p)?;

        let reader = std::io::BufReader::new(file);
        let ordered_schemas: OrderedSchemas = match serde_json::from_reader(reader) {
            Ok(os) => os,
            Err(e) => return Err(std::io::Error::from(e)),
        };

        Ok(ordered_schemas)
    }

    pub(super) fn from_schemas(
        mut named_ordered_schemas: Vec<(SchemaRef, String)>,
    ) -> Result<Self, FheSqlError> {
        named_ordered_schemas.sort_by(|(_, name_a), (_, name_b)| name_a.cmp(name_b));
        let ordered_schemas = named_ordered_schemas
            .into_iter()
            .map(|(schema_ref, name)| NamedSchema {
                schema: schema_ref,
                name,
            })
            .collect();
        let mut os = OrderedSchemas {
            ordered_schemas,
            row_max_width: 0,
            fields_max_width: vec![],
        };
        os.fields_max_width = os.compute_fields_width()?;
        os.row_max_width = os.compute_row_max_width();
        Ok(os)
    }

    #[inline]
    pub(crate) fn max_num_fields(&self) -> usize {
        self.fields_max_width.len()
    }

    #[inline]
    pub(crate) fn data_type_at(
        &self,
        schema_index: usize,
        field_index: usize,
    ) -> &arrow_schema::DataType {
        self.schema(schema_index).fields[field_index].data_type()
    }

    #[inline]
    fn num_schema_fields_at(&self, schema_index: usize) -> usize {
        self.schema(schema_index).fields.len()
    }

    fn compute_max_num_fields(&self) -> usize {
        let mut max_num = 0;
        for i in 0..self.len() {
            max_num = max_num.max(self.num_schema_fields_at(i));
        }
        max_num
    }

    fn compute_width_at(&self, field_index: usize) -> Result<usize, FheSqlError> {
        let mut max_width = 0;
        for schema_index in 0..self.len() {
            let num_fields = self.num_schema_fields_at(schema_index);
            if field_index < num_fields {
                let data_type = self.data_type_at(schema_index, field_index);
                max_width = max_width.max(arrow_shema_data_type_width(data_type)?);
            }
        }
        assert!(max_width <= (u8::MAX as usize));
        Ok(max_width)
    }

    fn compute_fields_width(&self) -> Result<Vec<usize>, FheSqlError> {
        let num_fields = self.compute_max_num_fields();
        let mut fields_width: Vec<usize> = vec![0; num_fields];
        match fields_width
            .iter_mut()
            .enumerate()
            .try_for_each(|(field_index, w)| {
                *w = self.compute_width_at(field_index)?;
                Ok(())
            }) {
            Ok(_) => Ok(fields_width),
            Err(err) => Err(err),
        }
    }

    fn compute_row_max_width(&self) -> usize {
        self.fields_max_width.iter().sum::<usize>()
    }

    fn find_table_index(&self, name: &str) -> Option<usize> {
        let name_lower_case = name.to_lowercase();
        (0..self.len()).find(|&i| self.name(i).to_lowercase() == name_lower_case)
    }

    fn find_schema_field_index(&self, schema_index: usize, field_name: &str) -> Option<usize> {
        self.ordered_schemas[schema_index]
            .schema
            .as_ref()
            .fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.as_ref().name().eq_ignore_ascii_case(field_name))
            .map(|(idx, _)| idx)
    }

    pub(crate) fn compute_schema_field_mask<T>(
        &self,
        schema_index: usize,
        projection: &[sqlparser::ast::SelectItem],
    ) -> Result<Mask<T>, FheSqlError>
    where
        T: UIntType + Clone,
    {
        assert!(self.max_num_fields() >= self.num_schema_fields_at(schema_index));
        let mut m = Mask::<T>::none(self.max_num_fields());
        projection.iter().try_for_each(|proj| match proj {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => match expr {
                sqlparser::ast::Expr::Identifier(ident) => {
                    let field_index = match self.find_schema_field_index(schema_index, &ident.value)
                    {
                        Some(idx) => idx,
                        None => return Err(FheSqlError::UnknownColumnName(ident.value.clone())),
                    };
                    m.set(field_index);
                    Ok(())
                }
                _ => panic!("Unexpected SelectedItem expression"),
            },
            sqlparser::ast::SelectItem::Wildcard(_) => {
                m.set_from_to(0, self.num_schema_fields_at(schema_index) - 1);
                Ok(())
            }
            _ => panic!("Unexpected SelectedItem"),
        })?;
        Ok(m)
    }

    pub(crate) fn compute_table_mask<T>(&self, idents: &[sqlparser::ast::Ident]) -> Mask<T>
    where
        T: UIntType + Clone,
    {
        let mut m = Mask::<T>::none(self.len());
        idents.iter().for_each(|ident| {
            if let Some(idx) = self.find_table_index(&ident.value) {
                m.set(idx)
            }
        });
        m
    }
}

#[cfg(test)]
mod test {
    use crate::test_util::tfhesql_test_db_dir;

    use super::OrderedSchemas;

    #[test]
    fn test() {
        let dir = tfhesql_test_db_dir("medium");
        let os = OrderedSchemas::load_from_directory(&dir).unwrap();
        assert_eq!(os.name(0), "Categories");
        assert_eq!(os.name(1), "Customers");

        os.save_as_json("./test/schemas.json").unwrap();
        let other_os = OrderedSchemas::load_json("./test/schemas.json").unwrap();
        assert_eq!(os, other_os);

        std::fs::remove_file("./test/schemas.json").unwrap();
    }
}
