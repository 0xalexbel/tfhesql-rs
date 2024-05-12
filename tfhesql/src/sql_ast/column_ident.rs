use super::data_type::DataType;
use crate::error::FheSqlError;
use sqlparser::ast::Ident;

////////////////////////////////////////////////////////////////////////////////
// ColumnIdent
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ColumnIdent {
    ident: Ident,
    index: i32,
    data_type: DataType,
}

////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for ColumnIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(format!("id:{}, type:{}", self.ident, self.data_type).as_str())
    }
}

impl TryFrom<&arrow_schema::DataType> for ColumnIdent {
    type Error = FheSqlError;

    fn try_from(schema_data_type: &arrow_schema::DataType) -> Result<Self, Self::Error> {
        let data_type = DataType::try_from(schema_data_type)?;
        Ok(ColumnIdent {
            ident: Ident::new(""),
            index: -1,
            data_type,
        })
    }
}

impl From<DataType> for ColumnIdent {
    fn from(data_type: DataType) -> Self {
        ColumnIdent {
            ident: Ident::new(""),
            index: -1,
            data_type,
        }
    }
}

impl ColumnIdent {
    pub fn try_from_ident(
        ident: &Ident,
        schema: &arrow_schema::Schema,
    ) -> Result<Self, FheSqlError> {
        match schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, f)| f.as_ref().name().eq_ignore_ascii_case(&ident.value))
        {
            Some((idx, field_ref)) => Ok(ColumnIdent {
                ident: ident.clone(),
                index: (idx as i32),
                data_type: DataType::try_from(field_ref.as_ref().data_type())?,
            }),
            None => Err(FheSqlError::UnknownColumnName(ident.value.clone())),
        }
    }

    #[inline]
    pub fn index(&self) -> i32 {
        self.index
    }

    #[inline]
    pub fn data_type(&self) -> DataType {
        self.data_type
    }
}
