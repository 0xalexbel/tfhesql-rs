use crate::ClearSqlQuery;
use crate::CompactFheSqlQuery;
use crate::CompressedFheSqlQuery;
use crate::FheSqlError;
use crate::FheSqlQuery;
use crate::OrderedSchemas;
use crate::SqlResultOptions;
use tfhe::ClientKey;
use tfhe::CompactPublicKey;

////////////////////////////////////////////////////////////////////////////////
// FheSqlClient
////////////////////////////////////////////////////////////////////////////////

pub struct FheSqlClient {
    ordered_schemas: OrderedSchemas,
}

impl FheSqlClient {
    /// Creates a new FheSqlClient with a fixed-order list of [`arrow_schema::Schema`]
    pub fn new(schemas: OrderedSchemas) -> Result<Self, FheSqlError> {
        Ok(FheSqlClient {
            ordered_schemas: schemas,
        })
    }

    /// Returns an immutable reference to the client's [OrderedSchemas]
    pub fn ordered_schemas(&self) -> &OrderedSchemas {
        &self.ordered_schemas
    }

    /// Creates a clear SqlQuery from SQL query text
    pub fn clear_sql(
        &self,
        sql: &str,
        options: SqlResultOptions,
    ) -> Result<ClearSqlQuery, FheSqlError> {
        let clear_query = self.build_query(sql, options)?;
        Ok(clear_query)
    }

    /// Creates a FHE encrypted SqlQuery from SQL query text and a [ClientKey]
    pub fn encrypt_sql(
        &self,
        sql: &str,
        key: &ClientKey,
        options: SqlResultOptions,
    ) -> Result<FheSqlQuery, FheSqlError> {
        use crate::encrypt::traits::EncryptRef;
        let clear_query = self.build_query(sql, options)?;
        Ok(FheSqlQuery::encrypt_ref(&clear_query, key))
    }

    /// Creates a compressed FHE encrypted SqlQuery from SQL query text and a [ClientKey]
    pub fn encrypt_compressed_sql(
        &self,
        sql: &str,
        key: &ClientKey,
        options: SqlResultOptions,
    ) -> Result<CompressedFheSqlQuery, FheSqlError> {
        use crate::encrypt::traits::EncryptRef;
        let clear_query = self.build_query(sql, options)?;
        Ok(CompressedFheSqlQuery::encrypt_ref(&clear_query, key))
    }

    /// Creates a compact FHE encrypted SqlQuery from SQL query text and a [ClientKey]
    pub fn encrypt_compact_sql(
        &self,
        sql: &str,
        key: &CompactPublicKey,
        options: SqlResultOptions,
    ) -> Result<CompactFheSqlQuery, FheSqlError> {
        use crate::encrypt::traits::EncryptRef;
        let clear_query = self.build_query(sql, options)?;
        Ok(CompactFheSqlQuery::encrypt_ref(&clear_query, key))
    }

    /// Creates a trivialy encrypted SqlQuery from SQL query text
    pub fn trivial_encrypt_sql(
        &self,
        sql: &str,
        options: SqlResultOptions,
    ) -> Result<FheSqlQuery, FheSqlError> {
        use crate::encrypt::traits::TrivialEncryptRef;
        let clear_query = self.build_query(sql, options)?;
        Ok(FheSqlQuery::encrypt_trivial_ref(&clear_query))
    }
}

////////////////////////////////////////////////////////////////////////////////

impl FheSqlClient {
    fn build_query(
        &self,
        sql: &str,
        options: SqlResultOptions,
    ) -> Result<ClearSqlQuery, FheSqlError> {
        use crate::bitops::RefNot;
        use crate::query::sql_query::ClearTableBoolMaskHeader;
        use crate::query::sql_query_tree::ClearSqlQueryTree;
        use crate::sql_ast::and_or_ast::{compute_ast_tree, AstTreeResult};
        use crate::sql_ast::parser::*;
        use crate::sql_ast::*;
        use crate::uint::mask::ClearBoolMask;
        use sqlparser::{dialect::GenericDialect, parser::Parser};

        let dialect = GenericDialect {}; // or AnsiDialect
        let statements = Parser::parse_sql(&dialect, sql).unwrap();

        // First quick synthax validation
        // Eliminate unsupported SQL features
        validate_statements(&statements, sql)?;

        let statement_ref = &statements[0];

        // Retrieve DISTINCT option if any
        let is_distinct = match get_statement_distinct_option(statement_ref)? {
            Some(o) => match o {
                sqlparser::ast::Distinct::Distinct => true,
                sqlparser::ast::Distinct::On(_) => {
                    return Err(FheSqlError::UnsupportedSqlQuery(
                        "DISTINCT ON clause is not supported.".to_string(),
                    ))
                }
            },
            None => false,
        };

        let from = get_statement_from(statement_ref)?;
        let projection = get_statement_projections(statement_ref)?;

        let table_mask: ClearBoolMask = self.ordered_schemas.compute_table_mask(&from.0);
        let table_index = match table_mask.index_of_first_set() {
            Some(idx) => idx,
            None => return Err(FheSqlError::syntax_error("No table selected")),
        };
        let table_schema = self.ordered_schemas.schema(table_index);

        let field_mask: ClearBoolMask = self
            .ordered_schemas
            .compute_schema_field_mask(table_index, projection)?;
        assert_eq!(field_mask.len(), self.ordered_schemas.max_num_fields());

        // Precomputed on the client side
        let not_field_mask: ClearBoolMask = field_mask.ref_not();

        let header = ClearTableBoolMaskHeader {
            table_mask,
            field_mask,
            not_field_mask,
        };

        let where_expr = match statement_ref.compile_where(table_schema)? {
            Some(we) => we,
            None => {
                // no WHERE clause is equivalent to TRUE
                let where_tree = ClearSqlQueryTree::build(AstTreeResult::Boolean(true))?;
                return Ok(ClearSqlQuery::new(
                    header,
                    is_distinct,
                    where_tree,
                    self.ordered_schemas.clone(),
                    options,
                ));
            }
        };

        let ast_tree = compute_ast_tree(
            &where_expr,
            table_schema,
            self.ordered_schemas.max_num_fields(),
        )?;
        let ast_tree_is_false = ast_tree.is_false();

        let where_tree = ClearSqlQueryTree::build(ast_tree)?;

        if ast_tree_is_false {
            Ok(ClearSqlQuery::new_empty(
                self.ordered_schemas.clone(),
                options,
            ))
        } else {
            Ok(ClearSqlQuery::new(
                header,
                is_distinct,
                where_tree,
                self.ordered_schemas.clone(),
                options,
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use arrow_array::*;
    use arrow_cast::pretty::print_batches;
    use arrow_schema::*;
    use std::sync::Arc;

    use crate::{
        FheRunSqlQuery, FheSqlClient, FheSqlServer, OrderedTables, SqlResultOptions, Table,
    };

    #[test]
    fn test_one_str() {
        //let value = "abcdefghijklmnopqrstuvwxyz012345";
        let value = "abcdefghijklmnopqrstuvwxyz";
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "Column1",
                DataType::Utf8,
                false,
            )])),
            vec![Arc::new(StringArray::from(vec![value]))],
        )
        .unwrap();

        let t1 = Table::new("table1", batch.clone());
        let tables: OrderedTables = OrderedTables::new(vec![t1]).unwrap();
        let public_ordered_schemas = tables.ordered_schemas();

        let sql_client = FheSqlClient::new(public_ordered_schemas.clone()).unwrap();

        let sql = format!(
            "SELECT DISTINCT Column1 FROM table1 WHERE Column1 = '{}'",
            value
        );
        let expected_batch = RecordBatch::try_new(
            batch.schema_ref().clone(),
            vec![Arc::new(StringArray::from(vec![value]))],
        )
        .unwrap();

        let clear_sql_query = sql_client
            .clear_sql(&sql, SqlResultOptions::default())
            .unwrap();
        let sql_result = FheSqlServer::run(&clear_sql_query, &tables).unwrap();

        let rb = sql_result.clone()
            .into_record_batch()
            .unwrap();

        assert_eq!(rb, expected_batch);

        print_batches(&[rb]).unwrap();

        #[cfg(feature = "stats")]
        sql_result.print_detailed_stats();
    }
}
