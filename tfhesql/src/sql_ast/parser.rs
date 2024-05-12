use crate::error::FheSqlError;
use sqlparser::ast::{
    Distinct, GroupByExpr, ObjectName, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins, ValueTableMode,
};

use super::helpers::SqlExprIdentifier;

pub fn get_statement_from(statement: &Statement) -> Result<&ObjectName, FheSqlError> {
    match statement {
        Statement::Query(query) => match query.as_ref().body.as_ref() {
            SetExpr::Select(select) => validate_from(&select.from),
            _ => Err(FheSqlError::UnsupportedSqlQuery(query.to_string())),
        },
        _ => Err(FheSqlError::UnsupportedSqlStatement(statement.to_string())),
    }
}

pub fn get_statement_projections(
    statement: &Statement,
) -> Result<&Vec<SelectItem>, FheSqlError> {
    match statement {
        Statement::Query(query) => match query.as_ref().body.as_ref() {
            SetExpr::Select(select) => validate_projection(&select.projection),
            _ => Err(FheSqlError::UnsupportedSqlQuery(query.to_string())),
        },
        _ => Err(FheSqlError::UnsupportedSqlStatement(statement.to_string())),
    }
}

pub fn get_statement_distinct_option(
    statement: &Statement,
) -> Result<Option<Distinct>, FheSqlError> {
    match statement {
        Statement::Query(query) => match query.as_ref().body.as_ref() {
            SetExpr::Select(select) => Ok(select.distinct.clone()),
            _ => Err(FheSqlError::UnsupportedSqlQuery(query.to_string())),
        },
        _ => Err(FheSqlError::UnsupportedSqlStatement(statement.to_string())),
    }
}

pub fn validate_statements(
    statements: &[Statement],
    sql: &str,
) -> Result<(), FheSqlError> {
    if statements.len() != 1 {
        return Err(FheSqlError::UnsupportedSqlQuery(format!(
            "Invalid statements in quey '{}'",
            sql
        )));
    }
    validate_statement(&statements[0])
}

fn validate_statement(statement: &Statement) -> Result<(), FheSqlError> {
    match statement {
        Statement::Query(query) => validate_query(query),
        _ => Err(FheSqlError::UnsupportedSqlStatement(statement.to_string())),
    }
}

fn validate_query(query: &Query) -> Result<(), FheSqlError> {
    if query.with.is_some() {
        return Err(FheSqlError::UnsupportedSqlQuery(query.to_string()));
    }
    if !query.order_by.is_empty() {
        return Err(FheSqlError::UnsupportedSqlQuery(query.to_string()));
    }
    if query.limit.is_some() {
        return Err(FheSqlError::UnsupportedSqlQuery(query.to_string()));
    }
    if !query.limit_by.is_empty() {
        return Err(FheSqlError::UnsupportedSqlQuery(query.to_string()));
    }
    if query.offset.is_some() {
        return Err(FheSqlError::UnsupportedSqlQuery(query.to_string()));
    }
    if query.fetch.is_some() {
        return Err(FheSqlError::UnsupportedSqlQuery(query.to_string()));
    }
    if !query.locks.is_empty() {
        return Err(FheSqlError::UnsupportedSqlQuery(query.to_string()));
    }
    if query.for_clause.is_some() {
        return Err(FheSqlError::UnsupportedSqlQuery(query.to_string()));
    }
    match query.body.as_ref() {
        SetExpr::Select(s) => validate_select_body(s.as_ref()),
        _ => Err(FheSqlError::UnsupportedSqlQuery(query.to_string())),
    }
}

fn validate_select_body(body: &Select) -> Result<(), FheSqlError> {
    validate_projection(&body.projection)?;
    validate_from(&body.from)?;
    if body.top.is_some() {
        return Err(FheSqlError::UnsupportedSqlQuery(
            "TOP clause not supported".to_string(),
        ));
    }
    if body.into.is_some() {
        return Err(FheSqlError::UnsupportedSqlQuery(
            "INTO statement not supported".to_string(),
        ));
    }
    match &body.group_by {
        GroupByExpr::All => {
            return Err(FheSqlError::UnsupportedSqlQuery(
                "GROUP BY clause not supported".to_string(),
            ));
        }
        GroupByExpr::Expressions(exprs) => {
            if !exprs.is_empty() {
                return Err(FheSqlError::UnsupportedSqlQuery(
                    "GROUP BY clause not supported".to_string(),
                ));
            }
        }
    }
    if !body.lateral_views.is_empty() {
        return Err(FheSqlError::UnsupportedSqlQuery(
            "LATERAL VIEW clause not supported".to_string(),
        ));
    }
    if !body.cluster_by.is_empty() {
        return Err(FheSqlError::UnsupportedSqlQuery(
            "CLUSTER BY clause not supported".to_string(),
        ));
    }
    if !body.distribute_by.is_empty() {
        return Err(FheSqlError::UnsupportedSqlQuery(
            "DISTRIBUTE BY clause not supported".to_string(),
        ));
    }
    if !body.sort_by.is_empty() {
        return Err(FheSqlError::UnsupportedSqlQuery(
            "SORT BY clause not supported".to_string(),
        ));
    }
    if body.having.is_some() {
        return Err(FheSqlError::UnsupportedSqlQuery(
            "HAVING clause not supported".to_string(),
        ));
    }
    if !body.named_window.is_empty() {
        return Err(FheSqlError::UnsupportedSqlQuery(
            "WINDOW AS clause not supported".to_string(),
        ));
    }
    if body.qualify.is_some() {
        return Err(FheSqlError::UnsupportedSqlQuery(
            "QUALIFY clause not supported".to_string(),
        ));
    }
    if body.value_table_mode.is_some() {
        match body.value_table_mode.unwrap() {
            ValueTableMode::AsStruct => {
                return Err(FheSqlError::UnsupportedSqlQuery(
                    "AS STRUCT clause not supported".to_string(),
                ))
            }
            ValueTableMode::AsValue => {
                return Err(FheSqlError::UnsupportedSqlQuery(
                    "AS VALUE clause not supported".to_string(),
                ))
            }
        }
    }
    Ok(())
}

fn validate_from(from: &[TableWithJoins]) -> Result<&ObjectName, FheSqlError> {
    if from.len() > 1 {
        return Err(FheSqlError::UnsupportedSqlQuery(
            "SELECT FROM multiple tables is not supported".to_string(),
        ));
    }
    if !from[0].joins.is_empty() {
        return Err(FheSqlError::UnsupportedSqlQuery(
            "SQL JOIN is not supported".to_string(),
        ));
    }
    validate_relation(&from[0].relation)
}

fn validate_relation(relation: &TableFactor) -> Result<&ObjectName, FheSqlError> {
    match relation {
        TableFactor::Table {
            name, alias, args, ..
        } => {
            if alias.is_some() {
                return Err(FheSqlError::UnsupportedSqlQuery(
                    "Table alias not supported".to_string(),
                ));
            }
            if args.is_some() {
                return Err(FheSqlError::UnsupportedSqlQuery(
                    "Table arguments not supported".to_string(),
                ));
            }
            Ok(name)
        }
        _ => Err(FheSqlError::UnsupportedSqlQuery(
            "SELECT FROM relation not supported".to_string(),
        )),
    }
}

fn validate_projection(projections: &Vec<SelectItem>) -> Result<&Vec<SelectItem>, FheSqlError> {
    projections.iter().try_for_each(|projection| {
        match projection {
            SelectItem::ExprWithAlias { .. } => {
                Err(FheSqlError::UnsupportedSqlQuery(
                    "Aliases are not supported".to_string(),
                ))
            }
            SelectItem::QualifiedWildcard(..) => {
                Err(FheSqlError::UnsupportedSqlQuery(
                    "Qualified wildcards are not supported".to_string(),
                ))
            }
            SelectItem::UnnamedExpr(expr) => {
                if !expr.is_identifier() {
                    Err(FheSqlError::UnsupportedSqlQuery(format!(
                        "Unsupported projection expressions '{}'",
                        expr
                    )))
                } else {
                    Ok(())
                }
            }
            _ => Ok(()),
        }
    })?;
    
    Ok(projections)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_array::*;
    use arrow_schema::*;
    //use arrow_cast::pretty::*;
    use sqlparser::ast::Ident;

    use crate::{
        table::{OrderedTables, Table},
        uint::mask::ClearByteMask,
    };

    fn simple_batch_1() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("CustomerID", DataType::Int16, false),
            Field::new("PostalCode", DataType::Int16, false),
            Field::new("Preferences", DataType::Int16, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int16Array::from(vec![21, 22, 23, 24])),
                Arc::new(Int16Array::from(vec![-5, -6, -7, -8])),
                Arc::new(Int16Array::from(vec![33, 34, 35, 36])),
            ],
        )
        .unwrap()
    }

    fn simple_batch_2() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("ProductID", DataType::Int16, false),
            Field::new("Type", DataType::Int16, false),
            Field::new("Style", DataType::Int16, false),
            Field::new("Category", DataType::Int16, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int16Array::from(vec![1, 2, 3])),
                Arc::new(Int16Array::from(vec![5, 6, 7])),
                Arc::new(Int16Array::from(vec![9, 10, 11])),
                Arc::new(Int16Array::from(vec![13, 14, 15])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_table_mask() {
        let t1 = Table::new("table1", simple_batch_1());
        let t2 = Table::new("table2", simple_batch_2());

        let tables: OrderedTables = OrderedTables::new(vec![t1, t2]).unwrap();

        let table_mask = tables.ordered_schemas().compute_table_mask(&[Ident {
            value: "table1".to_string(),
            quote_style: None,
        }]);
        assert_eq!(
            table_mask,
            ClearByteMask {
                mask: vec![u8::MAX, 0]
            }
        );
        assert_eq!(table_mask.index_of_first_set(), Some(0));

        let table_mask = tables.ordered_schemas().compute_table_mask(&[
            Ident {
                value: "table1".to_string(),
                quote_style: None,
            },
            Ident {
                value: "table2".to_string(),
                quote_style: None,
            },
        ]);
        assert_eq!(
            table_mask,
            ClearByteMask {
                mask: vec![u8::MAX, u8::MAX]
            }
        );
        assert_eq!(table_mask.index_of_first_set(), Some(0));

        let table_mask = tables.ordered_schemas().compute_table_mask(&[Ident {
            value: "table2".to_string(),
            quote_style: None,
        }]);
        assert_eq!(
            table_mask,
            ClearByteMask {
                mask: vec![0, u8::MAX]
            }
        );
        assert_eq!(table_mask.index_of_first_set(), Some(1));
    }
}
