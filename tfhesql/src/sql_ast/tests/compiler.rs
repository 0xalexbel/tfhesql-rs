#[cfg(test)]
mod test {
    use crate::sql_ast::*;
    use crate::sql_ast::to_parenthesized_string::*;
    use arrow_schema::Field;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn get_schema() -> Schema {
        Schema::new(vec![
            Field::new("a_s", arrow_schema::DataType::Utf8, false),
            Field::new("b_s", arrow_schema::DataType::Utf8, false),
            Field::new("c_s", arrow_schema::DataType::Utf8, false),
            Field::new("d_s", arrow_schema::DataType::Utf8, false),
            Field::new("a_b", arrow_schema::DataType::Boolean, false),
            Field::new("b_b", arrow_schema::DataType::Boolean, false),
            Field::new("c_b", arrow_schema::DataType::Boolean, false),
            Field::new("d_b", arrow_schema::DataType::Boolean, false),
            Field::new("a_i", arrow_schema::DataType::Int32, false),
            Field::new("b_i", arrow_schema::DataType::Int32, false),
            Field::new("c_i", arrow_schema::DataType::Int32, false),
            Field::new("d_i", arrow_schema::DataType::Int32, false),
            Field::new("a_u", arrow_schema::DataType::UInt32, false),
            Field::new("b_u", arrow_schema::DataType::UInt32, false),
            Field::new("c_u", arrow_schema::DataType::UInt32, false),
            Field::new("d_u", arrow_schema::DataType::UInt32, false),
        ])
    }

    #[test]
    fn test() {
        let schema = get_schema();
        let dialect = GenericDialect {}; // or AnsiDialect
        let where_clauses = [
            // "a_s",
            // "a_s > b_i",
            // "(a_s > b_i) AND c_u",
            // "((c_u > d_u) AND a_b)",
            // "((a_b > c_u))",
            "((a_i > b_i) OR NOT (a_i > -(-b_i))) AND ((a_i > b_i) AND (a_i > b_i))"


            //"(a > (f > c))",
            //(((a > 0) AND (((c >= 1) AND (f = true)) OR (f = false))) OR ((a > 1) AND ((c < 1) AND (f = true))))
// f = true c >= 1 false



            //"d OR ((a > (f > c)) AND ((c > d) AND e))"
            
            //
            //(((c < 1) AND (f = true))
            
            // "(a > b) AND (c > d)",
            // "(a > b) AND ((c > d) AND e)",
            // "(a > b) = (c > d)",
            // "(a > b) = c",
            // "a = -3",
            // "a = +3",
            // "-a = 3",
            // "-a = -3",
            // "-a = +3",
            // "+a = 3",
            // "+a = +3",
            // "+a = -3",
            // "-a = b",
            // "-a = -b",
            // "-a = +b",
            // "+a = b",
            // "+a = -b",
            // "+a = +b",
            // "(a > b) AND ((c > d) AND (e > f))",
            // "((a > b) AND (c > d)) AND (e > f)",
            // "(a > (f > g)) AND ((c > d) AND e)",
            // "h OR ((a > (f > g)) AND ((c > d) AND e))",
            // "NOT (a > b)",
            // "(a > b) AND NOT ((c > d) AND (e > f))",
        ];
        let expected_result = [
            // "(a <> 0)",
            // "(a > b)",
            // "((a > b) AND (c <> 0))",
            // "((c > d) AND (e = true))",
            // "((c < 1) AND (f = true))",
            "(a_i > b_i)",

            // "'AND' operator cannot be a leaf in 'a > b AND c'",
            // "",
            // "'AND' operator cannot be a leaf in 'c > d AND e'",
            // "'=' operator cannot be a parent in 'a > b = c > d'",
            // "'=' expression leaf mismatch error in 'a > b = c'",
            // "'=' expression leaf mismatch error in 'a = -3'",
            // "'=' expression leaf mismatch error in 'a = +3'",
            // "",
            // "'=' expression leaf mismatch error in '-a = -3'",
            // "'=' expression leaf mismatch error in '-a = +3'",
            // "'=' expression leaf mismatch error in '+a = 3'",
            // "'=' operator cannot be a parent in '+a = +3'",
            // "'=' operator cannot be a parent in '+a = -3'",
            // "",
            // "'=' expression leaf mismatch error in '-a = -b'",
            // "'=' expression leaf mismatch error in '-a = +b'",
            // "'=' expression leaf mismatch error in '+a = b'",
            // "'=' operator cannot be a parent in '+a = -b'",
            // "'=' operator cannot be a parent in '+a = +b'",
            // "",
            // "",
            // "'>' expression leaf mismatch error in 'a > f > g'",
            // "'OR' operator cannot be a leaf in 'h OR a > f > g AND c > d AND e'",
            // "'NOT' expression pre-processing error in 'NOT a > b'",
            // "'NOT' expression pre-processing error in 'NOT c > d AND e > f'",
        ];
        where_clauses
            .iter()
            .zip(expected_result.iter())
            .for_each(|(w, e)| {
                let sql = format!("SELECT * FROM t WHERE {}", w);
                let statements = Parser::parse_sql(&dialect, &sql).unwrap();
                assert!(!statements.is_empty());
                
                let compiled_where_expr = statements.first().unwrap().compile_where(&schema).unwrap().unwrap();
                assert_eq!(compiled_where_expr.to_parenthesized_string(), *e);
            });
    }
}
