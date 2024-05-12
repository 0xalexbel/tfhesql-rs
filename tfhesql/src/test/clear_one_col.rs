use super::{
    clear_test_in_out,
    simple_queries::{simple_bool_queries, simple_str_queries},
};

#[test]
fn test_str() {
    let sql_in_out = simple_str_queries("*", "table1", "Column1");
    sql_in_out.into_iter().for_each(clear_test_in_out::<String>);

    let sql_in_out = simple_str_queries("Column1", "table1", "Column1");
    sql_in_out.into_iter().for_each(clear_test_in_out::<String>);
}

#[test]
fn test_bool() {
    let sql_in_out = simple_bool_queries("*", "table1", "Column1");
    sql_in_out.into_iter().for_each(clear_test_in_out::<bool>);

    let sql_in_out = simple_bool_queries("Column1", "table1", "Column1");
    sql_in_out.into_iter().for_each(clear_test_in_out::<bool>);
}
