use super::{
    simple_queries::simple_str_queries,
    triv_test_in_out,
};
use tfhe::set_server_key;
use crate::{test_util::try_load_or_gen_test_keys, test_util::broadcast_set_server_key};

#[test]
fn test_str() {
    let (_ck, sk) = try_load_or_gen_test_keys(false);

    broadcast_set_server_key(&sk);
    set_server_key(sk);

    let sql_in_out = simple_str_queries("*", "table1", "Column1");
    sql_in_out.into_iter().for_each(|s| {
        triv_test_in_out::<String>(s);
    });

    let sql_in_out = simple_str_queries("Column1", "table1", "Column1");
    sql_in_out.into_iter().for_each(|s| {
        triv_test_in_out::<String>(s);
    });
}
