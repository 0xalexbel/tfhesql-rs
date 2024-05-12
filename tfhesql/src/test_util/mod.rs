use arrow_array::RecordBatch;
use arrow_cast::pretty::pretty_format_batches;
use std::{error::Error, path::PathBuf};
mod keys_loader;

pub use keys_loader::try_load_or_gen_test_keys;

pub fn broadcast_set_server_key(sk: &tfhe::ServerKey) {
    let pool_sk = sk.clone();
    ::rayon::broadcast(move |_| {
        let thread_local_sk = pool_sk.clone();
        tfhe::set_server_key(thread_local_sk);
    });
}

pub fn tfhesql_test_data_dir() -> String {
    match get_test_dir("TFHESQL_TEST_DATA", "../tfhesql/test/data") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get test data dir: {err}"),
    }
}

pub(self) fn tfhesql_test_keys_dir() -> String {
    match get_test_dir("TFHESQL_TEST_DATA", "../tfhesql/test/keys") {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get test keys dir: {err}"),
    }
}

pub fn tfhesql_test_db_dir(db: &str) -> String {
    let submodule_data = format!("../tfhesql/test/data/{}", db);
    match get_test_dir("TFHESQL_TEST_DATA", &submodule_data) {
        Ok(pb) => pb.display().to_string(),
        Err(err) => panic!("failed to get arrow data dir: {err}"),
    }
}

pub fn tfhesql_test_db_file(db: &str, file: &str) -> String {
    let dir = tfhesql_test_db_dir(db);
    let pb = PathBuf::from(dir).join(file);
    let pb_s = pb.display().to_string();
    if pb.is_file() {
        pb_s
    } else {
        panic!("failed to get tfhesql csv file: {pb_s}")
    }
}

pub fn print_pretty_batches(results: &[RecordBatch]) -> Result<(), Box<dyn Error>> {
    match pretty_format_batches(results) {
        Ok(s) => {
            println!("{}", s);
            Ok(())
        }
        Err(err) => Err(err.to_string().into()),
    }
}

fn get_test_dir(udf_env: &str, submodule_data: &str) -> Result<PathBuf, Box<dyn Error>> {
    // Try user defined env.
    if let Ok(dir) = std::env::var(udf_env) {
        let trimmed = dir.trim().to_string();
        if !trimmed.is_empty() {
            let pb = PathBuf::from(trimmed);
            if pb.is_dir() {
                return Ok(pb);
            } else {
                return Err(format!(
                    "the data dir `{}` defined by env {} not found",
                    pb.display(),
                    udf_env
                )
                .into());
            }
        }
    }

    let dir = env!("CARGO_MANIFEST_DIR");
    let pb = PathBuf::from(dir).join(submodule_data);
    if pb.is_dir() {
        Ok(pb)
    } else {
        Err(format!(
            "env `{}` is undefined or has empty value, and the pre-defined data dir `{}` not found",
            udf_env,
            pb.display(),
        ).into())
    }
}
