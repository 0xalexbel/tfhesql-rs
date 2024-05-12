// See: https://doc.rust-lang.org/book/ch09-02-recoverable-errors-with-result.html
// https://doc.rust-lang.org/book/ch17-02-trait-objects.html#using-trait-objects-that-allow-for-values-of-different-types
// Box<dyn Error> === "any kind of error"
// type () === "unit" type === void or anything
// Accept all type that can be converted into a &Path

// mod v1;
mod v2;
// mod v3;

use std::fs::File;
use std::io::{BufWriter, Write};
use tfhe::{ClientKey, CompressedServerKey, ConfigBuilder, ServerKey};

const CLIENT_KEY_PATH: &str = "test/test-client-key.json";
const SERVER_KEY_PATH: &str = "test/test-server-key.bin";
const _BOOL_CLIENT_KEY_PATH: &str = "test/test-bool-client-key.json";
const _BOOL_SERVER_KEY_PATH: &str = "test/test-bool-server-key.bin";

// Panic
fn save_json_key<P, K>(path: P, key: &K)
where
    P: AsRef<std::path::Path>,
    K: serde::Serialize,
{
    println!("Path={}", path.as_ref().display());
    let file = File::create(path).unwrap();
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, key).unwrap();
    writer.flush().unwrap();
}

// Panic
fn save_bin_key<P, K>(path: P, key: &K)
where
    P: AsRef<std::path::Path>,
    K: serde::Serialize,
{
    let file = File::create(path).unwrap();
    let mut writer = BufWriter::new(file);
    bincode::serialize_into(&mut writer, key).unwrap();
}

// Panic
fn gen_and_save_new_keys(paths: (&str, &str), use_compressed: bool) -> (ClientKey, ServerKey) {
    let config = ConfigBuilder::default().build();

    let ck = ClientKey::generate(config);
    save_json_key::<&str, ClientKey>(paths.0, &ck);

    let sk: ServerKey;
    if use_compressed {
        let comp_sk = ck.generate_compressed_server_key();
        save_bin_key::<&str, CompressedServerKey>(paths.1, &comp_sk);
        sk = comp_sk.decompress();
    } else {
        sk = ck.generate_server_key();
        save_bin_key::<&str, ServerKey>(paths.1, &sk);
    }

    (ck, sk)
}

pub fn try_load_or_gen_keys(use_compressed: bool) -> (ClientKey, ServerKey) {
    v2::try_load_or_gen_keys_v2((CLIENT_KEY_PATH, SERVER_KEY_PATH), use_compressed)
}
