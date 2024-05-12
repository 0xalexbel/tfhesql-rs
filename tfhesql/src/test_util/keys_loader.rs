// See: https://doc.rust-lang.org/book/ch09-02-recoverable-errors-with-result.html
// https://doc.rust-lang.org/book/ch17-02-trait-objects.html#using-trait-objects-that-allow-for-values-of-different-types
// Box<dyn Error> === "any kind of error"
// type () === "unit" type === void or anything
// Accept all type that can be converted into a &Path

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use tfhe::{ClientKey, CompressedServerKey, ConfigBuilder, ServerKey};

use super::tfhesql_test_keys_dir;

const CLIENT_KEY_JSON: &str = "test-client-key.json";
const SERVER_KEY_JSON: &str = "test-server-key.bin";

// Panic
fn save_json_key<P, K>(path: P, key: &K)
where
    P: AsRef<std::path::Path>,
    K: serde::Serialize,
{
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

fn try_load_or_gen_keys(
    paths: (&str, &str),
    use_compressed: bool,
) -> (ClientKey, ServerKey) {
    if let Ok(keys) = try_load_keys(paths, use_compressed) {
        return keys;
    }
    gen_and_save_new_keys(paths, use_compressed)
}

fn try_load_keys(
    paths: (&str, &str),
    use_compressed: bool,
) -> std::io::Result<(ClientKey, ServerKey)> {
    let ck = try_load_json_key::<ClientKey>(paths.0)?;

    if use_compressed {
        let comp_sk = try_load_bin_key::<CompressedServerKey>(paths.1)?;
        Ok((ck, comp_sk.decompress()))
    } else {
        let sk = try_load_bin_key::<ServerKey>(paths.1)?;
        Ok((ck, sk))
    }
}

fn try_load_json_key<K>(path: &str) -> std::io::Result<K>
where
    K: serde::de::DeserializeOwned,
{
    let k_path = std::path::Path::new(path);
    if !k_path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Path does not exist: {}", k_path.display()),
        ));
    }

    let file = File::open(k_path)?;

    let reader = BufReader::new(file);
    let k = match serde_json::from_reader(reader) {
        Ok(k) => k,
        Err(e) => return Err(std::io::Error::from(e)),
    };

    Ok(k)
}

fn try_load_bin_key<K>(path: &str) -> std::io::Result<K>
where
    K: serde::de::DeserializeOwned,
{
    let k_path = std::path::Path::new(path);
    if !k_path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Path does not exist: {}", k_path.display()),
        ));
    }

    let file = File::open(k_path)?;

    let reader = BufReader::new(file);
    let key = match bincode::deserialize_from(reader) {
        Ok(k) => k,
        Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())),
    };

    Ok(key)
}

pub fn try_load_or_gen_test_keys(use_compressed: bool) -> (ClientKey, ServerKey) {
    let dir = tfhesql_test_keys_dir();
    let client_key_path = format!("{}/{}", dir, CLIENT_KEY_JSON);
    let server_key_path = format!("{}/{}", dir, SERVER_KEY_JSON);
    try_load_or_gen_keys((&client_key_path, &server_key_path), use_compressed)
}
