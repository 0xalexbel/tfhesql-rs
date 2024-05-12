use std::io::BufReader;
use std::{fs::File, io};
use tfhe::{ClientKey, CompressedServerKey, ServerKey};

use super::gen_and_save_new_keys;

pub(super) fn try_load_or_gen_keys_v2(
    paths: (&str, &str),
    use_compressed: bool,
) -> (ClientKey, ServerKey) {
    if let Ok(keys) = try_load_keys_v2(paths, use_compressed) {
        return keys;
    }
    gen_and_save_new_keys(paths, use_compressed)
}

fn try_load_keys_v2(
    paths: (&str, &str),
    use_compressed: bool,
) -> std::io::Result<(ClientKey, ServerKey)> {
    let ck = try_load_json_key_v2::<ClientKey>(paths.0)?;

    if use_compressed {
        let comp_sk = try_load_bin_key_v2::<CompressedServerKey>(paths.1)?;
        Ok((ck, comp_sk.decompress()))
    } else {
        let sk = try_load_bin_key_v2::<ServerKey>(paths.1)?;
        Ok((ck, sk))
    }
}

fn try_load_json_key_v2<K>(path: &str) -> std::io::Result<K>
where
    K: serde::de::DeserializeOwned,
{
    let k_path = std::path::Path::new(path);
    if !k_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
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

fn try_load_bin_key_v2<K>(path: &str) -> std::io::Result<K>
where
    K: serde::de::DeserializeOwned,
{
    let k_path = std::path::Path::new(path);
    if !k_path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Path does not exist: {}", k_path.display()),
        ));
    }

    let file = File::open(k_path)?;

    let reader = BufReader::new(file);
    let key = match bincode::deserialize_from(reader) {
        Ok(k) => k,
        Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e.to_string())),
    };

    Ok(key)
}
