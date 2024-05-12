use std::{env, ffi::OsStr, fs, path::{Path, PathBuf}};

use crate::FheSqlError;

pub fn absolute_path(path: impl AsRef<Path>) -> Result<PathBuf, FheSqlError> {
    let path = path.as_ref();

    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        match env::current_dir() {
            Ok(cd) => cd.join(path),
            Err(e) => return Err(FheSqlError::IoError(e.to_string())),
        }
    };

    Ok(absolute_path)
}

pub fn extract_filename_without_ext(path: &str) -> Option<String> {
    match Path::new(path).file_stem() {
        Some(os_s) => os_s.to_str().map(|s| s.to_string()),
        None => None
    }
}

pub fn csv_sorted_list_in_dir<P: AsRef<std::path::Path>>(dir: P) -> Vec<String> {
    let mut v: Vec<String> = vec![];
    if !dir.as_ref().exists() {
        return v;
    }
    
    let read_dir = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(_) => return v,
    };

    for r_dir_entry in read_dir {
        let entry = match r_dir_entry {
            Ok(de) => de,
            Err(_) => continue,
        };
        let file_type = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };
        if !file_type.is_file() {
            continue;
        }
        
        let abs_filename = fs::canonicalize(entry.path()).unwrap();
        let ext = match abs_filename.extension() {
            Some(s) => s,
            None => continue,
        };

        if ext == OsStr::new("csv") {
            let s_filename = match abs_filename.to_str() {
                Some(s) => s.to_string(),
                None => continue,
            };
            v.push(s_filename);
        }
    }

    v.sort();
    v
}
