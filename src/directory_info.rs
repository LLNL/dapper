// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use directories::ProjectDirs;
use std::fs;
use std::path::PathBuf;

pub fn get_base_directory() -> Option<PathBuf> {
    if let Some(base_dirs) = ProjectDirs::from("", "", "dapper") {
        let path = base_dirs.data_local_dir().to_path_buf();
        // make sure the directory exists, if it doesn't then create it.
        if let Err(e) = fs::create_dir_all(&path) {
            eprintln!("Failed to create the dapper directory: {e} ");
            return None;
        }
        Some(path)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_base_directory() {
        let base_dir = get_base_directory();
        assert!(base_dir.is_some(), "Base directory should be available");
        let path = base_dir.unwrap();
        assert!(path.is_dir(), "Base directory should be a valid directory");
    }
}
