// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use crate::database::{open_database, prepare_statement, query_package_files};
use walkdir::{DirEntry, IntoIter};

pub fn collect_files(walker: IntoIter) -> Vec<DirEntry> {
    walker
        .filter_entry(|e| is_source_code(e) || e.file_type().is_dir())
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .collect()
}

pub fn process_files(files: Vec<DirEntry>) {
    use rayon::prelude::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    let conn = open_database("package-files_contents-amd64_long-package-names.db")
        .expect("Failed to open database");
    let mut stmt = prepare_statement(
        &conn,
        "SELECT package_name, file_path FROM package_files WHERE file_name = ?1",
    )
    .expect("Failed to prepare statement");

    // Use Rayon to process files in parallel -- need to share the maps between threads
    let system_include_map = Mutex::new(HashMap::new());
    let user_include_map = Mutex::new(HashMap::new());

    files.par_iter().for_each(|entry| {
        let (system_includes, user_includes) =
            crate::parser::extract_includes(entry.path().to_str().unwrap());

        {
            let mut system_include_map = system_include_map.lock().unwrap();
            for include in system_includes {
                // Corner case: Build system adding local directory to system include path?
                system_include_map
                    .entry(include)
                    .or_insert_with(Vec::new)
                    .push(entry.path().display().to_string());
            }
        }

        {
            let mut user_include_map = user_include_map.lock().unwrap();
            for include in user_includes {
                // TODO Before adding a path, check if the include is satisfied by a file in the source code
                // If it is, downgrade the likelihood of the header file being from a package...
                user_include_map
                    .entry(include)
                    .or_insert_with(Vec::new)
                    .push(entry.path().display().to_string());
            }
        }
    });

    let system_include_map = system_include_map.lock().unwrap();
    let user_include_map = user_include_map.lock().unwrap();

    let unique_system_includes: Vec<_> = system_include_map.keys().cloned().collect();
    let unique_user_includes: Vec<_> = user_include_map.keys().cloned().collect();

    println!("Unique System Includes: {:#?}", unique_system_includes);
    println!("Unique User Includes: {:#?}", unique_user_includes);

    for include in unique_system_includes
        .iter()
        .chain(unique_user_includes.iter())
    {
        let include_lower = include.rsplit('/').next().unwrap().to_lowercase();
        let matching_packages: Vec<_> = query_package_files(&mut stmt, &include_lower)
            .expect("Failed to query package files")
            .into_iter()
            .filter_map(|result| {
                let (package_name, file_path) = result;
                let path_buf = std::path::Path::new(&file_path);
                if path_buf.ends_with(include) && path_buf.to_str().unwrap().contains("include") {
                    return Some((package_name, file_path));
                }
                None
            })
            .collect();

        if !matching_packages.is_empty() {
            println!("Include: {} -> Packages: {:?}", include, matching_packages);
        }
    }

    // print size of each map
    println!(
        "System Unique Include Map Size: {}",
        unique_system_includes.len()
    );
    println!(
        "User Unique Include Map Size: {}",
        unique_user_includes.len()
    );
}

fn is_source_code(entry: &DirEntry) -> bool {
    let extensions = vec![
        "h", "c", "hh", "cc", "hpp", "cpp", "h++", "c++", "hxx", "cxx", "cppm", "ccm", "c++m",
        "cxxm", "ipp", "ixx", "inl", "tcc", "tpp",
    ];
    if entry.file_type().is_file() {
        if let Some(ext) = entry.path().extension() {
            if let Some(ext_str) = ext.to_str() {
                let ext_lower = ext_str.to_lowercase();
                return extensions.iter().any(|&e| e == ext_lower);
            }
        }
    }
    false
}
