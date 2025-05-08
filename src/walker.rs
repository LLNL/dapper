// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use crate::database::{
    open_database, prepare_statement,
    query_linux_package_files, query_python_package_imports
};
use crate::directory_info::get_base_directory;
use walkdir::{DirEntry, IntoIter};

pub fn collect_cpp_files(walker: IntoIter) -> Vec<DirEntry> {
    walker
        .filter_entry(|e| is_cpp_source_code(e) || e.file_type().is_dir())
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .collect()
}

pub fn process_cpp_files(files: Vec<DirEntry>) {
    use rayon::prelude::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    let mut database_dir =
        get_base_directory().expect("Unable to get the user's local data directory");
    database_dir.push("LinuxPackageDB.db");

    let conn = open_database(database_dir).expect("Failed to open database");
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
            crate::parser::extract_cpp_includes(entry.path().to_str().unwrap());

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
        let matching_packages: Vec<_> = query_linux_package_files(&mut stmt, &include_lower)
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

fn is_cpp_source_code(entry: &DirEntry) -> bool {
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


pub fn collect_python_files(walker: IntoIter) -> Vec<DirEntry> {
    walker
        .filter_entry(|e| is_python_source_code(e) || e.file_type().is_dir())
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file())
        .collect()
}

pub fn process_python_files(files: Vec<DirEntry>) {
    use rayon::prelude::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use crate::parser::PythonImport;

    let mut database_dir =
        get_base_directory().expect("Unable to get the user's local data directory");
    database_dir.push("PyPIPackageDB.db");

    let conn = open_database(database_dir).expect("Failed to open database");
    let mut stmt = prepare_statement(
        &conn,
        "SELECT package_name FROM v_package_imports WHERE import_as = ?1",
    )
    .expect("Failed to prepare statement");

    // Use Rayon to process files in parallel -- need to share the maps between threads
    let import_map = Mutex::new(HashMap::new());

    files.par_iter().for_each(|entry| {
        let imports = crate::parser::extract_python_includes(entry.path().to_str().unwrap());
        
        {
            let mut import_map = import_map.lock().unwrap();
            for import in imports {
                let import_name = match import {
                    PythonImport::Module(module_name) => module_name,
                    PythonImport::Alias(module_name, _) => module_name,
                    PythonImport::FromModule(module_name, _) => module_name,
                    PythonImport::FromAlias(module_name, _, _) => module_name,
                };
                
                if import_name.starts_with(".") {
                    //Skip relative imports since we're not likely to find them in the database
                    //And we're already likely to be scanning them anyway
                    continue
                }
                //Split the module name since the first portion should be the actual package, 
                //E.g. When importing matplotlib.pyplot, the actual module is matplotlib
                let import_name = import_name
                    .split_once(".")
                    .map(|(first, _)| first.to_string())
                    .unwrap_or(import_name);
                
                import_map
                    .entry(import_name)
                    .or_insert_with(Vec::new)
                    .push(entry.path().display().to_string());
            }
        }
    });
    
    let import_map = import_map.lock().unwrap();
    let unique_imports: Vec<_> = import_map.keys().cloned().collect();
    
    for import in unique_imports {
        let matching_packages: Vec<_> = query_python_package_imports(&mut stmt, &import)
            .unwrap_or_else(|_| Vec::new())//Effectively skip if no match is found
            .into_iter()
            .collect();

        if !matching_packages.is_empty() {
            println!("Include: {} -> Packages: {:?}", import, matching_packages);
            println!()
        }
    }

    // print size of the map
    println!(
        "Import Map Size: {}",
        import_map.len()
    );
}

fn is_python_source_code(entry: &DirEntry) -> bool {
    let extensions = vec!["py"];
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
