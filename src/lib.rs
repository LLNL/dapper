// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

pub mod database;
pub mod dataset_info;
pub mod debian_packaging;
pub mod directory_info;
pub mod file_path_utils;
pub mod parsing;

use crate::directory_info::get_base_directory;
use std::collections::HashMap;
use std::fs::metadata;
use std::path::Path;
use walkdir::WalkDir;
use crate::directory_info::get_base_directory;


pub fn run(arg_path: &str) {
    use crate::database::Database;
    use crate::dataset_info::create_dataset_info;
    use crate::parsing::cpp_parser::CPPParser;
    use crate::parsing::parser::LibProcessor;
    use crate::parsing::python_parser::PythonParser;
    use crate::dataset_info::create_dataset_info;

    // Initialize dataset_info.toml if it doesn't exist
    let db_dir = get_base_directory().expect("Unable to get the user's local data directory");
    match create_dataset_info(Some(db_dir.clone())) {
        Ok(true) => println!("Created dataset_info.toml in {}", db_dir.display()),
        Ok(false) => {},
        Err(e) => {
            eprintln!("Warning: Could not create dataset_info.toml: {}", e);
        }
    }

    // Initialize dataset_info.toml if it doesn't exist
    let db_dir = get_base_directory().expect("Unable to get the user's local data directory");
    match create_dataset_info(Some(db_dir.clone())) {
        Ok(()) => println!("Created dataset_info.toml in {}", db_dir.display()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            // File already exists, no need to print anything
        }
        Err(e) => {
            eprintln!("Warning: Could not create dataset_info.toml: {e}");
        }
    }

    //C++ database/parser
    let db_dir = get_base_directory().expect("Unable to get the user's local data directory");
    let db_path = db_dir.join("LinuxPackageDB.db");
    let os_database = Database::new(&db_path).expect("Unable to connect to C++ database");
    let cpp_parser = CPPParser::new(&os_database);

    //Python database/parser
    let db_dir = get_base_directory().expect("Unable to get the user's local data directory");
    let db_path = db_dir.join("PyPIPackageDB.db");
    let python_database = Database::new(&db_path).expect("Unable to connect to Python database");
    let python_parser = PythonParser::new(&python_database, &os_database);

    //Process includes for all known/supported languages
    let md = metadata(arg_path).unwrap();
    let mut libraries: HashMap<_, _> = HashMap::new();

    //Process C++
    let cpp_libs = if md.is_file() {
        LibProcessor::process_file(&cpp_parser, Path::new(arg_path))
    } else if md.is_dir() {
        LibProcessor::process_dir(&cpp_parser, WalkDir::new(arg_path).into_iter())
    } else {
        panic!("Unable to process input path argument");
    };
    libraries.extend(cpp_libs);

    //Process Python
    let python_libs = if md.is_file() {
        LibProcessor::process_file(&python_parser, Path::new(arg_path))
    } else if md.is_dir() {
        LibProcessor::process_dir(&python_parser, WalkDir::new(arg_path).into_iter())
    } else {
        panic!("Unable to process input path argument");
    };
    libraries.extend(python_libs);

    //Do something more useful with the includes later
    for (include, libs) in libraries.iter() {
        println!("{include:?}:");
        for rank in libs.iter() {
            println!("\t{rank:?}");
        }
        println!();
    }
}
