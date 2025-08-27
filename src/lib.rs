// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

pub mod dataset;
pub mod debian_packaging;
pub mod directory_info;
pub mod file_path_utils;
pub mod parsing;

use crate::directory_info::get_base_directory;

use std::collections::HashMap;
use std::fs::metadata;
use std::path::Path;
use walkdir::WalkDir;

pub fn run(
    arg_path: &str,
    list_datasets: bool,
    remote_datasets: bool,
    install: Option<String>,
    uninstall: Option<String>,
    update: Option<String>,
) {
    use crate::dataset::database::Database;
    use crate::dataset::dataset_info::create_dataset_info;
    use crate::dataset::dataset_ops::{
        install_all_datasets, install_dataset, list_available_datasets, list_installed_datasets,
        uninstall_dataset, update_all_datasets, update_dataset,
    };
    use crate::parsing::cpp_parser::CPPParser;
    use crate::parsing::parser::LibProcessor;
    use crate::parsing::python_parser::PythonParser;

    // Handle install command
    if let Some(dataset_name) = install {
        if dataset_name.to_lowercase() == "all" {
            if let Err(e) = install_all_datasets() {
                eprintln!("Error installing datasets: {e}");
                std::process::exit(1);
            }
        } else if let Err(e) = install_dataset(&dataset_name, true) {
            eprintln!("Error installing dataset '{dataset_name}': {e}");
            std::process::exit(1);
        }
        return;
    }

    // Handle uninstall command
    if let Some(dataset_name) = uninstall {
        if let Err(e) = uninstall_dataset(&dataset_name) {
            eprintln!("Error uninstalling dataset '{dataset_name}': {e}");
            std::process::exit(1);
        }
        return;
    }

    // Handle update command
    if let Some(dataset_name) = update {
        if dataset_name.to_lowercase() == "all" {
            if let Err(e) = update_all_datasets(true) {
                eprintln!("Error updating datasets: {e}");
                std::process::exit(1);
            }
        } else {
            match update_dataset(&dataset_name, false, None, None) {
                Ok(true) => println!("Dataset '{dataset_name}' was updated successfully"),
                Ok(false) => {}
                Err(e) => {
                    eprintln!("Error updating dataset '{dataset_name}': {e}");
                    std::process::exit(1);
                }
            }
        }
        return;
    }

    // Handle list-available-datasets command
    if remote_datasets {
        if let Err(e) = list_available_datasets(None) {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        return;
    }

    // If list_datasets flag is set, returns the list
    if list_datasets {
        if let Err(e) = list_installed_datasets() {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
        return;
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
    let db_path = db_dir.join("LinuxPackageDB.db");
    let os_database = Database::new(&db_path).expect("Unable to connect to C++ database");
    let cpp_parser = CPPParser::new(&os_database);

    //Python database/parser
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
