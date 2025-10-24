// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

pub mod dataset;
pub mod debian_packaging;
pub mod directory_info;
pub mod file_path_utils;
pub mod parsing;

use std::collections::HashMap;
use std::fs::metadata;
use std::path::Path;
use walkdir::WalkDir;

pub fn run(arg_path: &str) {
    use crate::dataset::database::Database;
    use crate::dataset::dataset_ops::find_datasets;
    use crate::parsing::cmake_parser::CMakeParser;
    use crate::parsing::cpp_parser::CPPParser;
    use crate::parsing::parser::{LibParser, LibProcessor, SourceFinder};
    use crate::parsing::python_parser::PythonParser;

    //TODO: This should probably eventually be more "library" like, and less interactive
    // E.g handle info through passing objects instead of printing
    // At some point should replaces printouts with returns, returning some kind of Result<..., errors>

    //C++ database/parser
    let cpp_db = match find_datasets(Some(vec!["linux", "ubuntu"]), None) {
        Ok(datasets) => datasets
            .into_iter()
            .next()
            .expect("Unable to find C++ (Ubuntu) database, please install one"),
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };
    let os_database =
        Database::new(cpp_db.filepath.as_path()).expect("Unable to connect to C++ database");
    let cpp_parser = CPPParser::new(&os_database);

    //Python database/parser
    let python_db = match find_datasets(Some(vec!["python", "pypi"]), None) {
        Ok(datasets) => datasets
            .into_iter()
            .next()
            .expect("Unable to find Python database, please install"),
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };
    let python_database =
        Database::new(python_db.filepath.as_path()).expect("Unable to connect to Python database");
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

    //Process CMake
    //
    //Functions differently because this is just the list of URLs, and doesn't have a mapping
    //Of import -> lib/package the same way that the C++ and Python parsers do
    //Thus cant use the same process_files calls
    let cmake_remotes = if md.is_file() {
        <CMakeParser as LibParser>::extract_includes(Path::new(arg_path))
    } else if md.is_dir() {
        let walker = WalkDir::new(arg_path).into_iter();
        let entries = <CMakeParser as SourceFinder>::collect_source_files(walker);

        entries
            .iter()
            .flat_map(|entry| <CMakeParser as LibParser>::extract_includes(entry))
            .collect()
    } else {
        panic!("Unable to process input path argument");
    };

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

    //Due to the different format, handle CMake includes separately
    for include in cmake_remotes.iter() {
        println!("{include:?}:");
    }
}
