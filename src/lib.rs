// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

pub mod database;
pub mod dataset_info;
pub mod debian_packaging;
pub mod directory_info;
pub mod file_path_utils;
pub mod parser;
pub mod walker;

use std::fs::metadata;
use walkdir::WalkDir;

pub fn run(arg_path: &str) {
    let md = metadata(arg_path).unwrap();

    if md.is_file() {
        // Single file case, no need for parallelism
        parser::extract_includes(arg_path);
        let (function_calls, function_args, declared_functions, declared_function_args) = parser::extract_function_calls(arg_path);
        println!("\n function_calls = {:?} \n function_args = {:?} \n declared_functions = {:?} \n declared_function_args = {:?}\n", function_calls, function_args, declared_functions, declared_function_args);
    } else if md.is_dir() {
        let walker = WalkDir::new(arg_path).into_iter();
        let files: Vec<_> = walker::collect_files(walker);

        // Process files in parallel
        walker::process_files(files);
    }
}
