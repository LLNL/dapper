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

    // C/C++ version
    if md.is_file() {
        // Single file case, no need for parallelism
        parser::extract_cpp_includes(arg_path);
    } else if md.is_dir() {
        let walker = WalkDir::new(arg_path).into_iter();
        let files: Vec<_> = walker::collect_cpp_files(walker);
        
        // Process files in parallel
        walker::process_cpp_files(files);
    }
    
    // Python version
    if md.is_file() {
        let data = parser::extract_python_includes(arg_path);
        println!("{:?}", data);
    } else if md.is_dir() {
        let walker = WalkDir::new(arg_path).into_iter();
        let files: Vec<_> = walker::collect_python_files(walker);
        
        walker::process_python_files(files);
    }
}
