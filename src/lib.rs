// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

pub mod database;
pub mod parser;
pub mod walker;

use std::fs::metadata;
use walkdir::WalkDir;

pub fn run(arg_path: &str) {
    let md = metadata(arg_path).unwrap();

    if md.is_file() {
        // Single file case, no need for parallelism
        parser::extract_includes(arg_path);
    } else if md.is_dir() {
        let walker = WalkDir::new(arg_path).into_iter();
        let files: Vec<_> = walker::collect_files(walker);

        // Process files in parallel
        walker::process_files(files);
    }
}
