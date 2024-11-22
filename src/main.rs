// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use rayon::prelude::*;
use std::env;
use std::fs;
use std::fs::metadata;
use streaming_iterator::StreamingIterator;
use tree_sitter::{Parser, Query, QueryCursor};
use walkdir::{DirEntry, WalkDir};

fn main() {
    let args: Vec<String> = env::args().collect();
    println!("{args:?}");
    let arg_path = &args[1];
    let md = metadata(arg_path).unwrap();

    // Create the Query object once
    let query = Query::new(
        &tree_sitter_cpp::LANGUAGE.into(),
        r#"
        (preproc_include
            (system_lib_string) @system_include
        )
        (preproc_include
            (string_literal) @user_include
        )
        "#,
    )
    .expect("Failed to create query");

    if md.is_file() {
        // Single file case, no need for parallelism
        extract_includes(arg_path, &query);
    } else if md.is_dir() {
        let walker = WalkDir::new(arg_path).into_iter();
        let files: Vec<_> = walker
            .filter_entry(|e| is_source_code(e) || e.file_type().is_dir())
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
            .collect();

        // Use Rayon to process files in parallel
        files.par_iter().for_each(|entry| {
            println!("{}", entry.path().display());
            extract_includes(entry.path().to_str().unwrap(), &query);
        });
    }
}

fn is_source_code(entry: &DirEntry) -> bool {
    let extensions = vec![
        "h", "hpp", "c", "cc", "hh", "cpp", "h++", "c++", "cxx", "hxx", "ixx", "cppm", "ccm",
        "c++m", "cxxm",
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

fn extract_includes(file_path: &str, query: &Query) {
    // Create a new parser for each file to ensure thread safety
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_cpp::LANGUAGE.into())
        .expect("Error loading C++ grammar");

    let source_code = fs::read_to_string(file_path).expect("Should be able to read the file");
    let tree = parser.parse(&source_code, None).unwrap();
    let root_node = tree.root_node();

    let mut query_cursor = QueryCursor::new();
    let mut matches = query_cursor.matches(query, root_node, source_code.as_bytes());

    // Iterate over matches and print the captured include files
    while let Some(m) = matches.next() {
        for capture in m.captures {
            let node = capture.node;
            let capture_name = &query.capture_names()[capture.index as usize];
            let mut include_name = node.utf8_text(source_code.as_bytes()).unwrap().chars();
            // Trim first and last characters from included file name
            include_name.next(); // < or "
            include_name.next_back(); // > or "
            let include_name = include_name.as_str();
            println!("{capture_name}: {include_name}")
        }
    }
}
