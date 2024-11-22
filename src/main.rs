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
        let system_include_map = std::sync::Mutex::new(std::collections::HashMap::new());
        let user_include_map = std::sync::Mutex::new(std::collections::HashMap::new());

        files.par_iter().for_each(|entry| {
            //println!("{}", entry.path().display());
            let (system_includes, user_includes) =
                extract_includes(entry.path().to_str().unwrap(), &query);

            {
                let mut system_include_map = system_include_map.lock().unwrap();
                for include in system_includes {
                    system_include_map
                        .entry(include)
                        .or_insert_with(Vec::new)
                        .push(entry.path().display().to_string());
                }
            }

            {
                let mut user_include_map = user_include_map.lock().unwrap();
                for include in user_includes {
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

fn extract_includes(file_path: &str, query: &Query) -> (Vec<String>, Vec<String>) {
    let mut system_includes = Vec::new();
    let mut user_includes = Vec::new();

    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_cpp::LANGUAGE.into())
        .expect("Error loading C++ grammar");

    let source_code = match fs::read_to_string(file_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading file {}: {}", file_path, e);
            return (system_includes, user_includes);
        }
    };
    let tree = parser.parse(&source_code, None).unwrap();
    let root_node = tree.root_node();

    let mut query_cursor = QueryCursor::new();
    let mut matches = query_cursor.matches(query, root_node, source_code.as_bytes());

    while let Some(m) = matches.next() {
        for capture in m.captures {
            let node = capture.node;
            let capture_name = query.capture_names()[capture.index as usize];
            let mut include_name = node.utf8_text(source_code.as_bytes()).unwrap().chars();
            include_name.next();
            include_name.next_back();
            let include_name = include_name.as_str().to_string();

            match capture_name {
                "system_include" => system_includes.push(include_name),
                "user_include" => user_includes.push(include_name),
                _ => {}
            }
        }
    }

    (system_includes, user_includes)
}
