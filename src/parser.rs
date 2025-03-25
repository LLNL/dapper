// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use std::fs;
use streaming_iterator::StreamingIterator;
use tree_sitter::{Parser, Query, QueryCursor};

lazy_static::lazy_static! {
    static ref INCLUDE_QUERY: Query = Query::new(
        &tree_sitter_cpp::LANGUAGE.into(),
        r#"
        (preproc_include
            (system_lib_string) @system_include
        )
        (preproc_include
            (string_literal) @user_include
        )
        "#
    ).expect("Error creating query");
}

pub fn extract_includes(file_path: &str) -> (Vec<String>, Vec<String>) {
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
    let mut matches = query_cursor.matches(&INCLUDE_QUERY, root_node, source_code.as_bytes());

    while let Some(m) = matches.next() {
        for capture in m.captures {
            let node = capture.node;
            let capture_name = INCLUDE_QUERY.capture_names()[capture.index as usize];
            let mut include_name = match node.utf8_text(source_code.as_bytes()) {
                Ok(text) => text.chars(),
                Err(e) => {
                    eprintln!(
                        "Error reading include name as utf8 text from {}: {}",
                        file_path, e
                    );
                    continue;
                }
            };
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

lazy_static::lazy_static! {
    static ref FUNCTION_CALL_QUERY: Query = Query::new(
        &tree_sitter_cpp::LANGUAGE.into(),
        r#"
        (call_expression
            function: (identifier) @function_name
            arguments: (argument_list) @arg_list
        )
        (function_declarator
            declarator: (identifier) @function_name
            parameters: (parameter_list) @function_arg_list
        )
        "#
    ).expect("Error creating query");
}

pub fn extract_function_calls(file_path: &str) -> (Vec<String>, Vec<String>) {
    let mut function_calls = Vec::new();
    let mut function_args = Vec::new();

    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_cpp::LANGUAGE.into())
        .expect("Error loading C++ grammar");

    let source_code = match fs::read_to_string(file_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading file {}: {}", file_path, e);
            return (function_calls, function_args);
        }
    };
    let tree = parser.parse(&source_code, None).unwrap();
    let root_node = tree.root_node();

    let mut query_cursor = QueryCursor::new();
    let mut matches = query_cursor.matches(&FUNCTION_CALL_QUERY, root_node, source_code.as_bytes());

    while let Some(m) = matches.next() {
        for capture in m.captures {
            let node = capture.node;
            let capture_name = FUNCTION_CALL_QUERY.capture_names()[capture.index as usize];
            let include_name = match node.utf8_text(source_code.as_bytes()) {
                Ok(text) => text.to_string(),
                Err(e) => {
                    eprintln!(
                        "Error reading include name as utf8 text from {}: {}",
                        file_path, e
                    );
                    continue;
                }
            };
            match capture_name {
                "function_name" => function_calls.push(include_name),
                "arg_list" | "function_arg_list" => function_args.push(include_name),
                _ => {}
            }
        }
    }

    (function_calls, function_args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_includes() {
        let (system_includes, user_includes) = extract_includes("tests/test_files/test.cpp");
        assert_eq!(system_includes, vec!["iostream"]);
        assert_eq!(user_includes, vec!["test.h"]);
    }
    #[test]
    fn test_extract_function_calls() {
        let (function_calls, function_args) = extract_function_calls("tests/test_files/test.cpp");
        assert_eq!(function_calls, vec!["main"]);
        assert_eq!(function_args, vec!["(int argc, char* argv[])"]);
    }
}
