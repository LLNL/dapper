// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use std::fs;
use streaming_iterator::StreamingIterator;
use tree_sitter::{Parser, Query, QueryCursor};

lazy_static::lazy_static! {
    static ref CPP_INCLUDE_QUERY: Query = Query::new(
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

pub fn extract_cpp_includes(file_path: &str) -> (Vec<String>, Vec<String>) {
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
    let mut matches = query_cursor.matches(&CPP_INCLUDE_QUERY, root_node, source_code.as_bytes());

    while let Some(m) = matches.next() {
        for capture in m.captures {
            let node = capture.node;
            let capture_name = CPP_INCLUDE_QUERY.capture_names()[capture.index as usize];
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
    static ref PYTHON_INCLUDE_QUERY: Query = Query::new(
        &tree_sitter_python::LANGUAGE.into(),
        r#"
        (
            import_statement
            name: [
                (dotted_name) @module
                (aliased_import name: (dotted_name) @module alias: (_) @alias)
            ]
        )
        (
            import_from_statement
            module_name: [
                (dotted_name) @module
                (relative_import) @module
            ]
            name: [
                (dotted_name) @item
                (aliased_import name: (dotted_name) @item alias: (_) @alias)
            ]
        )
        "#
    ).expect("Error creating query");
}

#[derive(Debug)]
pub enum PythonImport {
    Module(String), //module: import *module*
    Alias(String, String), //module, alias: import *module* as *alias*
    FromModule(String, String), //module, item: from *module* import *item*
    FromAlias(String, String, String), //module, item, alias: from *module import *item* as *alias*
}

pub fn extract_python_includes(file_path: &str) -> Vec<PythonImport> {
    let mut imports = Vec::new();

    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_python::LANGUAGE.into())
        .expect("Error loading Python grammar");

    let source_code = match fs::read_to_string(file_path) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading file {}: {}", file_path, e);
            return imports;
        }
    };

    let tree = parser.parse(&source_code, None).unwrap();
    let root_node = tree.root_node();

    let mut query_cursor = QueryCursor::new();

    let mut matches = query_cursor.matches(&PYTHON_INCLUDE_QUERY, root_node, source_code.as_bytes());
    while let Some(m) = matches.next() {
        let mut module_name = None;
        let mut item_name = None;
        let mut alias_name = None;

        for capture in m.captures {
            let node = capture.node;
            let capture_name = PYTHON_INCLUDE_QUERY.capture_names()[capture.index as usize];

            let token_value = match node.utf8_text(source_code.as_bytes()) {
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
                "module" => {
                    module_name = Some(token_value);
                }
                "alias" => {
                    alias_name = Some(token_value);
                }
                "item" => {
                    item_name = Some(token_value);
                }
                _ => {}
            }
        }

        // Construct the appropriate PythonImport variant
        match (module_name, item_name, alias_name) {
            (Some(module), None, None) => {
                imports.push(PythonImport::Module(module))
            }
            (Some(module), None, Some(alias)) => {
                imports.push(PythonImport::Alias(module, alias))
            }
            (Some(module), Some(item), None) => {
                imports.push(PythonImport::FromModule(module, item))
            }
            (Some(module), Some(item), Some(alias)) => {
                imports.push(PythonImport::FromAlias(module, item, alias))
            }
            _ => {
                eprintln!("Unexpected import format in file {}", file_path)
            },
        }
    }

    imports
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_cpp_includes() {
        let (system_includes, user_includes) = extract_cpp_includes("tests/test_files/test.cpp");
        assert_eq!(system_includes, vec!["iostream"]);
        assert_eq!(user_includes, vec!["test.h"]);
    }

    #[test]
    fn test_extract_python_includes() {
        let imports = extract_python_includes("tests/test_files/test.py");
    }
}