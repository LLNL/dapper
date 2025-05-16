// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Mutex;
use std::{fs, mem};
use streaming_iterator::StreamingIterator;

use rusqlite::params;
use tree_sitter::{Parser, Query, QueryCursor};

use super::parser::{par_file_iter, LibProcessor};
use super::parser::{LangInclude, LibParser, SourceFinder};

use crate::database::Database;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CPPInclude {
    SystemInclude(String),
    UserInclude(String),
}

pub struct CPPParser<'db> {
    database: &'db Database,
}

//TODO: Is there a better way to organize/encapsulate this?
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

//TODO: Maybe replace some of the calls to expect with better error handling?
//Though I'm not sure if those conditions are actually recoverable
impl<'db> CPPParser<'db> {
    pub fn new(database: &'db Database) -> Self {
        CPPParser { database }
    }

    pub fn extract_includes(file_path: &Path) -> HashSet<CPPInclude> {
        let mut includes: HashSet<CPPInclude> = HashSet::new();

        let source_code = match fs::read_to_string(file_path) {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Error reading file {}: {}", file_path.to_str().unwrap(), e);
                return includes;
            }
        };

        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_cpp::LANGUAGE.into())
            .expect("Error loading C++ grammar");
        let tree = parser.parse(&source_code, None).unwrap();
        let root_node = tree.root_node();

        let mut query_cursor = QueryCursor::new();
        let mut matches =
            query_cursor.matches(&CPP_INCLUDE_QUERY, root_node, source_code.as_bytes());

        while let Some(m) = matches.next() {
            for capture in m.captures {
                let node = capture.node;
                let capture_name = CPP_INCLUDE_QUERY.capture_names()[capture.index as usize];
                let mut include_name = match node.utf8_text(source_code.as_bytes()) {
                    Ok(text) => text.chars(),
                    Err(e) => {
                        eprintln!(
                            "Error reading include name as utf8 text from {}: {}",
                            file_path.to_str().unwrap(),
                            e
                        );
                        continue;
                    }
                };
                include_name.next();
                include_name.next_back();
                let include_name = include_name.as_str().to_string();

                match capture_name {
                    "system_include" => {
                        includes.insert(CPPInclude::SystemInclude(include_name));
                    }
                    "user_include" => {
                        includes.insert(CPPInclude::UserInclude(include_name));
                    }
                    _ => {}
                }
            }
        }

        includes
    }

    fn process_files<T>(&self, file_paths: T) -> HashMap<CPPInclude, Vec<String>>
    where
        T: IntoIterator,
        T::Item: AsRef<Path>,
    {
        //Using Rayon for parallel processing associates wrapping set with Mutex for synchronization
        let global_includes: Mutex<HashSet<CPPInclude>> = Mutex::new(HashSet::new());

        par_file_iter(file_paths, |file_path| {
            let file_includes = Self::extract_includes(file_path);
            let mut global_includes = global_includes.lock().unwrap();
            for include in file_includes {
                global_includes.insert(include);
            }
        });

        //Prepare SQL for database query
        //TODO: Double check this, might want to normalize and change query to normalized_name
        let mut sql_statement = self
            .database
            .prepare_cached_statement("SELECT package_name FROM package_files WHERE file_name = ?1")
            .expect("Error loading SQL statement");

        let mut query_db = |file_name: &str| -> Result<Vec<String>, _> {
            sql_statement
                .query_map(params![file_name], |row| row.get(0))?
                .collect()
        };

        //Take ownership of the global_includes HashSet back from the Mutex
        //As we are done with parallel processing and so that we can move the underlying data
        let global_includes = mem::take(&mut *global_includes.lock().unwrap());
        let mut global_include_map: HashMap<CPPInclude, Vec<String>> = HashMap::new();

        for include in global_includes.into_iter() {
            let raw_include = match &include {
                CPPInclude::SystemInclude(include_name) => include_name,
                CPPInclude::UserInclude(include_name) => include_name,
            };
            let include_lower = raw_include.rsplit('/').next().unwrap().to_lowercase();

            if let Ok(libs) = query_db(&include_lower) {
                global_include_map.insert(include, libs);
            }
        }

        global_include_map
    }
}

impl SourceFinder for CPPParser<'_> {
    // C & C++ extensions
    const EXTENSIONS: &'static [&'static str] = &[
        "h", "c", "hh", "cc", "hpp", "cpp", "h++", "c++", "hxx", "cxx", "cppm", "ccm", "c++m",
        "cxxm", "ipp", "ixx", "inl", "tcc", "tpp",
    ];
}

//Uses the concrete implementation and wraps the data in "generic" LangInclude enum
//To allow uniform interface between parsers
impl LibParser for CPPParser<'_> {
    fn extract_includes(file_path: &Path) -> HashSet<LangInclude> {
        Self::extract_includes(file_path)
            .into_iter()
            .map(LangInclude::CPP)
            .collect()
    }

    fn extract_sys_calls(file_path: &Path) -> HashSet<LangInclude>
    where
        Self: Sized,
    {
        HashSet::new()
    }
}

impl LibProcessor for CPPParser<'_> {
    fn process_files<T>(&self, file_paths: T) -> HashMap<LangInclude, Vec<String>>
    where
        T: IntoIterator,
        T::Item: AsRef<Path>,
    {
        // fn process_files(&self, file_path: Vec<&str>) -> Vec<(LangInclude, Vec<String>)>{
        self.process_files(file_paths)
            .into_iter()
            .map(|(cpp_include, vec)| (LangInclude::CPP(cpp_include), vec))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_cpp_includes() {
        let test_file = Path::new("tests/test_files/test.cpp");
        let includes = CPPParser::extract_includes(test_file);

        //Split includes categories for explicit checks
        let mut sys_includes = HashSet::new();
        let mut user_includes = HashSet::new();
        for include in includes.into_iter() {
            match &include {
                CPPInclude::SystemInclude(_) => {
                    sys_includes.insert(include);
                }
                CPPInclude::UserInclude(_) => {
                    user_includes.insert(include);
                }
            }
        }

        let exp_sys_includes = [CPPInclude::SystemInclude("iostream".to_string())]
            .into_iter()
            .collect();
        assert_eq!(sys_includes, exp_sys_includes);

        let exp_user_includes = [CPPInclude::UserInclude("test.h".to_string())]
            .into_iter()
            .collect();
        assert_eq!(user_includes, exp_user_includes);
    }
}
