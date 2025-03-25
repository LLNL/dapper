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
use tree_sitter::{Parser, Query, QueryCapture, QueryCursor};

use super::parser::{par_file_iter, LibProcessor};
use super::parser::{LangInclude, LibParser, SourceFinder, SystemProgram};

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

// tree-sitter query for called functions and their arguments
lazy_static::lazy_static! {
    static ref SYS_CALL_QUERY: Query = Query::new(
        &tree_sitter_cpp::LANGUAGE.into(),
        r#"
        (call_expression
            function: (identifier) @function_name
            arguments: (argument_list) @arg_list
        )
        "#
    ).expect("Error creating query");
}

// tree-sitter-bash query for extracting commands
lazy_static::lazy_static! {
    static ref SYS_CALL_QUERY_BASH: Query = Query::new(
        &tree_sitter_bash::LANGUAGE.into(),
        r#"
        (command
            name: (command_name) @cmd_name
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

    pub fn extract_sys_calls(file_path: &Path) -> HashSet<LangInclude> {
        let mut calls = HashSet::new(); // variable to hold the final grouping of calls
        let source_code = match fs::read_to_string(file_path) // read the file into a string
        {
            Ok(content) => content,
            Err(e) =>
            {
                eprintln!("Error reading {}: {}", file_path.to_str().unwrap(), e);
                return calls;
            }
        };

        // parse with tree-sitter
        let mut parser = Parser::new(); // create a new parser
        parser
            .set_language(&tree_sitter_cpp::LANGUAGE.into()) // set the parser language
            .expect("Error loading C++ grammar");
        let tree = parser.parse(&source_code, None).unwrap(); // create a tree
        let root = tree.root_node(); // set the root node

        let mut query_cursor = QueryCursor::new(); // object to query the tree
        let mut matches = query_cursor.matches(&SYS_CALL_QUERY, root, source_code.as_bytes()); // look for matches in the src file as bytes

        while let Some(m) = matches.next()
        // loop to process each match that is found
        {
            // capture slots
            let mut func_name: Option<String> = None; // variable to hold the function name
            let mut args_node = None; // variable to hold the args

            for QueryCapture { node, index, .. } in m.captures
            // for loop to loop over the matches
            {
                let capture_name = SYS_CALL_QUERY.capture_names()[*index as usize]; // represents the current capture
                match capture_name // set the func_name and args_node variables to what was in the capture
                {
                    "function_name" => 
                    {
                        if let Ok(t) = node.utf8_text(source_code.as_bytes())
                        {
                            func_name = Some(t.to_string());
                        }
                    }
                    "arg_list" => 
                    {
                        args_node = Some(node);
                    }
                    _ => {}
                }
            }

            if let (Some(f), Some(arg_list_node)) = (func_name, args_node)
            // check if both variables are not None
            {
                if !Self::is_likely_syscall(&f)
                // if not a syscall that spawns subprocesses, then continue
                {
                    continue;
                }
                // Prepare node for tree-sitter-bash parsing
                let mut saw_one = false; // bool to track if string literal found
                let mut stack = vec![*arg_list_node];
                while let Some(node) = stack.pop() {
                    if node.kind() == "string_literal" {
                        let raw = node
                            .utf8_text(source_code.as_bytes())
                            .unwrap()
                            .trim_matches('"');
                        // println!("DEBUG: Evaluating string literal: '{}'", raw);
                        let parsed =
                            Self::parse_bash_command(raw).unwrap_or_else(|| raw.to_string());
                        let cmd = std::path::Path::new(&parsed)
                            .file_name()
                            .map(|os| os.to_string_lossy().into_owned())
                            .unwrap_or_else(|| parsed.to_string());

                        calls.insert(LangInclude::OS(SystemProgram::Application(cmd)));
                        saw_one = true;
                    }
                    if saw_one {
                        break; // stop after the first literal which should return the name of a program
                    }
                    let mut child_cursor = node.walk();
                    for child in node.children(&mut child_cursor) {
                        stack.push(child);
                    }
                }
            }
        }

        calls
    }

    fn is_likely_syscall(func: &str) -> bool {
        let lower = func.to_lowercase();
        matches!(lower.as_str(), |"system"| "execlp" | "execve")
    }

    fn parse_bash_command(cmd: &str) -> Option<String> {
        // set up tree-sitter-bash parser
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_bash::LANGUAGE.into())
            .expect("Error loading Bash grammar");
        let tree = parser.parse(cmd, None).unwrap();
        let root = tree.root_node();
        let src = cmd.as_bytes();

        // run query to find command names
        let mut query_cursor = QueryCursor::new();
        let mut matches = query_cursor.matches(&SYS_CALL_QUERY_BASH, root, src);
        while let Some(m) = matches.next() {
            for capture in m.captures {
                if SYS_CALL_QUERY_BASH.capture_names()[capture.index as usize] == "cmd_name" {
                    if let Ok(text) = capture.node.utf8_text(src) {
                        return Some(text.to_string());
                    }
                }
            }
        }
        // If no command found, return None
        cmd.split_whitespace().next().map(|s| s.to_string())
    }

    fn process_files<T>(&self, file_paths: T) -> HashMap<LangInclude, Vec<String>>
    where
        T: IntoIterator,
        T::Item: AsRef<Path>,
    {
        //Using Rayon for parallel processing associates wrapping set with Mutex for synchronization
        let global_includes: Mutex<HashSet<CPPInclude>> = Mutex::new(HashSet::new());
        let global_sys_calls: Mutex<HashSet<LangInclude>> = Mutex::new(HashSet::new());

        par_file_iter(file_paths, |file_path| {
            let file_includes = Self::extract_includes(file_path);
            let mut global_includes = global_includes.lock().unwrap();
            for include in file_includes {
                global_includes.insert(include);
            }
            drop(global_includes); //Explicitly drop the lock to avoid deadlocks
            let file_sys_calls = Self::extract_sys_calls(file_path);
            let mut global_sys_calls = global_sys_calls.lock().unwrap();
            for sys_call in file_sys_calls {
                global_sys_calls.insert(sys_call);
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

        let global_sys_calls = mem::take(&mut *global_sys_calls.lock().unwrap());
        let mut global_sys_call_map: HashMap<LangInclude, Vec<String>> = HashMap::new();

        for sys_call in global_sys_calls.into_iter() {
            let func_name = match &sys_call {
                LangInclude::OS(SystemProgram::Application(combined)) => {
                    combined.split('(').next().unwrap().to_lowercase()
                }
                _ => continue,
            };

            if let Ok(libs) = query_db(&func_name) {
                global_sys_call_map.insert(sys_call, libs);
            }
        }
        // Merge both maps into a HashMap<LangInclude, Vec<String>>
        let mut result: HashMap<LangInclude, Vec<String>> = HashMap::new();
        for (inc, pkgs) in global_include_map {
            result.insert(LangInclude::CPP(inc), pkgs);
        }
        for (call, pkgs) in global_sys_call_map {
            result.insert(call, pkgs);
        }
        result
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
        CPPParser::extract_sys_calls(file_path)
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
    #[test]
    fn test_extract_sys_calls_from_real_file() {
        let test_file = Path::new("tests/test_files/test_sys_calls.cpp");
        let calls = CPPParser::extract_sys_calls(test_file);

        // Collect the extracted system call names
        let mut found = HashSet::new();
        for call in calls {
            if let LangInclude::OS(SystemProgram::Application(name)) = call {
                found.insert(name);
            }
        }

        // Check that the expected system calls are found
        // For system("ls -l /tmp"), should also find "ls"
        assert!(
            found.contains("ls"),
            "Expected to find 'ls' command from system()"
        );
        // Should NOT contain unrelated functions
        assert!(!found.contains("rm"), "Should not find 'rm'");
    }
}
