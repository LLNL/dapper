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
pub enum PythonImport {
    Module(String),                    //module: import *module*
    Alias(String, String),             //module, alias: import *module* as *alias*
    FromModule(String, String),        //module, item: from *module* import *item*
    FromAlias(String, String, String), //module, item, alias: from *module import *item* as *alias*
}

pub struct PythonParser<'db> {
    package_database: &'db Database, //Used to look up python packages from PyPI
    //TODO: Rename this to os_database once it's used
    //Clippy counts unused-variables without underscore prefix as an error, breaking the CI pipeline
    _os_database: &'db Database, //Used to look up subprocess commands from the OS
}

lazy_static::lazy_static! {
    // Note: __future__ imports are a separate category under TreeSitter,
    // which are not currently selected for parsing
    // So even though "from __future__ import annotations" looks like a "from" import,
    // It will not be included
    // This could be changed in the future if it is deemed necessary
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

lazy_static::lazy_static! {
    static ref PYTHON_STD_LIBS: HashSet<&'static str> = [
        "__future__",
        "_abc",
        "_aix_support",
        "_android_support",
        "_apple_support",
        "_ast",
        "_asyncio",
        "_bisect",
        "_blake2",
        "_bz2",
        "_codecs",
        "_codecs_cn",
        "_codecs_hk",
        "_codecs_iso2022",
        "_codecs_jp",
        "_codecs_kr",
        "_codecs_tw",
        "_collections",
        "_collections_abc",
        "_colorize",
        "_compat_pickle",
        "_compression",
        "_contextvars",
        "_csv",
        "_ctypes",
        "_curses",
        "_curses_panel",
        "_datetime",
        "_dbm",
        "_decimal",
        "_elementtree",
        "_frozen_importlib",
        "_frozen_importlib_external",
        "_functools",
        "_gdbm",
        "_hashlib",
        "_heapq",
        "_imp",
        "_interpchannels",
        "_interpqueues",
        "_interpreters",
        "_io",
        "_ios_support",
        "_json",
        "_locale",
        "_lsprof",
        "_lzma",
        "_markupbase",
        "_md5",
        "_multibytecodec",
        "_multiprocessing",
        "_opcode",
        "_opcode_metadata",
        "_operator",
        "_osx_support",
        "_overlapped",
        "_pickle",
        "_posixshmem",
        "_posixsubprocess",
        "_py_abc",
        "_pydatetime",
        "_pydecimal",
        "_pyio",
        "_pylong",
        "_pyrepl",
        "_queue",
        "_random",
        "_scproxy",
        "_sha1",
        "_sha2",
        "_sha3",
        "_signal",
        "_sitebuiltins",
        "_socket",
        "_sqlite3",
        "_sre",
        "_ssl",
        "_stat",
        "_statistics",
        "_string",
        "_strptime",
        "_struct",
        "_suggestions",
        "_symtable",
        "_sysconfig",
        "_thread",
        "_threading_local",
        "_tkinter",
        "_tokenize",
        "_tracemalloc",
        "_typing",
        "_uuid",
        "_warnings",
        "_weakref",
        "_weakrefset",
        "_winapi",
        "_wmi",
        "_zoneinfo",
        "abc",
        "antigravity",
        "argparse",
        "array",
        "ast",
        "asyncio",
        "atexit",
        "base64",
        "bdb",
        "binascii",
        "bisect",
        "builtins",
        "bz2",
        "cProfile",
        "calendar",
        "cmath",
        "cmd",
        "code",
        "codecs",
        "codeop",
        "collections",
        "colorsys",
        "compileall",
        "concurrent",
        "configparser",
        "contextlib",
        "contextvars",
        "copy",
        "copyreg",
        "csv",
        "ctypes",
        "curses",
        "dataclasses",
        "datetime",
        "dbm",
        "decimal",
        "difflib",
        "dis",
        "doctest",
        "email",
        "encodings",
        "ensurepip",
        "enum",
        "errno",
        "faulthandler",
        "fcntl",
        "filecmp",
        "fileinput",
        "fnmatch",
        "fractions",
        "ftplib",
        "functools",
        "gc",
        "genericpath",
        "getopt",
        "getpass",
        "gettext",
        "glob",
        "graphlib",
        "grp",
        "gzip",
        "hashlib",
        "heapq",
        "hmac",
        "html",
        "http",
        "idlelib",
        "imaplib",
        "importlib",
        "inspect",
        "io",
        "ipaddress",
        "itertools",
        "json",
        "keyword",
        "linecache",
        "locale",
        "logging",
        "lzma",
        "mailbox",
        "marshal",
        "math",
        "mimetypes",
        "mmap",
        "modulefinder",
        "msvcrt",
        "multiprocessing",
        "netrc",
        "nt",
        "ntpath",
        "nturl2path",
        "numbers",
        "opcode",
        "operator",
        "optparse",
        "os",
        "pathlib",
        "pdb",
        "pickle",
        "pickletools",
        "pkgutil",
        "platform",
        "plistlib",
        "poplib",
        "posix",
        "posixpath",
        "pprint",
        "profile",
        "pstats",
        "pty",
        "pwd",
        "py_compile",
        "pyclbr",
        "pydoc",
        "pydoc_data",
        "pyexpat",
        "queue",
        "quopri",
        "random",
        "re",
        "readline",
        "reprlib",
        "resource",
        "rlcompleter",
        "runpy",
        "sched",
        "secrets",
        "select",
        "selectors",
        "shelve",
        "shlex",
        "shutil",
        "signal",
        "site",
        "smtplib",
        "socket",
        "socketserver",
        "sqlite3",
        "sre_compile",
        "sre_constants",
        "sre_parse",
        "ssl",
        "stat",
        "statistics",
        "string",
        "stringprep",
        "struct",
        "subprocess",
        "symtable",
        "sys",
        "sysconfig",
        "syslog",
        "tabnanny",
        "tarfile",
        "tempfile",
        "termios",
        "textwrap",
        "this",
        "threading",
        "time",
        "timeit",
        "tkinter",
        "token",
        "tokenize",
        "tomllib",
        "trace",
        "traceback",
        "tracemalloc",
        "tty",
        "turtle",
        "turtledemo",
        "types",
        "typing",
        "unicodedata",
        "unittest",
        "urllib",
        "uuid",
        "venv",
        "warnings",
        "wave",
        "weakref",
        "webbrowser",
        "winreg",
        "winsound",
        "wsgiref",
        "xml",
        "xmlrpc",
        "zipapp",
        "zipfile",
        "zipimport",
        "zlib",
        "zoneinfo",
    ].into_iter().collect();
}

impl<'db> PythonParser<'db> {
    pub fn new(package_database: &'db Database, os_database: &'db Database) -> Self {
        PythonParser {
            package_database,
            //TODO: Replace "_os_database: os_database" with just "os_database" once in use
            _os_database: os_database,
        }
    }

    pub fn extract_includes(file_path: &Path) -> HashSet<PythonImport> {
        let mut imports: HashSet<PythonImport> = HashSet::new();

        let source_code = match fs::read_to_string(file_path) {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Error reading file {}: {}", file_path.to_str().unwrap(), e);
                return imports;
            }
        };

        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_python::LANGUAGE.into())
            .expect("Error loading C++ grammar");
        let tree = parser.parse(&source_code, None).unwrap();
        let root_node = tree.root_node();

        let mut query_cursor = QueryCursor::new();
        let mut matches =
            query_cursor.matches(&PYTHON_INCLUDE_QUERY, root_node, source_code.as_bytes());

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
                            file_path.to_str().unwrap(),
                            e
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
                    imports.insert(PythonImport::Module(module));
                }
                (Some(module), None, Some(alias)) => {
                    imports.insert(PythonImport::Alias(module, alias));
                }
                (Some(module), Some(item), None) => {
                    imports.insert(PythonImport::FromModule(module, item));
                }
                (Some(module), Some(item), Some(alias)) => {
                    imports.insert(PythonImport::FromAlias(module, item, alias));
                }
                _ => {
                    eprintln!(
                        "Unexpected import format in file {}",
                        file_path.to_str().unwrap()
                    );
                }
            }
        }

        imports
    }

    fn process_files<T>(&self, file_paths: T) -> HashMap<PythonImport, Vec<Vec<String>>>
    where
        T: IntoIterator,
        T::Item: AsRef<Path>,
    {
        //Using Rayon for parallel processing associates wrapping set with Mutex for synchronization
        let global_imports: Mutex<HashSet<PythonImport>> = Mutex::new(HashSet::new());

        par_file_iter(file_paths, |file_path| {
            let file_includes = Self::extract_includes(file_path);
            let mut global_includes = global_imports.lock().unwrap();
            for include in file_includes {
                global_includes.insert(include);
            }
        });

        //Prepare SQL for database query
        //TODO: Double check this, might want to normalize and change query to normalized_name
        let mut sql_statement = self
            .package_database
            .prepare_cached_statement(
                "SELECT package_name FROM v_package_imports WHERE import_as = ?1",
            )
            .expect("Error loading SQL statement");

        let mut query_db = |import_name: &str| -> Result<Vec<String>, _> {
            sql_statement
                .query_map(params![import_name], |row| row.get(0))?
                .collect()
        };

        //Take ownership of the global_includes HashSet back from the Mutex
        //As we are done with parallel processing and so that we can move the underlying data
        let global_imports = mem::take(&mut *global_imports.lock().unwrap());
        let mut global_import_map: HashMap<PythonImport, Vec<Vec<String>>> = HashMap::new();

        for import in global_imports.into_iter() {
            let module_import = match &import {
                PythonImport::Module(mod_name) => mod_name,
                PythonImport::Alias(mod_name, _alias) => mod_name,
                PythonImport::FromModule(mod_name, _item) => mod_name,
                PythonImport::FromAlias(mod_name, _item, _alias) => mod_name,
            };

            if module_import.starts_with(".") {
                //Skip relative imports since we're not likely to find them in the database
                //And we're already likely to be scanning them anyway
                continue;
            }

            //Split the module name since the first portion should be the actual package,
            //E.g. When importing matplotlib.pyplot, the actual module is matplotlib
            let module_import = module_import
                .split_once(".")
                .map(|(first, _)| first.to_string())
                .unwrap_or(module_import.clone());

            if PYTHON_STD_LIBS.contains(module_import.as_str()) {
                //Skip packages if they're part of the Python standard lib
                continue;
            } else if let Ok(libs) = query_db(&module_import) {
                //TODO: Implement heuristic to rank matches
                //For now all results are treated as equally "good"
                global_import_map.insert(import, vec![libs]);
            }
        }

        global_import_map
    }
}

impl SourceFinder for PythonParser<'_> {
    const EXTENSIONS: &'static [&'static str] = &["py"];
}

impl LibParser for PythonParser<'_> {
    fn extract_includes(file_path: &Path) -> HashSet<LangInclude> {
        Self::extract_includes(file_path)
            .into_iter()
            .map(LangInclude::Python)
            .collect()
    }

    fn extract_sys_calls(_file_path: &Path) -> HashSet<LangInclude>
    where
        Self: Sized,
    {
        //Argument _file_path prefixed with underscore to prevent complaints from cargo
        //Rename to "file_path" when implementing
        HashSet::new()
    }
}

impl LibProcessor for PythonParser<'_> {
    fn process_files<T>(&self, file_paths: T) -> HashMap<LangInclude, Vec<Vec<String>>>
    where
        T: IntoIterator,
        T::Item: AsRef<Path>,
    {
        // fn process_files(&self, file_path: Vec<&str>) -> Vec<(LangInclude, Vec<String>)>{
        self.process_files(file_paths)
            .into_iter()
            .map(|(python_include, vec)| (LangInclude::Python(python_include), vec))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_python_includes() {
        let test_file = Path::new("tests/test_files/test.py");
        let imports = PythonParser::extract_includes(test_file);

        //Split imports categories for explicit checks
        let mut norm_imports = HashSet::new();
        let mut alias_imports = HashSet::new();
        let mut from_imports = HashSet::new();
        let mut from_alias_imports = HashSet::new();
        for import in imports.into_iter() {
            match &import {
                PythonImport::Module(_) => {
                    norm_imports.insert(import);
                }
                PythonImport::Alias(_, _) => {
                    alias_imports.insert(import);
                }
                PythonImport::FromModule(_, _) => {
                    from_imports.insert(import);
                }
                PythonImport::FromAlias(_, _, _) => {
                    from_alias_imports.insert(import);
                }
            }
        }

        let exp_norm_imports = [
            PythonImport::Module("requests".to_string()),
            PythonImport::Module("concurrent.futures".to_string()),
            PythonImport::Module("MyCustomModuleV1".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(norm_imports, exp_norm_imports);

        let exp_alias_imports = [
            PythonImport::Alias("numpy".to_string(), "np".to_string()),
            PythonImport::Alias("scipy".to_string(), "sp".to_string()),
            PythonImport::Alias("matplotlib.pyplot".to_string(), "plt".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(alias_imports, exp_alias_imports);

        let exp_from_imports = [
            PythonImport::FromModule("pathlib".to_string(), "Path".to_string()),
            PythonImport::FromModule("tqdm.auto".to_string(), "tqdm".to_string()),
            PythonImport::FromModule("typing".to_string(), "Callable".to_string()),
            PythonImport::FromModule("MyLibrary1".to_string(), "my_func".to_string()),
            PythonImport::FromModule("MyLibrary2.submodule".to_string(), "MyClass".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(from_imports, exp_from_imports);

        let exp_from_alias_imports = [
            PythonImport::FromAlias(
                "..MyRelativeLib".to_string(),
                "rel_func".to_string(),
                "a_func".to_string(),
            ),
            PythonImport::FromAlias(
                "MyConstants".to_string(),
                "ConstantA".to_string(),
                "NewName".to_string(),
            ),
            PythonImport::FromAlias(
                "MyConstants".to_string(),
                "ConstantB".to_string(),
                "NewName2".to_string(),
            ),
        ]
        .into_iter()
        .collect();
        assert_eq!(from_alias_imports, exp_from_alias_imports);
    }
}
