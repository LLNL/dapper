// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::iter;
use std::path::{Path, PathBuf};
use walkdir;
use walkdir::DirEntry;

use super::cmake_parser::CMakeRemoteInclude;
use super::cpp_parser::CPPInclude;
use super::python_parser::PythonImport;

/// Represents a system program no specific to any particular language
/// Such as `ls`, `dir`, `grep`, `awk`, etc. which can be invoked by any language that can start new processes
/// Such as popen, execv in C++, subprocess in Python, etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SystemProgram {
    Application(String),
}

/// Wrapper enum for the different types of includes/imports to allow for a "generic" trait interface
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LangInclude {
    OS(SystemProgram),
    CPP(CPPInclude),
    Python(PythonImport),
    CMake(CMakeRemoteInclude),
}

// For the most part, all our parsers should implement all the following traits.
// But they are still created as separate, so if we want to define something that only finds files
// but doesn't process them, we can still do that

/// Trait for locating source files for a particular language within a directory
///
/// Suggestions welcome for a better name
pub trait SourceFinder {
    /// Will look for files with these extensions and consider them to be source files
    const EXTENSIONS: &'static [&'static str];

    /// Checks if a given file is a source file for the particular language
    fn is_source_file(entry: &DirEntry) -> bool
    where
        Self: Sized,
    {
        if !entry.file_type().is_file() {
            return false;
        }
        Self::check_extension(entry) || Self::check_other(entry)
    }

    /// Checks if a given file is a source file based off its extension
    fn check_extension(entry: &DirEntry) -> bool
    where
        Self: Sized,
    {
        if let Some(ext) = entry.path().extension() {
            if let Some(ext_str) = ext.to_str() {
                let ext_lower = ext_str.to_lowercase();
                return Self::EXTENSIONS.iter().any(|&e| e == ext_lower);
            }
        }
        false
    }

    /// Checks if a given file is a source file based off some other criteria
    ///
    /// Defaults implementation always returns false, i.e is ignored
    /// Created as a way for implementing structs to provide specific/concrete implementations
    fn check_other(_entry: &DirEntry) -> bool
    where
        Self: Sized,
    {
        false
    }

    /// Finds source files within the directory by checking each file against `is_source_file`
    /// And collects the ones that return true
    fn collect_source_files(walker: walkdir::IntoIter) -> Vec<PathBuf>
    where
        Self: Sized,
    {
        walker
            .filter_entry(|e| Self::is_source_file(e) || e.file_type().is_dir())
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_type().is_file())
            .map(|entry| entry.into_path())
            .collect()
    }
}

/// Trait which allows for extracting the includes/imports from a given source file
pub trait LibParser {
    /// Extracts list of the includes/imports from a given source file for the specific language
    ///
    /// Suggestions welcome for a better name since not all languages call it "includes"
    /// E.g. C++ uses "include", Python uses "import", Rust uses "use", etc.
    fn extract_includes(file_path: &Path) -> HashSet<LangInclude>
    where
        Self: Sized;

    /// Extracts list of the programs executed via calls to functions that create new processes
    /// E.g Popen, Execv, Subprocess, etc.
    ///
    /// TODO: May be refactored in the future, but for now should allow us to get a prototype working
    ///
    /// Actually may not get processed by the LibProcessor itself, since it will have a database
    /// For it's specific language (Ptyhon, .Net, etc) whereas this needs the OS database
    /// So it can be extracted using this function, but not necessarily processed
    fn extract_sys_calls(file_path: &Path) -> HashSet<LangInclude>
    where
        Self: Sized;
}

/// Tait for handling the processing of the includes/imports from a given source file as extracted by LibParser
/// Intended to take/collect files and use the Dapper databases to map imports to their corresponding libraries/packages
pub trait LibProcessor: SourceFinder + LibParser {
    /// Gets the includes/imports and maps to libraries for a single file
    fn process_file(&self, file_path: &Path) -> HashMap<LangInclude, Vec<Vec<String>>> {
        self.process_files(iter::once(file_path))
    }

    /// Takes an iterable of files and processes all referenced files to map includes to packages
    ///
    /// Mainly exists to allow both process_file and process_dir to both use the same code
    /// in the most efficient way possible
    fn process_files<T>(&self, file_paths: T) -> HashMap<LangInclude, Vec<Vec<String>>>
    where
        T: IntoIterator,
        T::Item: AsRef<Path>;

    /// Gets the includes/imports and maps to libraries for an entire directory
    ///
    /// TODO: Is this actually useful?
    /// Or would we either rather have the user collect the walker to files and call process_files?
    /// I.e write these two lines themselves
    /// If we remove this, we can also delete the constraint requiring SourceFinder
    fn process_dir(&self, walker: walkdir::IntoIter) -> HashMap<LangInclude, Vec<Vec<String>>>
    where
        Self: Sized,
    {
        let entries = Self::collect_source_files(walker);
        self.process_files(&entries)
    }
}

//==================== Util Functions ====================//
/// Helper function for calling rayon's parallel iterator on an iterable of files
/// (Which we do when processing multiple source files for includes)
///
/// Sample Usage:
/// par_file_iter(file_paths, |file_path| {
///     <Do something with file_path>
/// });
pub(crate) fn par_file_iter<T, F>(file_paths: T, closure: F)
where
    T: IntoIterator,
    T::Item: AsRef<Path>,
    F: Fn(&Path) + Sync + Send,
{
    use rayon::prelude::*;

    //Need to collect paths in order for the Rayon parallel iterator to work properly
    //Rayon requires owned data to work
    let file_paths: Vec<PathBuf> = file_paths
        .into_iter()
        .map(|entry| entry.as_ref().to_path_buf())
        .collect();

    file_paths
        .par_iter()
        .for_each(|file_path| closure(file_path.as_path()));
}

/// Helper function for removing duplicate entries from nested vectors
/// Keeps only the first occurrence and removes any future occurrences across all sub-vectors
///
/// Used to clean up ranked package matches, especially if multiple packages come from the same parent package
pub(crate) fn dedup_nested_vec<T>(input: Vec<Vec<T>>) -> Vec<Vec<T>>
where
    T: Eq + Hash + Clone,
{
    let mut seen = HashSet::new();
    let mut result = Vec::with_capacity(input.len());

    for inner in input {
        let mut filtered = Vec::with_capacity(inner.len());
        for item in inner {
            if seen.insert(item.clone()) {
                filtered.push(item);
            }
        }
        if !filtered.is_empty() {
            result.push(filtered);
        }
    }

    result
}
