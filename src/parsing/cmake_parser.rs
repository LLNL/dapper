// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use streaming_iterator::StreamingIterator;

use super::parser::{LangInclude, LibParser, SourceFinder};
use tree_sitter::{Node, Parser, Query, QueryCursor};
use walkdir::DirEntry;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CMakeRemoteInclude {
    URL(String),                     //Raw URL
    GitRepo(String, Option<String>), //Repo URL, Git Tag [optional]
}

pub struct CMakeParser {
    //Currently does not need any databases, just extracts URL(s)
}

lazy_static::lazy_static! {
    static ref CMAKE_INCLUDE_QUERY: Query = Query::new(
        &tree_sitter_cmake::LANGUAGE.into(),
        r#"
        (
            (
                normal_command
                (identifier) @name
                (argument_list (argument)* @arg)
            )
            (#match? @name "(?i)^ExternalProject_Add$")
        )
        (
            (
                normal_command
                (identifier) @name
                (argument_list (argument)* @arg)
            )
            (#match? @name "(?i)^FetchContent_Declare")
        )
        "#
    ).expect("Error creating query");
}

impl CMakeParser {
    pub fn new() -> Self {
        CMakeParser {}
    }

    /// Removes comments from the provided source file
    /// Used because of problems that the comments create when parsing and grouping with cmake grammer
    ///
    /// TODO: This seems somewhat computationally excessive for what we actually want
    ///  See if the query can be modified so that we don't need this
    fn remove_comments(source: &str) -> String {
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_cmake::LANGUAGE.into())
            .expect("Error loading Cmake grammar");

        let tree = parser.parse(&source, None).unwrap();
        let root_node = tree.root_node();

        fn collect_ranges(node: Node, ranges: &mut Vec<(usize, usize)>) {
            let mut cursor = node.walk();
            for child in node.children(&mut cursor) {
                let kind = child.kind();
                if kind == "line_comment" || kind == "block_comment" {
                    ranges.push((child.start_byte(), child.end_byte()));
                }
                // Recurse into every child
                collect_ranges(child, ranges);
            }
        }

        let mut ranges: Vec<(usize, usize)> = Vec::new();
        collect_ranges(root_node, &mut ranges);

        if ranges.is_empty() {
            return source.to_string();
        }

        ranges.sort_by_key(|r| r.0);
        let mut merged: Vec<(usize, usize)> = Vec::new();
        for (s, e) in ranges {
            if let Some(last) = merged.last_mut() {
                if s <= last.1 {
                    // overlap or adjacent
                    if e > last.1 {
                        last.1 = e;
                    }
                } else {
                    merged.push((s, e));
                }
            } else {
                merged.push((s, e));
            }
        }

        let mut cleaned = source.to_string();
        for (s, e) in merged.into_iter().rev() {
            cleaned.replace_range(s..e, "");
        }

        cleaned
    }

    pub fn extract_includes(file_path: &Path) -> HashSet<CMakeRemoteInclude> {
        let mut includes: HashSet<CMakeRemoteInclude> = HashSet::new();

        let source_code = match fs::read_to_string(file_path) {
            Ok(content) => content,
            Err(e) => {
                eprintln!("Error reading file {}: {}", file_path.to_str().unwrap(), e);
                return includes;
            }
        };
        //Remove comments because they break some of the tree-sitter parsing (grouping) for cmake
        let source_code = Self::remove_comments(source_code.as_str());

        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_cmake::LANGUAGE.into())
            .expect("Error loading Cmake grammar");
        let tree = parser.parse(&source_code, None).unwrap();
        let root_node = tree.root_node();
        let mut query_cursor = QueryCursor::new();
        let mut matches =
            query_cursor.matches(&CMAKE_INCLUDE_QUERY, root_node, source_code.as_bytes());

        while let Some(m) = matches.next() {
            let mut args: Vec<String> = Vec::new();

            for cap in m.captures {
                let cap_name = CMAKE_INCLUDE_QUERY.capture_names()[cap.index as usize];
                let text = cap
                    .node
                    .utf8_text(source_code.as_bytes())
                    .unwrap()
                    .to_string();

                // Strip any leading or trailing quotes
                // We want the contents of the string, not the raw string representation "contents"
                let text = text.strip_prefix('"').unwrap_or(&text).to_string();
                let text = text.strip_suffix('"').unwrap_or(&text).to_string();

                match cap_name {
                    "arg" => args.push(text),
                    _ => {}
                }
            }

            let mut arg_dict = HashMap::new();
            let groups = args.iter().skip(1).cloned().collect::<Vec<_>>();

            for pair in groups.chunks_exact(2) {
                arg_dict.insert(pair[0].clone(), pair[1].clone());
            }
            if let Some(dangling) = groups.chunks_exact(2).remainder().get(0) {
                eprintln!("Warning: Dangling key without value: {}", dangling);
            }

            if arg_dict.contains_key("URL") {
                let url = arg_dict.get("URL").unwrap().to_string();
                includes.insert(CMakeRemoteInclude::URL(url));
            } else if arg_dict.contains_key("GIT_REPOSITORY") {
                let repo_url = arg_dict.get("GIT_REPOSITORY").unwrap().to_string();
                let git_tag = match arg_dict.get("GIT_TAG") {
                    Some(tag) => Some(tag.to_string()),
                    None => None,
                };

                includes.insert(CMakeRemoteInclude::GitRepo(repo_url, git_tag));
            }
        }

        includes
    }
}

impl SourceFinder for CMakeParser {
    const EXTENSIONS: &'static [&'static str] = &["cmake"];

    fn check_other(entry: &DirEntry) -> bool
    where
        Self: Sized,
    {
        const MATCH_FILENAME: &str = "CMakeLists.txt";
        if let Some(file_name) = entry.path().file_name() {
            if let Some(file_name) = file_name.to_str() {
                if file_name.to_lowercase() == MATCH_FILENAME.to_lowercase() {
                    return true;
                }
            }
        }
        false
    }
}

impl LibParser for CMakeParser {
    fn extract_includes(file_path: &Path) -> HashSet<LangInclude> {
        Self::extract_includes(file_path)
            .into_iter()
            .map(LangInclude::CMake)
            .collect()
    }

    fn extract_sys_calls(_file_path: &Path) -> HashSet<LangInclude>
    where
        Self: Sized,
    {
        HashSet::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_cmake_includes() {
        let test_file = Path::new("tests/test_files/test.cmake");
        let includes = CMakeParser::extract_includes(test_file);

        let exp_includes = [
            CMakeRemoteInclude::URL("https://github.com/fakeorg/zeta/releases/download/zeta-1.0.0/zeta-1.0.0-win32.zip".to_string()),
            CMakeRemoteInclude::URL("https://gitlab.com/opensource-devs/epsilon/releases/download/2.5.1/epsilon-2.5.1.zip".to_string()),
            CMakeRemoteInclude::URL("https://github.com/fakeuser/theta/releases/download/4.3.2/theta-4.3.2.bin.WIN32.zip".to_string()),
            CMakeRemoteInclude::URL("https://example.com/sigma-binaries/archive/v3.1.4.zip".to_string()),
            CMakeRemoteInclude::GitRepo(
                "https://github.com/exampleorg/alpha-lib.git".to_string(),
                Some("master".to_string()),
            ),
            CMakeRemoteInclude::GitRepo(
                "https://gitlab.com/opensource/bravo.git".to_string(),
                Some("2.3.1".to_string()),
            ),
            CMakeRemoteInclude::GitRepo("https://github.com/fakeuser/charlie.git".to_string(), None),
            CMakeRemoteInclude::GitRepo(
                "https://bitbucket.org/deltateam/delta.git".to_string(),
                Some("v0.9.8".to_string()),
            ),
            CMakeRemoteInclude::GitRepo(
                "https://github.com/echodevs/echo.git".to_string(),
                Some("v1.2.0".to_string()),
            ),
            CMakeRemoteInclude::GitRepo(
                "https://github.com/foxtrotorg/foxtrot.git".to_string(),
                Some("v4.0.0".to_string()),
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(includes, exp_includes);
    }
}
