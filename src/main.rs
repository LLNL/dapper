// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use rayon::prelude::*;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::fs::metadata;
use streaming_iterator::StreamingIterator;
use tree_sitter::{Parser, Query, QueryCursor};
use walkdir::{DirEntry, WalkDir};

fn read_contents_file(file_path: &str) -> HashMap<String, Vec<(String, String)>> {
    let mut package_map = HashMap::new();
    let contents =
        fs::read_to_string(file_path).expect("Failed to read name to package mapping file");

    for line in contents.lines() {
        if let Some((file_path, package_name)) = line.rsplit_once([' ', '\t'].as_ref()) {
            let file_name = file_path.trim_end().rsplit('/').next().unwrap().to_string();
            package_map
                .entry(file_name)
                .or_insert_with(Vec::new)
                .push((package_name.to_string(), file_path.trim_end().to_string()));
        }
    }

    package_map
}

fn so_name_normalization() {
    let package_map = read_contents_file("Contents-amd64-noble");
    for key in package_map.keys() {
        if key.ends_with(".so") {
            // For matching cpython libraries it would probably benefit if the -312-x86_64-linux-gnu portion is removed
            // e.g. stringprep.cpython-312-x86_64-linux-gnu.so -> stringprep.cpython.so
            // Two kind of strange CPython cases: libsamba-net.cpython-312-x86-64-linux-gnu-samba4.so.0 and libpytalloc-util.cpython-312-x86-64-linux-gnu.so
            // Things compiled by the haskell compiler also have a pretty standard format: libHSsetlocale-1.0.0.10-EX0ACS22UctCUxDRUitp1V-ghc9.4.7.so
            if let Some(pos) = key.find(".cpython-") {
                let base_key = format!("{}.cpython.so", &key[..pos]);
                match package_map.contains_key(&base_key) {
                    false => println!("{}", base_key),
                    true => println!("Collision {}", base_key),
                }
            } else if let Some(pos) = key.find(".pypy") {
                let base_key = format!("{}.pypy.so", &key[..pos]);
                match package_map.contains_key(&base_key) {
                    false => println!("{}", base_key),
                    true => println!("Collision {}", base_key),
                }
            } else {
                // May be worth checking for some other special cases like ld that contain x86-64 or x86_64 in their name...
                // A binary having ld64.so.1 as a shared library dependency is a good indicator it targets ppc, ppc64le, or s390x
                println!("{}", key);
            }
        } else if key.contains(".so.")
            && ![".gz", ".patch", ".diff", ".hmac", ".qm"]
                .iter()
                .any(|suffix| key.ends_with(suffix))
        {
            // Filter out files such as 0001-MIPS-SPARC-fix-wrong-vfork-aliases-in-libpthread.so.patch, t.so.gz, getmax.so.gz, "*.so.0.*" (what?), .libkcapi.so.hmac, libnss_cache_oslogin.so.2.8.gz, local-ldconfig-ignore-ld.so.diff, scribus.so.qm
            // Interesting SOABI version for a file with a letter in it: libpsmile.MPI1.so.0d
            let base_key = format!("{}.so", key.split(".so.").next().unwrap());
            if !package_map.contains_key(&base_key) {
                println!("{}", key);
            }
        } else if key.contains(".so-") {
            // Decide if the "Kernels" ending in ".hsaco" need to be filtered out
            let base_key = format!("{}.so", key.split(".so-").next().unwrap());
            if !package_map.contains_key(&base_key) {
                println!("{}", key);
            }
        } else if key.contains(".so_") {
            // Decide if this category should be included -- basically just lib_postgresqludf_sys.so_
            let base_key = format!("{}.so", key.split(".so_").next().unwrap());
            if !package_map.contains_key(&base_key) {
                println!("{}", key);
            }
        }
    }
    return;
}

fn main() {
    let conn = rusqlite::Connection::open("package-files_contents-amd64_long-package-names.db")
        .expect("Failed to open database");
    let mut stmt = conn
        .prepare("SELECT package_name, file_path FROM package_files WHERE file_name = ?1")
        .expect("Failed to prepare statement");

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
                    // Corner case: Build system adding local directory to system include path?
                    system_include_map
                        .entry(include)
                        .or_insert_with(Vec::new)
                        .push(entry.path().display().to_string());
                }
            }

            {
                let mut user_include_map = user_include_map.lock().unwrap();
                for include in user_includes {
                    // TODO Before adding a path, check if the include is satisfied by a file in the source codet
                    // If it is, downgrade the likelihood of the header file being from a package...
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
        for include in unique_system_includes
            .iter()
            .chain(unique_user_includes.iter())
        {
            let include_lower = include.rsplit('/').next().unwrap().to_lowercase();
            let matching_packages: Vec<_> = stmt
                .query_map([&include_lower], |row| {
                    let package_name: String = row.get(0)?;
                    let file_path: String = row.get(1)?;
                    Ok((package_name, file_path))
                })
                .expect("Failed to query database")
                .filter_map(|result| {
                    if let Ok((package_name, file_path)) = result {
                        let path_buf = std::path::Path::new(&file_path);
                        if path_buf.ends_with(include)
                            && path_buf.to_str().unwrap().contains("include")
                        {
                            return Some((package_name, file_path));
                        }
                    }
                    None
                })
                .collect();

            if !matching_packages.is_empty() {
                println!("Include: {} -> Packages: {:?}", include, matching_packages);
            }
        }
        // print size of each map
        println!(
            "System Unique Include Map Size: {}",
            unique_system_includes.len()
        );
        println!(
            "User Unique Include Map Size: {}",
            unique_user_includes.len()
        );
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
