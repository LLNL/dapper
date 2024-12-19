// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use rusqlite::{params, Connection, Result};
use std::collections::HashMap;
use std::fs;

pub fn open_database(path: &str) -> Result<Connection> {
    Connection::open(path)
}

pub fn prepare_statement<'a>(conn: &'a Connection, sql: &str) -> Result<rusqlite::Statement<'a>> {
    conn.prepare(sql)
}

// file_name should be normalized according to a few rules, including lower case
// functions for normalizing e.g. so files, include paths, etc will be provided in another module
pub fn query_package_files(
    stmt: &mut rusqlite::Statement,
    file_name: &str,
) -> Result<Vec<(String, String)>> {
    stmt.query_map(params![file_name], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect()
}

pub fn read_contents_file(file_path: &str) -> HashMap<String, Vec<(String, String)>> {
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
