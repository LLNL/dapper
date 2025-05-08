// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use rusqlite::{params, Connection, Result};
use std::path::Path;

pub fn open_database<P: AsRef<Path>>(path: P) -> Result<Connection> {
    Connection::open(path)
}

pub fn prepare_statement<'a>(conn: &'a Connection, sql: &str) -> Result<rusqlite::Statement<'a>> {
    conn.prepare(sql)
}

// file_name should be normalized according to a few rules, including lower case
// functions for normalizing e.g. so files, include paths, etc will be provided in another module
pub fn query_linux_package_files(
    stmt: &mut rusqlite::Statement,
    file_name: &str,
) -> Result<Vec<(String, String)>> {
    stmt.query_map(params![file_name], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect()
}

pub fn query_python_package_imports(
    stmt: &mut rusqlite::Statement,
    import_name: &str,
) -> Result<Vec<String>> {
    stmt.query_map(params![import_name], |row| row.get(0))?
        .collect()
}
