// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use rusqlite::{params, Connection, Result};

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
