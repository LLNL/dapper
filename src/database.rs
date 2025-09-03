// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use rusqlite::{self, OpenFlags};
use rusqlite::{CachedStatement, Connection, Statement};
use std::path::Path;

pub struct Database {
    connection: Connection,
}

impl Database {
    ///Create database object from sqlite file at the provided path
    pub fn new(path: &Path) -> rusqlite::Result<Database> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        Ok(Database { connection })
    }

    /// Create a prepared statement for the database from the given SQL statement string
    pub fn prepare_statement(&self, sql: &str) -> rusqlite::Result<Statement<'_>> {
        self.connection.prepare(sql)
    }

    /// Creates a prepared statement from the given SQL statement string
    ///
    /// Caches the result so that when no longer in use, it can be used again
    /// Should improve performance by skipping repeatedly compiling statements used multiple times
    pub fn prepare_cached_statement(&self, sql: &str) -> rusqlite::Result<CachedStatement<'_>> {
        self.connection.prepare_cached(sql)
    }
}
