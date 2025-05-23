// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use rusqlite;
use rusqlite::{CachedStatement, Connection, Statement};
use std::path::Path;

pub struct Database {
    connection: Connection,
}

impl Database {
    ///Create database object from sqlite file at the provided path
    pub fn new(path: &Path) -> rusqlite::Result<Database> {
        let connection = Connection::open(path)?;
        Ok(Database { connection })
    }

    /// Create a prepared statement for the database from the given SQL statement string
    pub fn prepare_statement(&self, sql: &str) -> rusqlite::Result<Statement> {
        self.connection.prepare(sql)
    }

    /// Creates a prepared statement from the given SQL statement string
    ///
    /// Caches the result so that when no longer in use, it can be used again
    /// Should improve performance by skipping repeatedly compiling statements used multiple times
    pub fn prepare_cached_statement(&self, sql: &str) -> rusqlite::Result<CachedStatement> {
        self.connection.prepare_cached(sql)
    }
}
