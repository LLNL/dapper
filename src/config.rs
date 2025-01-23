// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use directories::ProjectDirs;
use std::path::PathBuf;
 
pub fn get_base_directory() -> Option<PathBuf>
{
    match ProjectDirs::from("", "", "dapper") {
        Some(base_dirs) =>  Some(base_dirs.data_local_dir().to_path_buf()),
        _ => None,
    }
}

// metadata file load
    // look into creating a struct for more automation once format is set

// find databases that fit within x category

// save updated metadata file
    // may require database update


// add unit tests for the functions here