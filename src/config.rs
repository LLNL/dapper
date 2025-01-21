// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use directories::BaseDirs;

pub fn get_base_directory() -> &Path
{
    if let Some(base_dirs) = BaseDirs::new() {
        base_dirs.data_local_dir();
    }
}