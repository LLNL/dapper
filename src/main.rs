// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    println!("{args:?}");
    let arg_path = &args[1];
    dapper::run(arg_path);
}
