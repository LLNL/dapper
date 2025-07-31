// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Args {
    #[arg(help = "The path to a directory or a file to be analyzed.", index = 1)]
    path: String,

    #[arg(long, short = 'l', help = "List installed datasets")]
    list_datasets: bool,
}

fn main() {
    let args = Args::parse();
    dapper::run(&args.path, args.list_datasets);
}
