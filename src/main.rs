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

    #[arg(long, help = "List available datasets")]
    list_available_datasets: bool,

    #[arg(
        long,
        help = "Install dataset(s) from remote catalog (use 'all' to install all datasets)"
    )]
    install: Option<String>,

    #[arg(long, help = "Uninstall a specific dataset")]
    uninstall: Option<String>,

    #[arg(
        long,
        help = "Update dataset(s) to latest version (use 'all' to update all datasets)"
    )]
    update: Option<String>,
}

fn main() {
    let args = Args::parse();
    dapper::run(
        &args.path,
        args.list_datasets,
        args.list_available_datasets,
        args.install,
        args.uninstall,
        args.update,
    );
}
