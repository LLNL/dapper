// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use clap::Parser;
use std::process::Command;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[command(arg_required_else_help(false))]
struct Args {
    #[arg(help = "The path to a directory or a file to be analyzed.", index = 1)]
    path: Option<String>,
    
    #[arg(long, help = "List available datasets")]
    list_datasets: bool,
}

fn main() {
    let args = Args::parse();
    
    if args.list_datasets {
        run_python_command(&["--list-datasets"]);
        return;
    }
    
    if let Some(path) = args.path {
        dapper::run(&path);
    } else {
        eprintln!("Error: Must provide either a path to analyze or use --list-datasets");
        std::process::exit(1);
    }
}

fn run_python_command(args: &[&str]) {
    let python_dir = std::env::current_dir()
        .unwrap()
        .join("python")
        .join("dapper_python");
    
    let script_path = python_dir.join("dataset_loader.py");
    
    let mut cmd = Command::new("python3");
    cmd.arg(&script_path);
    for arg in args {
        cmd.arg(arg);
    }
    
    let output = cmd.output().expect("Failed to execute Python script");
    
    print!("{}", String::from_utf8_lossy(&output.stdout));
    if !output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
    }
    
    if !output.status.success() {
        std::process::exit(output.status.code().unwrap_or(1));
    }
}
