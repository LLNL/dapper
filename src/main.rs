// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
#[command(arg_required_else_help(true))]
struct Cli {
    #[command(subcommand)]
    command: Option<SubCommands>,

    #[arg(help = "Path to directory or file(s) to be analyzed")]
    paths: Vec<String>,
}

#[derive(Subcommand, Debug)]
enum SubCommands {
    #[command(about = "Dataset operations for downloading and managing datasets")]
    DB {
        #[command(subcommand)]
        action: DBAction,
    },
}

#[derive(Subcommand, Debug)]
enum DBAction {
    #[command(
        name = "list-installed",
        about = "List the datasets installed on the system"
    )]
    ListInstalled,
    #[command(
        name = "list-available",
        about = "List available datasets from the remote catalog"
    )]
    ListAvailable,
    #[command(
        about = "Install the specified dataset from the remote catalog (Use 'all' to install all datasets)"
    )]
    Install { dataset: String },
    #[command(
        about = "Update the specified dataset to latest version (Use 'all' to update all datasets)"
    )]
    Update { dataset: String },
    #[command(about = "Remove the specified dataset from the system")]
    Uninstall { dataset: String },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        //Database subcommands
        Some(SubCommands::DB { action }) => {
            use dapper::dataset::dataset_info::create_dataset_info;
            use dapper::dataset::dataset_ops::{
                install_all_datasets, install_dataset, list_available_datasets,
                list_installed_datasets, uninstall_dataset, update_all_datasets, update_dataset,
            };

            use dapper::directory_info::get_base_directory;

            // Initialize dataset_info.toml if it doesn't exist
            let db_dir =
                get_base_directory().expect("Unable to get the user's local data directory");
            match create_dataset_info(Some(db_dir.clone())) {
                Ok(()) => println!("Created dataset_info.toml in {}", db_dir.display()),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // File already exists, no need to print anything
                }
                Err(e) => {
                    eprintln!("Warning: Could not create dataset_info.toml: {e}");
                }
            }

            match action {
                DBAction::ListInstalled => {
                    if let Err(e) = list_installed_datasets() {
                        eprintln!("Error: {e}");
                    }
                }
                DBAction::ListAvailable => {
                    if let Err(e) = list_available_datasets(None) {
                        eprintln!("Error: {e}");
                    }
                }
                DBAction::Install {
                    dataset: dataset_name,
                } => {
                    if dataset_name.to_lowercase() == "all" {
                        if let Err(e) = install_all_datasets() {
                            eprintln!("Error installing datasets: {e}");
                        }
                    } else if let Err(e) = install_dataset(&dataset_name, true) {
                        eprintln!("Error installing dataset '{dataset_name}': {e}");
                    }
                }
                DBAction::Update {
                    dataset: dataset_name,
                } => {
                    if dataset_name.to_lowercase() == "all" {
                        if let Err(e) = update_all_datasets(true) {
                            eprintln!("Error updating datasets: {e}");
                        }
                    } else {
                        match update_dataset(&dataset_name, false, None, None) {
                            Ok(true) => {
                                println!("Dataset '{dataset_name}' was updated successfully")
                            }
                            Ok(false) => {}
                            Err(e) => {
                                eprintln!("Error updating dataset '{dataset_name}': {e}");
                            }
                        }
                    }
                }
                DBAction::Uninstall {
                    dataset: dataset_name,
                } => {
                    if let Err(e) = uninstall_dataset(&dataset_name) {
                        eprintln!("Error uninstalling dataset '{dataset_name}': {e}");
                    }
                }
            }
        }

        //Run application
        None => {
            if cli.paths.is_empty() {
                println!("No paths provided");
            }
            for path in cli.paths.iter() {
                dapper::run(path)
            }
        }
    }
}
