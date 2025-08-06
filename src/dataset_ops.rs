use crate::dataset_info::{read_dataset_info, update_dataset_info, Config};
use crate::dataset_list::{read_dataset_list, RemoteCatalog};
use crate::directory_info::get_base_directory;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use toml;
use zip::ZipArchive;

pub fn list_installed_datasets() -> Result<(), Box<dyn Error>> {
    let base_dir = get_base_directory().ok_or("Unable to get the user's local data directory")?;

    let config = match read_dataset_info(Some(base_dir)) {
        Ok(config) => config,
        Err(_) => {
            println!("No datasets installed. Use --download to get started.");
            return Ok(());
        }
    };

    if config.datasets.is_empty() {
        println!("No datasets installed. Use --install to get started.");
        return Ok(());
    }

    config.display_horizontal();

    println!("\n{} dataset(s) installed", config.datasets.len());

    Ok(())
}

pub fn list_available_datasets(_filter: Option<&str>) -> Result<(), Box<dyn Error>> {
    println!("Fetching available datasets from remote catalog...");

    let catalog = read_dataset_list()?;

    if catalog.datasets.is_empty() {
        println!("No datasets available in remote catalog.");
        return Ok(());
    }

    catalog.display_vertical();
    println!("\n{} dataset(s) available", catalog.datasets.len());

    Ok(())
}

pub fn install_dataset(dataset_name: &str, prompt: bool) -> Result<(), Box<dyn Error>> {
    println!("Installing dataset: {}", dataset_name);

    let base_dir = get_base_directory().ok_or("Unable to get the user's local data directory")?;

    // Check if dataset already exists
    if dataset_exists(&base_dir, dataset_name) {
        if prompt {
            let message = format!("Dataset '{}' already exists. Overwrite?", dataset_name);
            let update = prompt_user(&message, false);
            if !update {
                println!("Installation cancelled.");
                return Ok(());
            }
        }
        println!("Updating existing dataset...");
    }

    // Fetch dataset_info.toml
    let catalog = read_dataset_list()?;

    let dataset = catalog
        .datasets
        .get(dataset_name)
        .ok_or_else(|| format!("Dataset '{}' not found in remote catalog", dataset_name))?;

    let version = parse_version(&dataset.version)?;
    let format = &dataset.format;
    let categories = &dataset.categories;
    let url = dataset
        .urls
        .first()
        .ok_or("No download URL found in dataset")?;

    // Download the dataset
    println!("Downloading from: {}", url);
    let download_response =
        reqwest::blocking::get(url).map_err(|e| format!("Failed to download dataset: {e}"))?;

    if !download_response.status().is_success() {
        return Err(format!(
            "Failed to download dataset: HTTP {}",
            download_response.status()
        )
        .into());
    }

    // Saves it to tmp file
    let temp_zip_path = base_dir.join(format!("{}.zip.tmp", dataset_name));
    let mut temp_file = File::create(&temp_zip_path)
        .map_err(|e| format!("Failed to create temporary file: {e}"))?;

    let content = download_response
        .bytes()
        .map_err(|e| format!("Failed to read download content: {e}"))?;

    std::io::copy(&mut content.as_ref(), &mut temp_file)
        .map_err(|e| format!("Failed to write temporary file: {e}"))?;

    drop(temp_file);

    // Extract the zip file
    println!("Extracting dataset...");
    let file = File::open(&temp_zip_path).map_err(|e| format!("Failed to open zip file: {e}"))?;

    let mut archive =
        ZipArchive::new(file).map_err(|e| format!("Failed to read zip archive: {e}"))?;

    // Find the .db file in the archive
    let mut db_filename = None;
    for i in 0..archive.len() {
        let file = archive.by_index(i)?;
        if file.name().ends_with(".db") {
            db_filename = Some(file.name().to_string());
            break;
        }
    }

    let db_filename = db_filename.ok_or("No .db file found in archive")?;

    // Extract the .db file
    let mut db_file = archive
        .by_name(&db_filename)
        .map_err(|e| format!("Failed to find {} in archive: {e}", db_filename))?;

    let db_path = base_dir.join(format!("{}.db", dataset_name));
    let mut output_file =
        File::create(&db_path).map_err(|e| format!("Failed to create database file: {e}"))?;

    std::io::copy(&mut db_file, &mut output_file)
        .map_err(|e| format!("Failed to extract database file: {e}"))?;

    fs::remove_file(&temp_zip_path).ok();

    // update dataset_info.toml
    update_dataset_info(
        Some(base_dir.clone()),
        dataset_name,
        Some(format),
        Some(categories.clone()),
        Some(PathBuf::from(format!("{}.db", dataset_name))),
        true,
        Some(version),
        dataset.timestamp,
    )?;

    println!("Successfully installed dataset: {}", dataset_name);
    Ok(())
}

pub fn install_all_datasets() -> Result<(), Box<dyn Error>> {
    println!("Installing all available datasets...\n");

    let catalog = read_dataset_list()?;

    let total = catalog.datasets.len();
    let mut succeeded = 0;
    let mut failed = Vec::new();

    for name in catalog.datasets.keys() {
        println!(
            "Installing {} ({}/{})",
            name,
            succeeded + failed.len() + 1,
            total
        );
        match install_dataset(name, true) {
            Ok(_) => succeeded += 1,
            Err(e) => {
                eprintln!("Failed to install {}: {}", name, e);
                failed.push(name.clone());
            }
        }
        println!();
    }

    println!("Installation summary:");
    println!("  Succeeded: {}", succeeded);
    println!("  Failed: {}", failed.len());

    if !failed.is_empty() {
        println!("\nFailed datasets:");
        for name in &failed {
            println!("  - {}", name);
        }
    }

    if succeeded == 0 {
        Err("No datasets were successfully installed".into())
    } else {
        Ok(())
    }
}

pub fn uninstall_dataset(dataset_name: &str) -> Result<(), Box<dyn Error>> {
    println!("Uninstalling dataset: {}", dataset_name);

    let base_dir = get_base_directory().ok_or("Unable to get the user's local data directory")?;

    // Read current dataset info
    // let dataset_info_path = base_dir.join("dataset_info.toml");

    let mut config = read_dataset_info(Some(base_dir.clone()))?;

    // Check if dataset exists
    let dataset = config
        .datasets
        .get(dataset_name)
        .ok_or_else(|| format!("Dataset '{}' is not installed", dataset_name))?;

    let db_path = base_dir.join(&dataset.filepath);

    // Remove the database file
    if db_path.exists() {
        fs::remove_file(&db_path).map_err(|e| format!("Failed to remove database file: {e}"))?;
        println!("Removed database file: {}", db_path.display());
    } else {
        println!("Warning: Database file not found: {}", db_path.display());
    }

    // Remove from dataset_info.toml
    config.datasets.remove(dataset_name);

    // Write back the updated config
    let dataset_info_path = base_dir.join("dataset_info.toml");
    let updated_toml = toml::to_string_pretty(&config)
        .map_err(|e| format!("Failed to serialize dataset info: {e}"))?;

    fs::write(&dataset_info_path, updated_toml)
        .map_err(|e| format!("Failed to update dataset_info.toml: {e}"))?;

    println!("Successfully uninstalled dataset: {}", dataset_name);
    Ok(())
}

pub fn update_dataset(
    dataset_name: &str,
    prompt: bool,
    installed_config: Option<&Config>,
    remote_catalog: Option<&RemoteCatalog>,
) -> Result<bool, Box<dyn Error>> {
    let base_dir = get_base_directory().ok_or("Unable to get the user's local data directory")?;

    // Use provided config or read it
    let config;
    let local_dataset = match installed_config {
        Some(cfg) => cfg.datasets.get(dataset_name),
        None => {
            config = read_dataset_info(Some(base_dir.clone()))?;
            config.datasets.get(dataset_name)
        }
    }
    .ok_or_else(|| format!("Dataset '{}' is not installed", dataset_name))?;

    // Use provided catalog or fetch it
    let catalog;
    let remote_dataset = match remote_catalog {
        Some(cat) => cat.datasets.get(dataset_name),
        None => {
            catalog = read_dataset_list()?;
            catalog.datasets.get(dataset_name)
        }
    }
    .ok_or_else(|| format!("Dataset '{}' not found in remote catalog", dataset_name))?;

    // Compare timestamps
    let needs_update = match (local_dataset.timestamp, remote_dataset.timestamp) {
        (Some(local_ts), Some(remote_ts)) => remote_ts > local_ts,
        // (None, Some(_)) => true,
        _ => false,
    };

    if !needs_update {
        println!("Dataset '{}' is already up to date", dataset_name);
        return Ok(false);
    }

    // Show update info and no prompt.
    let local_date = local_dataset
        .timestamp
        .map(|t| t.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let remote_date = remote_dataset
        .timestamp
        .map(|t| t.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| "unknown".to_string());

    println!(
        "Update available for '{}' (local: {}, remote: {})",
        dataset_name, local_date, remote_date
    );

    // Perform update using install_dataset. No prompt
    install_dataset(dataset_name, prompt)?;
    Ok(true)
}

pub fn update_all_datasets(prompt: bool) -> Result<(), Box<dyn Error>> {
    let base_dir = get_base_directory().ok_or("Unable to get the user's local data directory")?;

    // Read once
    let installed_config = read_dataset_info(Some(base_dir.clone()))?;
    let remote_catalog = read_dataset_list()?;

    let mut updated = 0;
    let mut up_to_date = 0;
    let mut failed = Vec::new();

    // Use cached data for each dataset
    for dataset_name in installed_config.datasets.keys() {
        match update_dataset(
            dataset_name,
            prompt,
            Some(&installed_config),
            Some(&remote_catalog),
        ) {
            Ok(true) => updated += 1,
            Ok(false) => up_to_date += 1,
            Err(e) => {
                eprintln!("Failed to update {}: {}", dataset_name, e);
                failed.push(dataset_name.clone());
            }
        }
    }

    // Print summary
    println!("\nUpdate summary:");
    println!("  Total datasets: {}", installed_config.datasets.len());
    println!("  Updated: {}", updated);
    println!("  Already up to date: {}", up_to_date);
    println!("  Failed: {}", failed.len());

    if !failed.is_empty() {
        println!("\nFailed updates:");
        for name in &failed {
            println!("  - {}", name);
        }
    }

    Ok(())
}

fn dataset_exists(base_dir: &Path, dataset_name: &str) -> bool {
    if let Ok(config) = read_dataset_info(Some(base_dir.to_path_buf())) {
        return config.datasets.contains_key(dataset_name);
    }

    false
}

fn prompt_user(message: &str, default: bool) -> bool {
    let default_indicator = if default { "[Y/n]" } else { "[y/N]" };
    print!("{} {}: ", message, default_indicator);
    std::io::stdout().flush().unwrap();

    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();

    let trimmed = input.trim().to_lowercase();
    if trimmed.is_empty() {
        return default;
    }

    matches!(trimmed.as_str(), "y" | "yes")
}

fn parse_version(version_str: &str) -> Result<u8, Box<dyn Error>> {
    let version_num = version_str.trim_start_matches('v');
    version_num
        .parse::<u8>()
        .map_err(|e| format!("Failed to parse version '{}': {e}", version_str).into())
}

pub trait DatasetDisplay {
    fn display_horizontal(&self);
    fn display_vertical(&self);
}

impl DatasetDisplay for Config {
    fn display_horizontal(&self) {
        // Print header
        println!(
            "{:<20} {:<10} {:<10} {:<20} {:<30} {:<50}",
            "NAME", "VERSION", "FORMAT", "TIMESTAMP", "CATEGORIES", "FILEPATH"
        );
        println!("{}", "-".repeat(140));

        // Sort names
        let mut names: Vec<&String> = self.datasets.keys().collect();
        names.sort();

        // Print each dataset
        for name in names {
            let dataset = self.datasets.get(name).unwrap();
            let timestamp = dataset
                .timestamp
                .map(|t| t.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let categories = dataset.categories.join(", ");

            println!(
                "{:<20} {:<10} {:<10} {:<20} {:<30} {:<50}",
                name,
                dataset.version,
                dataset.format,
                timestamp,
                if categories.is_empty() {
                    "none".to_string()
                } else {
                    categories
                },
                dataset.filepath.display()
            );
        }
    }

    fn display_vertical(&self) {
        let mut names: Vec<&String> = self.datasets.keys().collect();
        names.sort();

        println!("{}", "=".repeat(80));

        for (index, name) in names.iter().enumerate() {
            if index > 0 {
                println!("{}", "-".repeat(80));
            }

            let dataset = self.datasets.get(*name).unwrap();
            println!("Dataset: {}", name);
            println!("Version: {}", dataset.version);
            println!("Format: {}", dataset.format);
            println!(
                "Timestamp: {}",
                dataset
                    .timestamp
                    .map(|t| t.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            );
            println!(
                "Categories: {}",
                if dataset.categories.is_empty() {
                    "none".to_string()
                } else {
                    dataset.categories.join(", ")
                }
            );
            println!("Filepath: {}", dataset.filepath.display());
        }

        println!("{}", "=".repeat(80));
    }
}
impl DatasetDisplay for RemoteCatalog {
    fn display_horizontal(&self) {
        println!(
            "{:<20} {:<10} {:<10} {:<20} {:<30}",
            "NAME", "VERSION", "FORMAT", "TIMESTAMP", "CATEGORIES"
        );
        println!("{}", "-".repeat(100));

        let mut names: Vec<&String> = self.datasets.keys().collect();
        names.sort();

        for name in names {
            let dataset = self.datasets.get(name).unwrap();
            let timestamp = dataset
                .timestamp
                .map(|t| t.format("%Y-%m-%d").to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let categories = dataset.categories.join(", ");

            println!(
                "{:<20} {:<10} {:<10} {:<20} {:<30}",
                name,
                dataset.version,
                dataset.format,
                timestamp,
                if categories.is_empty() {
                    "none".to_string()
                } else {
                    categories
                }
            );
        }
    }

    fn display_vertical(&self) {
        let mut names: Vec<&String> = self.datasets.keys().collect();
        names.sort();

        println!("{}", "=".repeat(80));

        for (index, name) in names.iter().enumerate() {
            if index > 0 {
                println!("{}", "-".repeat(80));
            }

            let dataset = self.datasets.get(*name).unwrap();
            println!("Dataset: {}", name);
            println!("Version: {}", dataset.version);
            println!("Format: {}", dataset.format);
            println!(
                "Timestamp: {}",
                dataset
                    .timestamp
                    .map(|t| t.format("%Y-%m-%d").to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            );
            println!("Filepath: {}", dataset.filepath);
            println!(
                "Categories: {}",
                if dataset.categories.is_empty() {
                    "none".to_string()
                } else {
                    dataset.categories.join(", ")
                }
            );
            println!("URLs:");
            for url in &dataset.urls {
                println!("  - {}", url);
            }
        }

        println!("{}", "=".repeat(80));
    }
}
