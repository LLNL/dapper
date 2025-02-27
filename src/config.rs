// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::path::PathBuf;
use std::fs::File;
use std::io::{self, Write};
use toml::Table;
use std::error::Error;
use serde::{Serialize, Deserialize};
use toml::to_string;
use chrono::{DateTime, Utc};
use std::fs;

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    schema_version: u8,
    packages: HashMap<String, Package>,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Package {
    pub version: u8,
    pub format: String,
    pub timestamp: DateTime<Utc>,
    pub categories: Vec<String>,
    pub filepath: PathBuf,
}

pub fn create_metadata_file(output_path: Option<PathBuf>) -> std::io::Result<()>
{
    let mut packages = HashMap::new();
    
    // Create the struct to hold the metadata information in
    let config = Config {
        schema_version: 1,
        packages,
    };

    // Get the file path for the metadata toml file 
    let path = output_path.unwrap_or_else(|| PathBuf::from("metadata.toml"));
    let file_path = path.join("metadata.toml");

    // Check if the file already exists
    if file_path.exists() {
        eprintln!("File '{}' already exists. Aborting.", file_path.display());
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "File already exists",
        ));
    }

    // if the file doesn't exist, then create the file and write the struct to it.
    let toml_string = to_string(&config).expect("Failed to open");
    let mut file = File::create(&file_path)?;
    file.write_all(toml_string.as_bytes())?;
    Ok(())

}

pub fn update_metadata(base_dir: Option<PathBuf>, package_name: &str, new_format: Option<&str>, new_category: Option<&str>, new_pkg_file_path: Option<PathBuf>, add_new_package: bool,) -> io::Result<()>
{
    // Read the TOML file into a string
    let path = base_dir.unwrap_or_else(|| PathBuf::from("metadata.toml"));
    let file_path = path.join("metadata.toml");
    let toml_content = fs::read_to_string(file_path)?;
    
    // Deserialize the TOML string into the Config struct
    let mut config: Config = toml::from_str(&toml_content)
        .expect("Failed to parse TOML file");

    // Check if the package exists
    if let Some(package) = config.packages.get_mut(package_name) {
        // If the package exists, update its fields
        if let Some(format) = new_format {
            package.format = format.to_string();
        }
        if let Some(category) = new_category {
            package.categories.push(category.to_string());
        }
    } else if add_new_package {
        // If the package does not exist and the user wants to add a new one
        let new_package = Package {
            version: 1,
            format: new_format.unwrap_or("default_format").to_string(),
            timestamp: Utc::now(),
            categories: new_category
                .map(|cat| vec![cat.to_string()])
                .unwrap_or_else(Vec::new),
            filepath: new_pkg_file_path.unwrap_or_else(|| PathBuf::from("default/path")),
        };
        config.packages.insert(package_name.to_string(), new_package);
    } else {
        // If the package does not exist and the user does not want to add a new one
        eprintln!("Package '{}' does not exist and 'add_new_package' is false.", package_name);
        return Err(io::Error::new(io::ErrorKind::NotFound, "Package not found"));
    }

    // Serialize the updated Config struct back to a TOML string
    let updated_toml = toml::to_string_pretty(&config)
        .expect("Failed to serialize TOML data");

    // Write the updated TOML string back to the file
    let file_path = path.join("metadata.toml");
    let mut file = fs::File::create(file_path)?;
    file.write_all(updated_toml.as_bytes())?;

    Ok(())

}

pub fn read_metadata(path: Option<PathBuf>) -> Result<Table, Box<dyn Error>>
{
    // Clean up the file path
    let path = path.unwrap_or_else(|| PathBuf::from("metadata.toml"));
    let file_path = path.join("metadata.toml");
    // Read the file into a string
    let content = std::fs::read_to_string(file_path.clone())?;
    println!("file read successful");
    // Convert the contents of the file into a Table 
    let config: Table = content.parse()?;
    Ok(config)
}

pub fn search_by_category(file_path: PathBuf,category: &str,) -> io::Result<HashMap<String, Package>> // TODO: Is this function more public than it needs to be? 
{
    // Read the TOML file into a string
    let toml_content = std::fs::read_to_string(file_path)?;

    // Deserialize the TOML string into the Config struct
    let config: Config = toml::from_str(&toml_content)
        .expect("Failed to parse TOML file");

    // Filter packages by the specified category
    let matching_packages: HashMap<String, Package> = config
        .packages
        .into_iter()
        .filter(|(_, package)| package.categories.contains(&category.to_string()))
        .collect();

    // Return the matching packages
    Ok(matching_packages)
}

pub fn get_file_paths(metadata_path: PathBuf, category_filter: Option<&str>) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    // Read the TOML file
    let toml_content = fs::read_to_string(metadata_path)?;
    
    // Deserialize the TOML content into the Config struct
    let config: Config = toml::from_str(&toml_content)?;
    
    // Filter and collect file paths
    let file_paths: Vec<PathBuf> = config.packages.values()
        .filter(|package| {
            // If a category filter is provided, check if the package contains the category
            if let Some(category) = category_filter {
                package.categories.contains(&category.to_string())
            } else {
                true // No filter, include all packages
            }
        })
        .map(|package| package.filepath.clone()) // Collect the file paths
        .collect();
    
    Ok(file_paths)
}


// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::fs;

    #[test]
    fn test_create_metadata_file() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create metadata file
        let result = create_metadata_file(Some(output_path.clone()));
        assert!(result.is_ok(), "Metadata file creation should succeed");

        // Check if the file exists
        let metadata_file = output_path.join("metadata.toml");
        assert!(metadata_file.exists(), "Metadata file should exist");

        // Read and verify the contents
        let content = fs::read_to_string(metadata_file).unwrap();
        let config: Config = toml::from_str(&content).unwrap();
        assert_eq!(config.schema_version, 1, "Schema version should be 1");
        assert!(config.packages.is_empty(), "Initial packages list should be empty");
    }

    #[test]
    fn test_update_metadata_add_new_package() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create metadata file
        create_metadata_file(Some(output_path.clone())).unwrap();

        // Update metadata by adding a new package
        let result = update_metadata(
            Some(output_path.clone()),
            "test_package",
            Some("json"),
            Some("test_category"),
            Some(PathBuf::from("test/path")),
            true,
        );
        assert!(result.is_ok(), "Updating metadata should succeed");

        // Verify the contents
        let metadata_file = output_path.join("metadata.toml");
        let content = fs::read_to_string(metadata_file).unwrap();
        let config: Config = toml::from_str(&content).unwrap();

        assert!(config.packages.contains_key("test_package"), "Package should be added");
        let package = config.packages.get("test_package").unwrap();
        assert_eq!(package.format, "json", "Package format should be updated");
        assert!(package.categories.contains(&"test_category".to_string()), "Category should be added");
        assert_eq!(package.filepath, PathBuf::from("test/path"), "Filepath should be updated");
    }

    #[test]
    fn test_update_metadata_update_existing_package() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create metadata file with an initial package
        create_metadata_file(Some(output_path.clone())).unwrap();
        update_metadata(
            Some(output_path.clone()),
            "test_package",
            Some("json"),
            Some("test_category"),
            Some(PathBuf::from("test/path")),
            true,
        )
        .unwrap();

        // Update the existing package
        let result = update_metadata(
            Some(output_path.clone()),
            "test_package",
            Some("sqlite"),
            Some("new_category"),
            None,
            false,
        );
        assert!(result.is_ok(), "Updating existing package should succeed");

        // Verify the updated contents
        let metadata_file = output_path.join("metadata.toml");
        let content = fs::read_to_string(metadata_file).unwrap();
        let config: Config = toml::from_str(&content).unwrap();

        let package = config.packages.get("test_package").unwrap();
        assert_eq!(package.format, "sqlite", "Package format should be updated");
        assert!(package.categories.contains(&"new_category".to_string()), "New category should be added");
    }

    #[test]
    fn test_read_metadata() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create metadata file
        create_metadata_file(Some(output_path.clone())).unwrap();

        // Read metadata
        let result = read_metadata(Some(output_path.clone()));
        assert!(result.is_ok(), "Reading metadata should succeed");

        let config = result.unwrap();
        assert_eq!(config["schema_version"].as_integer().unwrap(), 1, "Schema version should be 1");
    }

    #[test]
    fn test_search_by_category() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create metadata file with two packages
        create_metadata_file(Some(output_path.clone())).unwrap();
        update_metadata(
            Some(output_path.clone()),
            "package1",
            Some("json"),
            Some("category1"),
            Some(PathBuf::from("path1")),
            true,
        )
        .unwrap();
        update_metadata(
            Some(output_path.clone()),
            "package2",
            Some("json"),
            Some("category2"),
            Some(PathBuf::from("path2")),
            true,
        )
        .unwrap();

        // Search for packages in "category1"
        let result = search_by_category(output_path.join("metadata.toml"), "category1");
        assert!(result.is_ok(), "Searching by category should succeed");

        let matching_packages = result.unwrap();
        assert_eq!(matching_packages.len(), 1, "Only one package should match");
        assert!(matching_packages.contains_key("package1"), "Matching package should be 'package1'");
    }

    #[test]
    fn test_get_file_paths() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create metadata file with two packages
        create_metadata_file(Some(output_path.clone())).unwrap();
        update_metadata(
            Some(output_path.clone()),
            "package1",
            Some("json"),
            Some("category1"),
            Some(PathBuf::from("path1")),
            true,
        )
        .unwrap();
        update_metadata(
            Some(output_path.clone()),
            "package2",
            Some("json"),
            Some("category2"),
            Some(PathBuf::from("path2")),
            true,
        )
        .unwrap();

        // Get file paths for all packages
        let result = get_file_paths(output_path.join("metadata.toml"), None);
        assert!(result.is_ok(), "Getting file paths should succeed");

        let file_paths = result.unwrap();
        assert_eq!(file_paths.len(), 2, "There should be two file paths");
        assert!(file_paths.contains(&PathBuf::from("path1")), "File paths should include 'path1'");
        assert!(file_paths.contains(&PathBuf::from("path2")), "File paths should include 'path2'");
    }
}