// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

// use crate::directory_info::get_base_directory;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use toml::to_string;
// use toml::Table;

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub schema_version: u8,
    pub datasets: HashMap<String, Dataset>,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct Dataset {
    pub version: u8,
    pub format: String,
    pub timestamp: Option<DateTime<Utc>>,
    pub categories: Vec<String>,
    pub filepath: PathBuf,
}

pub fn create_dataset_info(output_path: Option<PathBuf>) -> std::io::Result<()> {
    let datasets = HashMap::new();

    // Create the struct to hold the dataset information in
    let config = Config {
        schema_version: 1,
        datasets,
    };

    // Get the file path for the dataset_info toml file
    let path = output_path.unwrap_or_else(|| PathBuf::from("."));
    let file_path = path.join("dataset_info.toml");

    // Check if the file already exists
    if file_path.exists() {
        return Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, ""));
    }

    // if the file doesn't exist, then create the file and write the struct to it.
    let toml_string = to_string(&config).expect("Failed to open");
    let mut file = File::create(&file_path)?;
    file.write_all(toml_string.as_bytes())?;
    Ok(())
}

pub fn update_dataset_info(
    base_dir: Option<PathBuf>,
    dataset_name: &str,
    new_dataset: Dataset,
    add_new_dataset: bool,
) -> io::Result<()> {
    // Read the TOML file into a string
    let path = base_dir.unwrap_or_else(|| PathBuf::from("dataset_info.toml"));
    let file_path = path.join("dataset_info.toml");
    let toml_content = fs::read_to_string(file_path)?;

    // Deserialize the TOML string into the Config struct
    let mut config: Config = toml::from_str(&toml_content).expect("Failed to parse TOML file");

    // Check if the dataset exists
    if let Some(dataset) = config.datasets.get_mut(dataset_name) {
        // If the dataset exists, update its fields
        *dataset = new_dataset;
    } else if add_new_dataset {
        // If the dataset does not exist and the user wants to add a new one
        config
            .datasets
            .insert(dataset_name.to_string(), new_dataset);
    } else {
        // Dataset doesn't exist and we don't want to add it
        eprintln!("Dataset '{dataset_name}' does not exist and 'add_new_dataset' is false.");
        return Err(io::Error::new(io::ErrorKind::NotFound, "Dataset not found"));
    }

    // Serialize the updated Config struct back to a TOML string
    let updated_toml = toml::to_string_pretty(&config).expect("Failed to serialize TOML data");

    // Write the updated TOML string back to the file
    let file_path = path.join("dataset_info.toml");
    let mut file = fs::File::create(file_path)?;
    file.write_all(updated_toml.as_bytes())?;

    Ok(())
}

pub fn read_dataset_info(path: Option<PathBuf>) -> Result<Config, Box<dyn Error>> {
    // Clean up the file path
    let path = path.unwrap_or_else(|| PathBuf::from("dataset_info.toml"));
    let file_path = path.join("dataset_info.toml");
    // Read the file into a string
    let content = std::fs::read_to_string(file_path.clone())?;
    // println!("file read successful");
    // Convert the contents of the file into a Table
    let config: Config = toml::from_str(&content)?;
    Ok(config)
}

pub fn search_dataset_by_category(
    file_path: PathBuf,
    category: &str,
) -> io::Result<HashMap<String, Dataset>> {
    // Read the TOML file into a string
    let toml_content = std::fs::read_to_string(file_path)?;

    // Deserialize the TOML string into the Config struct
    let config: Config = toml::from_str(&toml_content).expect("Failed to parse TOML file");

    // Filter datasets by the specified category
    let matching_datasets: HashMap<String, Dataset> = config
        .datasets
        .into_iter()
        .filter(|(_, dataset)| dataset.categories.contains(&category.to_string()))
        .collect();

    // Return the matching datasets
    Ok(matching_datasets)
}

pub fn get_dataset_file_paths(
    dataset_info_path: PathBuf,
    category_filter: Option<&str>,
) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    // Read the TOML file
    let toml_content = fs::read_to_string(dataset_info_path)?;

    // Deserialize the TOML content into the Config struct
    let config: Config = toml::from_str(&toml_content)?;

    // Filter and collect file paths
    let file_paths: Vec<PathBuf> = config
        .datasets
        .values()
        .filter(|dataset| {
            // If a category filter is provided, check if the dataset contains the category
            if let Some(category) = category_filter {
                dataset.categories.contains(&category.to_string())
            } else {
                true // No filter, include all datasets
            }
        })
        .map(|dataset| dataset.filepath.clone()) // Collect the file paths
        .collect();

    Ok(file_paths)
}

// Unit tests
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_create_dataset_info() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create dataset_info file
        let result = create_dataset_info(Some(output_path.clone()));
        assert!(result.is_ok(), "Metadata file creation should succeed");

        // Check if the file exists
        let dataset_info_file = output_path.join("dataset_info.toml");
        assert!(dataset_info_file.exists(), "Metadata file should exist");

        // Read and verify the contents
        let content = fs::read_to_string(dataset_info_file).unwrap();
        let config: Config = toml::from_str(&content).unwrap();
        assert_eq!(config.schema_version, 1, "Schema version should be 1");
        assert!(
            config.datasets.is_empty(),
            "Initial datasets list should be empty"
        );
    }

    #[test]
    fn test_update_dataset_info_add_new_dataset() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create dataset_info file
        create_dataset_info(Some(output_path.clone())).unwrap();

        // Update dataset_info by adding a new dataset
        let new_dataset = Dataset {
            version: 1,
            format: "json".to_string(),
            timestamp: None,
            categories: vec!["test_category".to_string()],
            filepath: PathBuf::from("test/path"),
        };

        let result =
            update_dataset_info(Some(output_path.clone()), "test_dataset", new_dataset, true);
        assert!(result.is_ok(), "Updating dataset_info should succeed");

        // Verify the contents
        let dataset_info_file = output_path.join("dataset_info.toml");
        let content = fs::read_to_string(dataset_info_file).unwrap();
        let config: Config = toml::from_str(&content).unwrap();

        assert!(
            config.datasets.contains_key("test_dataset"),
            "Dataset should be added"
        );
        let dataset = config.datasets.get("test_dataset").unwrap();
        assert_eq!(dataset.format, "json", "Dataset format should be updated");
        assert!(
            dataset.categories.contains(&"test_category".to_string()),
            "Category should be added"
        );
        assert_eq!(
            dataset.filepath,
            PathBuf::from("test/path"),
            "Filepath should be updated"
        );
    }

    #[test]
    fn test_update_dataset_info_update_existing_dataset() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create dataset_info file with an initial dataset
        create_dataset_info(Some(output_path.clone())).unwrap();

        let initial_dataset = Dataset {
            version: 1,
            format: "json".to_string(),
            timestamp: None,
            categories: vec!["test_category".to_string()],
            filepath: PathBuf::from("test/path"),
        };

        update_dataset_info(
            Some(output_path.clone()),
            "test_dataset",
            initial_dataset,
            true,
        )
        .unwrap();

        // Update the existing dataset
        let updated_dataset = Dataset {
            version: 2,
            format: "sqlite".to_string(),
            timestamp: None,
            categories: vec!["test_category".to_string(), "new_category".to_string()],
            filepath: PathBuf::from("test/path"),
        };

        let result = update_dataset_info(
            Some(output_path.clone()),
            "test_dataset",
            updated_dataset,
            false,
        );
        assert!(result.is_ok(), "Updating existing dataset should succeed");

        // Verify the updated contents
        let dataset_info_file = output_path.join("dataset_info.toml");
        let content = fs::read_to_string(dataset_info_file).unwrap();
        let config: Config = toml::from_str(&content).unwrap();

        let dataset = config.datasets.get("test_dataset").unwrap();
        assert_eq!(dataset.format, "sqlite", "Dataset format should be updated");
        assert!(
            dataset.categories.contains(&"new_category".to_string()),
            "New category should be added"
        );
    }

    #[test]
    fn test_read_dataset_info() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create dataset_info file
        create_dataset_info(Some(output_path.clone())).unwrap();

        // Read dataset_info
        let result = read_dataset_info(Some(output_path.clone()));
        assert!(result.is_ok(), "Reading dataset_info should succeed");

        let config = result.unwrap();
        assert_eq!(config.schema_version, 1, "Schema version should be 1");
    }

    #[test]
    fn test_search_dataset_by_category() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create dataset_info file with two datasets
        create_dataset_info(Some(output_path.clone())).unwrap();

        let dataset1 = Dataset {
            version: 1,
            format: "json".to_string(),
            timestamp: None,
            categories: vec!["category1".to_string()],
            filepath: PathBuf::from("path1"),
        };

        update_dataset_info(Some(output_path.clone()), "dataset1", dataset1, true).unwrap();

        let dataset2 = Dataset {
            version: 1,
            format: "json".to_string(),
            timestamp: None,
            categories: vec!["category2".to_string()],
            filepath: PathBuf::from("path2"),
        };

        update_dataset_info(Some(output_path.clone()), "dataset2", dataset2, true).unwrap();

        // Search for datasets in "category1"
        let result = search_dataset_by_category(output_path.join("dataset_info.toml"), "category1");
        assert!(result.is_ok(), "Searching by category should succeed");

        let matching_datasets = result.unwrap();
        assert_eq!(matching_datasets.len(), 1, "Only one dataset should match");
        assert!(
            matching_datasets.contains_key("dataset1"),
            "Matching dataset should be 'dataset1'"
        );
    }

    #[test]
    fn test_get_dataset_file_paths() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        // Create dataset_info file with two datasets
        create_dataset_info(Some(output_path.clone())).unwrap();

        let dataset1 = Dataset {
            version: 1,
            format: "json".to_string(),
            timestamp: None,
            categories: vec!["category1".to_string()],
            filepath: PathBuf::from("path1"),
        };

        update_dataset_info(Some(output_path.clone()), "dataset1", dataset1, true).unwrap();

        let dataset2 = Dataset {
            version: 1,
            format: "json".to_string(),
            timestamp: None,
            categories: vec!["category2".to_string()],
            filepath: PathBuf::from("path2"),
        };

        update_dataset_info(Some(output_path.clone()), "dataset2", dataset2, true).unwrap();

        // Get file paths for all datasets
        let result = get_dataset_file_paths(output_path.join("dataset_info.toml"), None);
        assert!(result.is_ok(), "Getting file paths should succeed");

        let file_paths = result.unwrap();
        assert_eq!(file_paths.len(), 2, "There should be two file paths");
        assert!(
            file_paths.contains(&PathBuf::from("path1")),
            "File paths should include 'path1'"
        );
        assert!(
            file_paths.contains(&PathBuf::from("path2")),
            "File paths should include 'path2'"
        );
    }
}
