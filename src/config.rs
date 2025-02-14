// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use directories::ProjectDirs;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
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
 
pub fn get_base_directory() -> Option<PathBuf>
{
    match ProjectDirs::from("", "", "dapper") {
        Some(base_dirs) =>  Some(base_dirs.data_local_dir().to_path_buf()),
        _ => None,
    }
}

pub fn create_metadata_file(output_path: Option<PathBuf>) -> std::io::Result<()>
{
    let mut packages = HashMap::new();
    // uncomment this out to initialize the metadata file with some data.
    // packages.insert(
    //     "linux_debian".to_string(),
    //     Package {
    //         version: 1,
    //         format: "sqlite".to_string(),
    //         timestamp: Utc::now(),
    //         categories: vec![
    //             "category1".to_string(),
    //             "category2".to_string(),
    //             "category3".to_string(),
    //         ],
    //         filepath: PathBuf::from("this/is/a/test/file/path.sqlite"),
    //     },
    // );

    // packages.insert(
    //     "nuget".to_string(),
    //     Package {
    //         version: 1,
    //         format: "sqlite".to_string(),
    //         timestamp: Utc::now(),
    //         categories: vec![
    //             "categoryA".to_string(),
    //             "categoryB".to_string(),
    //             "categoryC".to_string(),
    //         ],
    //         filepath: PathBuf::from("this/is/a/test/file/path.sqlite"),
    //     },
    // );

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
    // println!("using read path: {}", file_path.display());
    // Read the file into a string
    let content = std::fs::read_to_string(file_path.clone())?;
    println!("file read successful");
    // Print the file contents
    // println!("Raw content: \n{}", content);
    // Convert the contents of the file into a Table 
    let config: Table = content.parse()?;
    // eprintln!("file contents: \n{:#?}", &config);
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


// add unit tests for the functions here