use chrono::{DateTime, Utc};
use serde::{
    de::{self, Deserializer},
    Deserialize,
};
use std::collections::HashMap;
use std::error::Error;

const DATASET_LIST_URL: &str = "https://dapper.readthedocs.io/en/latest/dataset_list.toml";

#[derive(Deserialize, Debug)]
pub struct RemoteCatalog {
    pub schema_version: u8,
    pub datasets: HashMap<String, RemoteDataset>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RemoteDataset {
    pub version: String,
    pub format: String,
    #[serde(deserialize_with = "deserialize_timestamp")]
    pub timestamp: Option<DateTime<Utc>>,
    pub filepath: String,
    pub categories: Vec<String>,
    pub urls: Vec<String>,
}

fn deserialize_timestamp<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(s) => {
            // Parse ISO 8601 for DateTime Object
            if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
                return Ok(Some(dt.with_timezone(&Utc)));
            }

            // Handle simple Date String to make a DateTime Object
            let date_str = format!("{} 00:00:00 +0000", s);
            DateTime::parse_from_str(&date_str, "%Y-%m-%d %H:%M:%S %z")
                .map(|dt| Some(dt.with_timezone(&Utc)))
                .map_err(|e| de::Error::custom(format!("Invalid timestamp format: {}", e)))
        }
        None => Ok(None),
    }
}

pub fn read_dataset_list() -> Result<RemoteCatalog, Box<dyn Error>> {
    let response = reqwest::blocking::get(DATASET_LIST_URL)
        .map_err(|e| format!("Failed to fetch dataset catalog: {e}"))?;

    if !response.status().is_success() {
        return Err(format!(
            "Failed to fetch dataset catalog: HTTP {}",
            response.status()
        )
        .into());
    }

    let content = response
        .text()
        .map_err(|e| format!("Failed to read response: {e}"))?;

    let catalog: RemoteCatalog = toml::from_str(&content)?;
    Ok(catalog)
}
