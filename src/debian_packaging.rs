use std::collections::HashMap;
use std::fs;

pub fn read_contents_file(file_path: &str) -> HashMap<String, Vec<(String, String)>> {
    let mut package_map = HashMap::new();
    let contents =
        fs::read_to_string(file_path).expect("Failed to read name to package mapping file");

    for line in contents.lines() {
        if let Some((file_path, package_name)) = line.rsplit_once([' ', '\t'].as_ref()) {
            let file_name = file_path.trim_end().rsplit('/').next().unwrap().to_string();
            package_map
                .entry(file_name)
                .or_insert_with(Vec::new)
                .push((package_name.to_string(), file_path.trim_end().to_string()));
        }
    }

    package_map
}
