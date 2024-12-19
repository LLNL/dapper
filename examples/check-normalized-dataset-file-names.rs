use dapper::debian_packaging::read_contents_file;
use dapper::file_path_utils::{normalize_file_name, NormalizedFileName, NormalizedSoname};

fn main() {
    let package_map = read_contents_file("Contents-amd64-noble");
    for key in package_map.keys() {
        match normalize_file_name(key) {
            NormalizedFileName::NormalizedSoname(NormalizedSoname {
                name: normalized_soname,
                version,
                soabi,
                normalized,
            }) => match normalized {
                false => println!(
                    "{} (Version: {}, SOABI: {})",
                    normalized_soname,
                    version.unwrap_or("None".to_string()),
                    soabi.unwrap_or("None".to_string())
                ),
                true => {
                    static EMPTY_VEC: Vec<(String, String)> = Vec::new();
                    let original_package_names = package_map.get(key).unwrap_or(&EMPTY_VEC);
                    let normalized_package_names =
                        package_map.get(&normalized_soname).unwrap_or(&EMPTY_VEC);
                    let collision_detected = original_package_names.iter().any(|(orig_pkg, _)| {
                        normalized_package_names
                            .iter()
                            .any(|(norm_pkg, _)| orig_pkg != norm_pkg)
                    });

                    if collision_detected {
                        println!(
                            "Collision {} from {} with different package names",
                            normalized_soname, key
                        );
                        println!("Original package names: {:?}", original_package_names);
                        println!("Normalized package names: {:?}", normalized_package_names);
                    }
                }
            },
            NormalizedFileName::Normalized(normalized_name) => {
                println!("{}", normalized_name);
            }
            NormalizedFileName::Unchanged => (),
        }
    }
}
