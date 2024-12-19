// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use crate::database::read_contents_file;
use once_cell::sync::Lazy;
use regex::Regex;

// Eventually will want to update this to return the normalized name, captured version number, and rules applied
// Assumption: The key/filename given ends in ".so"
// Returns the normalized name, the version number if present, and whether normalization was applied
fn process_shared_object(soname: &str) -> (String, Option<String>, bool) {
    if let Some(pos) = soname.find(".cpython-") {
        // For matching cpython libraries, remove the platform specific tags
        // e.g. stringprep.cpython-312-x86_64-linux-gnu.so -> stringprep.cpython.so
        let base_soname = format!("{}.cpython.so", &soname[..pos]);
        (base_soname, None, true)
    } else if let Some(pos) = soname.find(".pypy") {
        // For matching pypy libraries, remove the platform specific tags (much less common)
        let base_soname = format!("{}.pypy.so", &soname[..pos]);
        (base_soname, None, true)
    } else if soname.starts_with("libHS") {
        // Things compiled by the haskell compiler also have a pretty standard format: libHSsetlocale-<version>-<api_hash>-ghc<ghc_version>.so
        // The API hash may or may not be present. The version number is always present.
        if let Some(pos) = soname.rfind("-ghc") {
            match soname[..pos]
                .rsplit('-')
                .next()
                .map(|api_hash| {
                    // remove the API hash part of the file name if it is present
                    if (api_hash.len() == 22 || api_hash.len() == 21 || api_hash.len() == 20)
                        && api_hash.chars().all(|c| c.is_ascii_alphanumeric())
                    {
                        soname[..pos - api_hash.len() - 1].to_string()
                    } else {
                        soname[..pos].to_string()
                    }
                })
                .map(|name| {
                    // Pull out the version number portion of the name (seems to always be present for libHS ghc libraries)
                    // some version numbers may have suffixes such as _thr and _debug
                    name.rsplit_once('-')
                        .map(|(name, version)| (format!("{}.so", name), Some(version.to_string())))
                })
                .unwrap()
            {
                Some((base_soname, version)) => (base_soname, version, true),
                None => ("".to_string(), None, true),
            }
        } else {
            // No ghc version number found -- maybe not a valid haskell library?
            println!("No GHC Version Number Found: {}", soname);
            (soname.to_string(), None, true)
        }
    } else {
        // Not a cpython, pypy, or haskell library -- check for a version number at the end
        static VERSION_PATTERN: Lazy<Regex> =
            Lazy::new(|| Regex::new(r"-(\d+(\.\d+)+)\.so").unwrap());

        if let Some(captures) = VERSION_PATTERN.captures(soname) {
            let version = captures.get(1).map(|v| v.as_str().to_string());
            let base_soname = soname
                .rsplit_once('-')
                .map(|(name, _)| format!("{}.so", name));
            (base_soname.unwrap(), version, true)
        } else {
            (soname.to_string(), None, false)
        }
    }
}

pub fn so_name_normalization() {
    let package_map = read_contents_file("Contents-amd64-noble");
    for key in package_map.keys() {
        // ".so.", ".so-", and ".so_" should probably get renamed to end in ".so", then normalized in the same way
        // Additional potential filters to remove more version numbers:
        // - v\d+(\.\d+)*.*\.so -- 478/39k have something looking like a v-prefixed version... most are pv: libvtkCommonSystem-pv5.11.so
        // - -\d+(\.\d+)+\.so -- 1076/39k that end in a -<version> suffix
        // - -\d+(\.\d+)+.*\.so -- 1166/39k that have a -<version> in them somewhere (captures both prev and next pattern -- with 1-2 exceptions, libdsdp-5.8gf.so and libsingular-omalloc-4.3.2+0.9.6.so)
        // - -\d+(\.\d+)+-.*\.so -- 89/39k have "skewered" version numbers in the middle, often followed by a CPU arch
        // - \d+(\.\d+)+-.*\.so -- in addition to previous, mostly catches liblua5.*- names
        // - \d+(_\d+)+.*\.so -- 139/39k, underscore separated version numbers aren't popular and lots of false positives (x86_64)
        // - popular to append a 64 or 32 to denote number of bits for system
        // - \d+(-\d+)+.*\.so -- 1012/39k numbers separated by a "-", 888 are amd64-64, amd64-32, and amd64-linux
        // - \d+(\.\d+)*\+.*\.so -- 17/39k version number-ish things followed by a "+"
        if key.ends_with(".so") {
            let (normalized_soname, version, normalized) = process_shared_object(key);
            match package_map.contains_key(&normalized_soname) && normalized {
                false => println!(
                    "{} (Version: {})",
                    normalized_soname,
                    version.unwrap_or("None".to_string())
                ),
                true => {
                    // For multiple packages with the same name, usually due to vendoring in "plugins"
                    // Need to add functions for recognizing /usr/lib/x86_64-linux-gnu/... names to discern the correct package
                    // Also need functions for normalizing package names (removing -dev and -3.4.2 version suffixes and -3.4.2-dev suffixes)
                    // Mapping to source package names may collapse most of these cases...
                    // libpipewire, libgrilo, and libruby may be exceptions to these rules -- for the /usr/lib stuff, do we also need a way to check libblah-<version>.so within them being a match? abstracted so version in package name returned gets correctly replaced?
                    // check libodin vs mitools packages too -- are they the same thing?
                    // libmjpegtools and libmplex (or liblavjpeg)? libgstreamer vs libgupnp-dlna? lib++dfb vs libdirectfb? libp4est-sc vs libp4est?
                    let original_package_names = package_map.get(key).unwrap();
                    let normalized_package_names = package_map.get(&normalized_soname).unwrap();
                    let collision_detected = original_package_names.iter().any(|(orig_pkg, _)| {
                        normalized_package_names
                            .iter()
                            .any(|(norm_pkg, _)| orig_pkg == norm_pkg)
                    });

                    if collision_detected {
                        println!(
                            "Collision {} from {} with matching package names",
                            normalized_soname, key
                        );
                    } else {
                        println!(
                            "Collision {} from {} with different package names",
                            normalized_soname, key
                        );
                        println!("Original package names: {:?}", original_package_names);
                        println!("Normalized package names: {:?}", normalized_package_names);
                    }
                }
            }
        } else if key.contains(".so.")
            && ![".gz", ".patch", ".diff", ".hmac", ".qm"]
                .iter()
                .any(|suffix| key.ends_with(suffix))
        {
            // Filter out files such as 0001-MIPS-SPARC-fix-wrong-vfork-aliases-in-libpthread.so.patch, t.so.gz, getmax.so.gz, "*.so.0.*" (what?), .libkcapi.so.hmac, libnss_cache_oslogin.so.2.8.gz, local-ldconfig-ignore-ld.so.diff, scribus.so.qm
            // Interesting SOABI version for a file with a letter in it: libpsmile.MPI1.so.0d
            let base_key = format!("{}.so", key.split(".so.").next().unwrap());
            if !package_map.contains_key(&base_key) {
                let (normalized_soname, version, normalized) =
                    process_shared_object(base_key.as_str());
                match package_map.contains_key(&normalized_soname) && normalized {
                    false => println!(
                        "{} (Version: {})",
                        normalized_soname,
                        version.unwrap_or("None".to_string())
                    ),
                    true => {
                        let original_package_names = package_map.get(key).unwrap();
                        let normalized_package_names = package_map.get(&normalized_soname).unwrap();
                        let collision_detected =
                            original_package_names.iter().any(|(orig_pkg, _)| {
                                normalized_package_names
                                    .iter()
                                    .any(|(norm_pkg, _)| orig_pkg == norm_pkg)
                            });

                        if collision_detected {
                            println!(
                                "Collision {} from {} with matching package names",
                                normalized_soname, key
                            );
                        } else {
                            println!(
                                "Collision {} from {} with different package names",
                                normalized_soname, key
                            );
                            println!("Original package names: {:?}", original_package_names);
                            println!("Normalized package names: {:?}", normalized_package_names);
                        }
                    }
                }
            }
        } else if key.contains(".so-") {
            // Decide if the "Kernels" ending in ".hsaco" need to be filtered out
            let base_key = format!("{}.so", key.split(".so-").next().unwrap());
            if !package_map.contains_key(&base_key) {
                println!("{}", key);
            }
        } else if key.contains(".so_") {
            // Decide if this category should be included -- basically just lib_postgresqludf_sys.so_
            let base_key = format!("{}.so", key.split(".so_").next().unwrap());
            if !package_map.contains_key(&base_key) {
                println!("{}", key);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn do_soname_normalization_tests(test_cases: Vec<(&str, &str, Option<&str>, bool)>) {
        for (input, expected_name, expected_version, expected_normalized) in test_cases {
            let (normalized_soname, version, normalized) = process_shared_object(input);
            assert_eq!(normalized_soname, expected_name);
            assert_eq!(version, expected_version.map(String::from));
            assert_eq!(normalized, expected_normalized);
        }
    }

    #[test]
    fn test_cpython_normalization() {
        #[rustfmt::skip]
        let test_cases = vec![
            ("stringprep.cpython-312-x86_64-linux-gnu.so", "stringprep.cpython.so", None, true),
            // This one is strange -- has x86-64 instead of x86_64
            ("libpytalloc-util.cpython-312-x86-64-linux-gnu.so", "libpytalloc-util.cpython.so", None, true),
            // This one is also a bit odd, has samba4 in the platform tag
            ("libsamba-net.cpython-312-x86-64-linux-gnu-samba4.so.0", "libsamba-net.cpython.so", None, true),
        ];
        do_soname_normalization_tests(test_cases);
    }

    #[test]
    fn test_pypy_normalization() {
        #[rustfmt::skip]
        let test_cases = vec![
            ("tklib_cffi.pypy39-pp73-x86_64-linux-gnu.so", "tklib_cffi.pypy.so", None, true),
        ];
        do_soname_normalization_tests(test_cases);
    }

    #[test]
    fn test_haskell_normalization() {
        #[rustfmt::skip]
        let test_cases = vec![
            ("libHSAgda-2.6.3-F91ij4KwIR0JAPMMfugHqV-ghc9.4.7.so", "libHSAgda.so", Some("2.6.3"), true),
            ("libHScpphs-1.20.9.1-1LyMg8r2jodFb2rhIiKke-ghc9.4.7.so", "libHScpphs.so", Some("1.20.9.1"), true),
            ("libHSrts-1.0.2_thr_debug-ghc9.4.7.so", "libHSrts.so", Some("1.0.2_thr_debug"), true),
        ];
        do_soname_normalization_tests(test_cases);
    }

    #[test]
    fn test_dash_version_suffix_normalization() {
        #[rustfmt::skip]
        let test_cases = vec![
            ("libsingular-factory-4.3.2.so", "libsingular-factory.so", Some("4.3.2"), true),
            // Add libvtkIOCGNSReader-9.1.so.9.1.0 as a test case if function is updated to handle .so. directly
            ("libvtkIOCGNSReader-9.1.so", "libvtkIOCGNSReader.so", Some("9.1"), true),
            // No dots in the version number is not normalized -- many false positives with 32/64 bit markers
            ("switch.linux-amd64-64.so", "switch.linux-amd64-64.so", None, false),
            // Version number isn't at the end, so not normalized
            ("liblua5.3-luv.so.1", "liblua5.3-luv.so.1", None, false),
            // v prefixed versions not normalized since most match this false positive
            ("libvtkCommonSystem-pv5.11.so", "libvtkCommonSystem-pv5.11.so", None, false),
            // A few letters added to the end of the version number are not normalized
            ("libpsmile.MPI1.so.0d", "libpsmile.MPI1.so.0d", None, false),
            ("libdsdp-5.8gf.so", "libdsdp-5.8gf.so", None, false),
            // Potential + in the middle of a version number also makes so it won't be normalized
            ("libgupnp-dlna-0.10.5+0.10.5.so", "libgupnp-dlna-0.10.5+0.10.5.so", None, false),
            ("libsingular-omalloc-4.3.2+0.9.6.so", "libsingular-omalloc-4.3.2+0.9.6.so", None, false),
        ];
        do_soname_normalization_tests(test_cases);
    }
}
