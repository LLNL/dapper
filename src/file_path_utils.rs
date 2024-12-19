// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use once_cell::sync::Lazy;
use regex::Regex;

pub enum NormalizedFileName {
    NormalizedSoname(NormalizedSoname),
    Normalized(String),
    Unchanged,
}

pub struct NormalizedSoname {
    pub name: String,
    pub version: Option<String>,
    pub soabi: Option<String>,
    pub normalized: bool,
}

pub fn normalize_file_name(name: &str) -> NormalizedFileName {
    // ".so." files with SOABI need some filtering by their real file extension such as:
    //  - "0001-MIPS-SPARC-fix-wrong-vfork-aliases-in-libpthread.so.patch"
    //  - "t.so.gz"
    //  - "libnss_cache_oslogin.so.2.8.gz"
    //  - "".libkcapi.so.hmac"
    //  - "local-ldconfig-ignore-ld.so.diff"
    //  - "scribus.so.qm"
    // After filtering, the only remaining odd ".so." cases are:
    //  - libpsmile.MPI1.so.0d
    //  - *.so.0.* (from happycodes-libsocket-dev)
    // ".so_" files only appear in sqlmap for UDF files to run on MySQL and PostgreSQL remote hosts
    //  - Source code: https://github.com/sqlmapproject/udfhack/tree/master
    //  - Binaries: https://github.com/sqlmapproject/sqlmap/tree/master/data/udf
    // ".so-" files are either not .so files (e.g. svg or something else) or are covered better by other matches:
    //  - libapache2-mod-wsgi-py3 installs both "mod_wsgi.so-3.12" and "mod_wsgi.so"
    if name.ends_with(".so")
        || (name.contains(".so.")
            && ![".gz", ".patch", ".diff", ".hmac", ".qm"]
                .iter()
                .any(|suffix| name.ends_with(suffix)))
    {
        return NormalizedFileName::NormalizedSoname(normalize_soname(name));
    }
    NormalizedFileName::Unchanged
}

// Assumption: the filename given is a shared object file
// Callers should do other checks on the file name to ensure this is the case
pub fn normalize_soname(soname: &str) -> NormalizedSoname {
    // Strip SOABI version if present (not considering this normalization)
    let (soname, soabi) = extract_soabi_version(soname);
    let soabi_version = if soabi.is_empty() {
        None
    } else {
        Some(soabi.to_string())
    };

    // Normalize cpython, pypy, and haskell library names
    if let Some(pos) = soname.find(".cpython-") {
        NormalizedSoname {
            name: normalize_cpython(soname, pos),
            version: None,
            soabi: soabi_version,
            normalized: true,
        }
    } else if let Some(pos) = soname.find(".pypy") {
        NormalizedSoname {
            name: normalize_pypy(soname, pos),
            version: None,
            soabi: soabi_version,
            normalized: true,
        }
    } else if soname.starts_with("libHS") {
        let (normalized_name, version, normalized) = normalize_haskell(soname);
        NormalizedSoname {
            name: normalized_name,
            version,
            soabi: soabi_version,
            normalized,
        }
    } else {
        // Not a cpython, pypy, or haskell library -- check for a version number at the end
        if let (normalized_name, Some(version)) = extract_version_suffix(soname) {
            NormalizedSoname {
                name: normalized_name,
                version: Some(version),
                soabi: soabi_version,
                normalized: true,
            }
        } else {
            NormalizedSoname {
                name: soname.to_string(),
                version: None,
                soabi: soabi_version,
                normalized: false,
            }
        }
    }
}

fn extract_soabi_version(soname: &str) -> (&str, &str) {
    let (soname, soabi) = if let Some(pos) = soname.find(".so.") {
        (&soname[..pos + 3], &soname[pos + 4..])
    } else {
        (soname, "")
    };
    (soname, soabi)
}

fn extract_version_suffix(soname: &str) -> (String, Option<String>) {
    // Extract the version number from the end of the file name
    // e.g. libfoo-1.2.3.so -> name: libfoo.so, version: 1.2.3
    static VERSION_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"-(\d+(\.\d+)+)\.so").unwrap());
    if let Some(captures) = VERSION_PATTERN.captures(soname) {
        let version = captures.get(1).map(|v| v.as_str().to_string());
        let base_soname = soname.rsplit_once('-');
        (format!("{}.so", base_soname.unwrap().0), version)
    } else {
        (soname.to_string(), None)
    }
}

fn normalize_cpython(soname: &str, pos: usize) -> String {
    // Remove cpython platform tags
    // e.g. stringprep.cpython-312-x86_64-linux-gnu.so -> stringprep.cpython.so
    format!("{}.cpython.so", &soname[..pos])
}

fn normalize_pypy(soname: &str, pos: usize) -> String {
    // Remove pypy platform tags (much less common than cpython)
    // e.g. tklib_cffi.pypy39-pp73-x86_64-linux-gnu.so -> tklib_cffi.pypy.so
    format!("{}.pypy.so", &soname[..pos])
}

fn normalize_haskell(soname: &str) -> (String, Option<String>, bool) {
    // GHC compiled library names follow the format: libHSsetlocale-<version>-<api_hash>-ghc<ghc_version>.so
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
        eprintln!("No GHC Version Number Found: {}", soname);
        (soname.to_string(), None, false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn do_soname_normalization_tests(
        test_cases: Vec<(&str, &str, Option<&str>, Option<&str>, bool)>,
    ) {
        for (input, expected_name, expected_version, expected_soabi, expected_normalized) in
            test_cases
        {
            let NormalizedSoname {
                name: normalized_soname,
                version,
                soabi,
                normalized,
                ..
            } = normalize_soname(input);
            assert_eq!(normalized_soname, expected_name);
            assert_eq!(version, expected_version.map(String::from));
            assert_eq!(soabi, expected_soabi.map(String::from));
            assert_eq!(normalized, expected_normalized);
        }
    }

    #[test]
    fn test_cpython_normalization() {
        #[rustfmt::skip]
        let test_cases = vec![
            ("stringprep.cpython-312-x86_64-linux-gnu.so", "stringprep.cpython.so", None, None, true),
            // This one is strange -- has x86-64 instead of x86_64
            ("libpytalloc-util.cpython-312-x86-64-linux-gnu.so", "libpytalloc-util.cpython.so", None, None, true),
            // This one is also a bit odd, has samba4 in the platform tag
            ("libsamba-net.cpython-312-x86-64-linux-gnu-samba4.so.0", "libsamba-net.cpython.so", None, Some("0"), true),
        ];
        do_soname_normalization_tests(test_cases);
    }

    #[test]
    fn test_pypy_normalization() {
        #[rustfmt::skip]
        let test_cases = vec![
            ("tklib_cffi.pypy39-pp73-x86_64-linux-gnu.so", "tklib_cffi.pypy.so", None, None, true),
        ];
        do_soname_normalization_tests(test_cases);
    }

    #[test]
    fn test_haskell_normalization() {
        #[rustfmt::skip]
        let test_cases = vec![
            ("libHSAgda-2.6.3-F91ij4KwIR0JAPMMfugHqV-ghc9.4.7.so", "libHSAgda.so", Some("2.6.3"), None, true),
            ("libHScpphs-1.20.9.1-1LyMg8r2jodFb2rhIiKke-ghc9.4.7.so", "libHScpphs.so", Some("1.20.9.1"), None, true),
            ("libHSrts-1.0.2_thr_debug-ghc9.4.7.so", "libHSrts.so", Some("1.0.2_thr_debug"), None, true),
        ];
        do_soname_normalization_tests(test_cases);
    }

    #[test]
    fn test_dash_version_suffix_normalization() {
        #[rustfmt::skip]
        let test_cases = vec![
            ("libsingular-factory-4.3.2.so", "libsingular-factory.so", Some("4.3.2"), None, true),
            // Filename includes an SOABI version
            ("libvtkIOCGNSReader-9.1.so.9.1.0", "libvtkIOCGNSReader.so", Some("9.1"), Some("9.1.0"), true),
            // No dots in the version number is not normalized -- many false positives with 32/64 bit markers
            ("switch.linux-amd64-64.so", "switch.linux-amd64-64.so", None, None, false),
            // Version number isn't at the end, so not normalized
            ("liblua5.3-luv.so.1", "liblua5.3-luv.so", None, Some("1"), false),
            // v prefixed versions not normalized since most match this false positive
            ("libvtkCommonSystem-pv5.11.so", "libvtkCommonSystem-pv5.11.so", None, None, false),
            // A few letters added to the end of the version number are not normalized
            ("libpsmile.MPI1.so.0d", "libpsmile.MPI1.so", None, Some("0d"), false),
            ("libdsdp-5.8gf.so", "libdsdp-5.8gf.so", None, None, false),
            // Potential + in the middle of a version number also makes so it won't be normalized
            ("libgupnp-dlna-0.10.5+0.10.5.so", "libgupnp-dlna-0.10.5+0.10.5.so", None, None, false),
            ("libsingular-omalloc-4.3.2+0.9.6.so", "libsingular-omalloc-4.3.2+0.9.6.so", None, None, false),
        ];
        do_soname_normalization_tests(test_cases);
    }

    #[test]
    fn test_weird_soabi_normalization() {
        #[rustfmt::skip]
        let test_cases = vec![
            //"*.so.0.*" (accidentally created file in happycoders-libsocket-dev? https://bugs.launchpad.net/ubuntu/+source/libsocket/+bug/636598)
            ("*.so.0.*", "*.so", None, Some("0.*"), false),
        ];
        do_soname_normalization_tests(test_cases);
    }
}
