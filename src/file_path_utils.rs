// Copyright 2024 Lawrence Livermore National Security, LLC
// See the top-level LICENSE file for details.
//
// SPDX-License-Identifier: MIT

use std::path::{Path, PathBuf};
use path_slash::PathBufExt;
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
                // Pull out the version number portion of the name if it is present
                // some version numbers may have suffixes such as _thr and _debug
                name.rsplit_once('-').map_or_else(
                    || (format!("{}.so", name), None),
                    |(name, version)| (format!("{}.so", name), Some(version.to_string())),
                )
            }) {
            Some((base_soname, version)) => (base_soname, version, true),
            None => ("".to_string(), None, true),
        }
    } else {
        // No ghc version number found -- maybe not a valid haskell library?
        eprintln!("No GHC Version Number Found: {}", soname);
        (soname.to_string(), None, false)
    }
}

static MULTIARCH_PATTERN: Lazy<Regex> = Lazy::new(|| {
    //Some "known" values were pulled from: https://wiki.debian.org/Multiarch/Tuples

    //TODO: Are there any additional sources/values that we'd want
    //Or some better way to get this than these hard-coded values
    let architectures = [
        "aarch64",
        "alpha",
        "amd64",
        "arc",
        "arm",
        "arm64",
        "arm64ilp32",
        "armel",
        "armhf",
        "hppa",
        "hurd-amd64",
        "hurd-i386",
        "i386",
        "ia64",
        "kfreebsd-amd64",
        "kfreebsd-i386",
        "loong64",
        "m68k",
        "mips",
        "mips64",
        "mips64el",
        "mips64r6",
        "mips64r6el",
        "mipsel",
        "mipsn32",
        "mipsn32el",
        "mipsn32r6",
        "mipsn32r6el",
        "mipsr6",
        "mipsr6el",
        "powerpc",
        "powerpc64",
        "powerpcspe",
        "ppc64",
        "ppc64el",
        "riscv",
        "riscv64",
        "s390",
        "s390x",
        "sh4",
        "sparc",
        "sparc64",
        "uefi-amd64",
        "uefi-arm64",
        "uefi-armhf",
        "uefi-i386",
        "x32",
        "x86",
        "x86_64",
    ];
    let architectures_pattern = architectures
        .map(|s| format!(r"(?:{})",s))
        .join("|");

    let vendors = [
        "apple",
        "debian",
        "ibm",
        "microsoft",
        "none",
        "nvidia",
        "pc",
        "redhat",
        "suse",
        "ubuntu",
        "unknown",
    ];
    let vendors_pattern = vendors
        .map(|s| format!(r"(?:{})",s))
        .join("|");

    let os = [
        "aix",
        "android",
        "darwin",
        "freebsd",
        "linux",
        "netbsd",
        "openbsd",
        "solaris",
        "windows",
    ];
    let os_pattern = os
        .map(|s| format!(r"(?:{})",s))
        .join("|");

    let libs = [
        "eabi",
        "eabihf",
        "gnu",
        "musl",
        "uclibc",
    ];
    let libs_pattern = libs
        .map(|s| format!(r"(?:{})",s))
        .join("|");

    let regex_pattern = format!(
        r"(?x)
        (?<arch>{arch})
        (?:-(?<vendor>{vendor}))?   #Vendor is optional
        -(?<os>{os})
        (?:-(?<lib>{lib}))?     #Lib is optional (ex: linux vs linux-gnu)
        ",
        arch=architectures_pattern,
        vendor=vendors_pattern,
        os=os_pattern,
        lib=libs_pattern,
    );
    Regex::new(&regex_pattern).expect("Failed to compile regex")
});

/// Normalizes a string containing a multiarch tuple
///
/// **Ex:** `usr/lib/x86_64-unknown-linux-gnu/bin/gcc-ld/ld` -> `usr/lib/bin/gcc-ld/ld`
///
/// More information on multarch tuples can be found here:
/// * https://wiki.debian.org/Multiarch/Tuples
/// * https://wiki.ubuntu.com/MultiarchSpec
fn normalize_multiarch(path: &str) -> (String, Option<Vec<&str>>, bool) {
    let path = Path::new(path);

    let mut working_path = PathBuf::new();
    let mut matches = Vec::<&str>::new();

    //Process parents/ancestors
    if let Some(parent) = path.parent() {
        for component in parent.components(){
            //We should be able to just unwrap the call to to_str
            //Since the path was originally constructed from a &str
            //And &str cannot contain non-unicode characters
            let component = component.as_os_str().to_str().unwrap();

            if MULTIARCH_PATTERN.is_match(component){
                matches.push(component);
            }
            else {
                working_path.push(component);
            }
        }
    }

    //Re-add the filename itself
    if let Some(filename) = path.file_name() {
        working_path.push(filename);
    }

    let normalized_path = working_path.to_slash().unwrap().to_string();
    if matches.is_empty(){
        (normalized_path, None, false)
    }
    else{
        (normalized_path, Some(matches), true)
    }
}

/// Checks if two paths are the same after some normalization, such as ignoring multiarch tuples
///
/// **Ex:** Both `usr/lib/x86_64-unknown-linux-gnu/bin/gcc-ld/ld` and `usr/lib/bin/gcc-ld/ld`
/// would be treated as equivalent and thus return `true`
///
/// Currently only ignores multiarch tuples and does not normalize the filename itself
fn match_canonical_path(path1: &str, path2: &str) -> bool {

    ///Creates an iterator that yields only the non-multiarch directory components of path
    fn iter_components(path: &Path) -> impl Iterator<Item = &str>{
        path.parent()
            .unwrap_or_else(|| Path::new(""))
            .components()
            .filter_map(|component| {
                //Same as above, as the path was passed as a &str, we know it contains valid Unicode
                let component = component.as_os_str().to_str().unwrap();

                if MULTIARCH_PATTERN.is_match(component){
                    None
                }
                else{
                    Some(component)
                }
            })
    }

    let path1 = Path::new(path1);
    let path2 = Path::new(path2);

    if path1.file_name() != path2.file_name(){
        return false;
    }
    iter_components(path1).eq(iter_components(path2))
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
            ("libHSrts-ghc8.6.5.so", "libHSrts.so", None, None, true),
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

    #[test]
    fn test_multiarch_normalization() {
        #[rustfmt::skip]
        let test_cases = vec![
            (
                "usr/lib/rust-1.74/lib/rustlib/x86_64-unknown-linux-gnu/bin/gcc-ld/ld",
                "usr/lib/rust-1.74/lib/rustlib/bin/gcc-ld/ld",
                Some(vec!["x86_64-unknown-linux-gnu"]),
                true,
            ),
            (
                "usr/share/cargo/registry/libm-0.2.7/ci/docker/aarch64-unknown-linux-gnu/Dockerfile",
                "usr/share/cargo/registry/libm-0.2.7/ci/docker/Dockerfile",
                Some(vec!["aarch64-unknown-linux-gnu"]),
                true,
            ),
            (
                "usr/share/doc/language-pack-gnome-az/copyright",
                "usr/share/doc/language-pack-gnome-az/copyright",
                None,
                false,
            ),
            //Interesting case with a multiarch tuple, but not a directory
            (
                "usr/share/cargo/registry/cryptoki-sys-0.1.7/src/bindings/aarch64-apple-darwin.rs",
                "usr/share/cargo/registry/cryptoki-sys-0.1.7/src/bindings/aarch64-apple-darwin.rs",
                None,
                false,
            ),
            //Multiple multiarches
            (
                "usr/share/cargo/x86_64-unknown-linux-gnu/ci/docker/aarch64-unknown-linux-gnu/samplefile",
                "usr/share/cargo/ci/docker/samplefile",
                Some(vec!["x86_64-unknown-linux-gnu","aarch64-unknown-linux-gnu"]),
                true,
            ),
            //Filename only
            (
                "sample_filename.json",
                "sample_filename.json",
                None,
                false
            )
        ];

        for (input, expected_path, expected_replacements, expected_changed) in test_cases {
            let (normalized_path, replacements, changed) = normalize_multiarch(input);

            assert_eq!(normalized_path, expected_path);
            assert_eq!(replacements, expected_replacements);
            assert_eq!(changed, expected_changed);
            
            assert!(match_canonical_path(input, expected_path))
        }
    }
}
