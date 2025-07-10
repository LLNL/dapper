import pytest
from dapper_python.normalize import normalize_file_name, NormalizedFileName


def do_soname_normalization_tests(test_cases):
    for (
        input_name,
        expected_name,
        expected_version,
        expected_soabi,
        expected_normalized,
    ) in test_cases:
        result = normalize_file_name(input_name)
        if isinstance(result, NormalizedFileName):
            assert result.name == expected_name
            assert result.version == expected_version
            assert result.soabi == expected_soabi
            assert result.normalized == expected_normalized
        else:
            assert result == expected_name


def test_basic_normalization():
    test_cases = [
        ("libexample.so", "libexample.so", None, None, False),
        ("libexample.so.1", "libexample.so", None, "1", False),
        ("libexample-1.2.3.so", "libexample.so", "1.2.3", None, True),
    ]
    do_soname_normalization_tests(test_cases)


def test_edge_cases():
    test_cases = [
        ("libexample.so.gz", "libexample.so.gz", None, None, False),
        ("libexample.so.patch", "libexample.so.patch", None, None, False),
        ("libexample.so.diff", "libexample.so.diff", None, None, False),
        ("libexample.so.hmac", "libexample.so.hmac", None, None, False),
        ("libexample.so.qm", "libexample.so.qm", None, None, False),
    ]
    do_soname_normalization_tests(test_cases)


def test_version_extraction():
    test_cases = [
        ("libexample-1.2.3.so", "libexample.so", "1.2.3", None, True),
        ("libexample-1.2.3.4.so", "libexample.so", "1.2.3.4", None, True),
        ("libexample-1.2.3-beta.so", "libexample-1.2.3-beta.so", None, None, False),
    ]
    do_soname_normalization_tests(test_cases)


def test_soabi_handling():
    test_cases = [
        ("libexample.so.0d", "libexample.so", None, "0d", False),
        ("libexample.so.1", "libexample.so", None, "1", False),
        ("libexample.so.1.2.3", "libexample.so", None, "1.2.3", False),
        ("libexample.so.1.2.3.4", "libexample.so", None, "1.2.3.4", False),
    ]
    do_soname_normalization_tests(test_cases)


def test_cpython_normalization():
    test_cases = [
        ("stringprep.cpython-312-x86_64-linux-gnu.so", "stringprep.cpython.so", None, None, True),
        # This one is strange -- has x86-64 instead of x86_64
        (
            "libpytalloc-util.cpython-312-x86-64-linux-gnu.so",
            "libpytalloc-util.cpython.so",
            None,
            None,
            True,
        ),
        # This one is also a bit odd, has samba4 in the platform tag
        (
            "libsamba-net.cpython-312-x86-64-linux-gnu-samba4.so.0",
            "libsamba-net.cpython.so",
            None,
            "0",
            True,
        ),
    ]
    do_soname_normalization_tests(test_cases)


def test_pypy_normalization():
    test_cases = [
        ("tklib_cffi.pypy39-pp73-x86_64-linux-gnu.so", "tklib_cffi.pypy.so", None, None, True),
    ]
    do_soname_normalization_tests(test_cases)


def test_haskell_normalization():
    test_cases = [
        ("libHSAgda-2.6.3-F91ij4KwIR0JAPMMfugHqV-ghc9.4.7.so", "libHSAgda.so", "2.6.3", None, True),
        (
            "libHScpphs-1.20.9.1-1LyMg8r2jodFb2rhIiKke-ghc9.4.7.so",
            "libHScpphs.so",
            "1.20.9.1",
            None,
            True,
        ),
        ("libHSrts-1.0.2_thr_debug-ghc9.4.7.so", "libHSrts.so", "1.0.2_thr_debug", None, True),
        ("libHSrts-ghc8.6.5.so", "libHSrts.so", None, None, True),
    ]
    do_soname_normalization_tests(test_cases)


def test_dash_version_suffix_normalization():
    test_cases = [
        ("libsingular-factory-4.3.2.so", "libsingular-factory.so", "4.3.2", None, True),
        # Filename includes an SOABI version
        ("libvtkIOCGNSReader-9.1.so.9.1.0", "libvtkIOCGNSReader.so", "9.1", "9.1.0", True),
        # No dots in the version number is not normalized -- many false positives with 32/64 bit markers
        ("switch.linux-amd64-64.so", "switch.linux-amd64-64.so", None, None, False),
        # Version number isn't at the end, so not normalized
        ("liblua5.3-luv.so.1", "liblua5.3-luv.so", None, "1", False),
        # v prefixed versions not normalized since most match this false positive
        ("libvtkCommonSystem-pv5.11.so", "libvtkCommonSystem-pv5.11.so", None, None, False),
        # A few letters added to the end of the version number are not normalized
        ("libpsmile.MPI1.so.0d", "libpsmile.MPI1.so", None, "0d", False),
        ("libdsdp-5.8gf.so", "libdsdp-5.8gf.so", None, None, False),
        # Potential + in the middle of a version number also makes so it won't be normalized
        ("libgupnp-dlna-0.10.5+0.10.5.so", "libgupnp-dlna-0.10.5+0.10.5.so", None, None, False),
        (
            "libsingular-omalloc-4.3.2+0.9.6.so",
            "libsingular-omalloc-4.3.2+0.9.6.so",
            None,
            None,
            False,
        ),
    ]
    do_soname_normalization_tests(test_cases)


def test_weird_soabi_normalization():
    test_cases = [
        # "*.so.0.*" (accidentally created file in happycoders-libsocket-dev? https://bugs.launchpad.net/ubuntu/+source/libsocket/+bug/636598)
        ("*.so.0.*", "*.so", None, "0.*", False),
    ]
    do_soname_normalization_tests(test_cases)
