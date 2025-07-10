import re

from dataclasses import dataclass

from typing import Optional, Union


@dataclass
class NormalizedFileName:
    """
    Represents a normalized file name with optional version and SOABI information.

    Attributes:
        name (str): The normalized file name.
        version (Optional[str]): The version number, if available.
        soabi (Optional[str]): The SOABI version, if available.
        normalized (bool): Indicates if the file name was normalized.
    """

    name: str
    version: Optional[str] = None
    soabi: Optional[str] = None
    normalized: bool = False

    def __str__(self) -> str:
        return self.name


def normalize_file_name(name: str) -> Union[NormalizedFileName, str]:
    """
    Normalize a shared library file name.

    Args:
        name (str): The file name to normalize.

    Returns:
        Union[NormalizedFileName, str]: A NormalizedFileName object if the file name is a shared library,
        otherwise the original file name.
    """
    if name.endswith(".so") or (
        ".so." in name
        and not any(name.endswith(suffix) for suffix in [".gz", ".patch", ".diff", ".hmac", ".qm"])
    ):
        return normalize_soname(name)
    return name


def normalize_soname(soname: str) -> NormalizedFileName:
    """
    Normalize a shared object file name.

    Args:
        soname (str): The shared object file name to normalize.

    Returns:
        NormalizedFileName: A NormalizedFileName object with the normalized name, version, and SOABI information.
    """
    soname, soabi = extract_soabi_version(soname)
    soabi_version = soabi if soabi else None

    if ".cpython-" in soname:
        pos = soname.find(".cpython-")
        return NormalizedFileName(
            normalize_cpython(soname, pos), soabi=soabi_version, normalized=True
        )
    elif ".pypy" in soname:
        pos = soname.find(".pypy")
        return NormalizedFileName(normalize_pypy(soname, pos), soabi=soabi_version, normalized=True)
    elif soname.startswith("libHS"):
        normalized_name, version, normalized = normalize_haskell(soname)
        return NormalizedFileName(normalized_name, version, soabi_version, normalized)
    else:
        normalized_name, version = extract_version_suffix(soname)
        if version:
            return NormalizedFileName(normalized_name, version, soabi_version, True)
        return NormalizedFileName(soname, soabi=soabi_version, normalized=False)


def extract_soabi_version(soname: str) -> (str, str):
    """
    Extract the SOABI version from a shared object file name.

    Args:
        soname (str): The shared object file name.

    Returns:
        (str, str): A tuple containing the base file name and the SOABI version.
    """
    if ".so." in soname:
        pos = soname.find(".so.")
        return soname[: pos + 3], soname[pos + 4 :]
    return soname, ""


def extract_version_suffix(soname: str) -> (str, Optional[str]):
    """
    Extract the version number from a shared object file name.

    Args:
        soname (str): The shared object file name.

    Returns:
        (str, Optional[str]): A tuple containing the base file name and the version number, if available.
    """
    version_pattern = re.compile(r"-(\d+(\.\d+)+)\.so")
    match = version_pattern.search(soname)
    if match:
        version = match.group(1)
        base_soname = soname.rsplit("-", 1)[0]
        return f"{base_soname}.so", version
    return soname, None


def normalize_cpython(soname: str, pos: int) -> str:
    """
    Normalize a CPython shared object file name.

    Args:
        soname (str): The shared object file name.
        pos (int): The position of the CPython tag in the file name.

    Returns:
        str: The normalized file name.
    """
    return f"{soname[:pos]}.cpython.so"


def normalize_pypy(soname: str, pos: int) -> str:
    """
    Normalize a PyPy shared object file name.

    Args:
        soname (str): The shared object file name.
        pos (int): The position of the PyPy tag in the file name.

    Returns:
        str: The normalized file name.
    """
    return f"{soname[:pos]}.pypy.so"


def normalize_haskell(soname: str) -> (str, Optional[str], bool):
    """
    Normalize a Haskell shared object file name.

    Args:
        soname (str): The shared object file name.

    Returns:
        (str, Optional[str], bool): A tuple containing the normalized file name, version number, and a boolean
        indicating if the file name was normalized.
    """
    if "-ghc" in soname:
        pos = soname.rfind("-ghc")
        name = soname[:pos]
        api_hash = name.rsplit("-", 1)[-1]
        if len(api_hash) in [20, 21, 22] and api_hash.isalnum():
            name = name[: -(len(api_hash) + 1)]
        if "-" in name:
            name, version = name.rsplit("-", 1)
            return f"{name}.so", version, True
        else:
            return f"{name}.so", None, True
    return soname, None, False
