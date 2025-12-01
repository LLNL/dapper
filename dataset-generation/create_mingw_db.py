from __future__ import annotations

import re
import argparse
import json
import tarfile
import warnings
import logging
import tempfile
import more_itertools
import urllib3
import requests
import zstandard as zstd  # Newer python has this built in, but only in 3.14+
import magic
import angr
import pydemumble

from io import BytesIO
from pathlib import Path, PurePosixPath
from datetime import datetime
from dataclasses import dataclass
from sqlmodel import select, delete
from contextlib import contextmanager, suppress, ExitStack
from tarfile import TarFile
from bs4 import BeautifulSoup, Tag
from tqdm.auto import tqdm
from methodtools import lru_cache

try:
    from enum import StrEnum
except ImportError:
    from backports.strenum import StrEnum

from typing import Any
from typing_extensions import Self

from dapper_python.databases_v2.database import Metadata
from dapper_python.databases_v2.mingw_db import MinGWDatabase
from dapper_python.databases_v2.mingw_db import Package, PackageFile, SourceFile
from dapper_python.databases_v2.mingw_db import FunctionSymbol, PreprocessDefine, StringLiteral
from dapper_python.dataset_generation.parsing.cpp import CPPTreeParser
from dapper_python.dataset_generation.utils.archive import SafeTarFile, SafeZipFile

# Note: Using verify=False for requests is not ideal, but otherwise breaks due to corporate network certificates
PACKAGE_INDEX_URL = "https://packages.msys2.org/packages"


class Arch(StrEnum):
    """The architecture options available on MySYS2"""
    UCRT_64 = "ucrt64"
    CLANG_64 = "clang64"
    CLANG_ARM_64 = "clangarm64"
    MYSYS = "mysys"
    MINGW_64 = "mingw64"
    MINGW_32 = "mingw32"


@dataclass
class MySysPackage:
    package_name: str
    package_version: str
    package_url: str
    description: str | None = None

    @property
    def source_url(self) -> str:
        source_url, _ = self._fetch_artifact_urls()
        return source_url

    def get_source(self) -> BytesIO:
        """Gets the source tarball for the package

        Returned bytes should be opened as a tarfile
        """
        with suppress_warnings():
            with requests.get(self.source_url, verify=False, stream=True) as response:
                with zstd.ZstdDecompressor().stream_reader(response.content) as reader:
                    decompressed_tarball = BytesIO(reader.read())
                    return decompressed_tarball

    @property
    def contents_url(self) -> str:
        _, binary_url = self._fetch_artifact_urls()
        return binary_url

    def get_contents(self) -> BytesIO:
        """Gets the package contents tarball for the pacakge

        Returned bytes should be opened as a tarfile
        """
        with suppress_warnings():
            with requests.get(self.contents_url, verify=False, stream=True) as response:
                with zstd.ZstdDecompressor().stream_reader(response.content) as reader:
                    decompressed_tarball = BytesIO(reader.read())
                    return decompressed_tarball

    @lru_cache(maxsize=1)
    def _fetch_artifact_urls(self) -> tuple[str, str]:
        with suppress_warnings(), requests.get(self.package_url, verify=False) as response:
            soup = BeautifulSoup(response.text, 'html.parser')

            def find_entry(label: str) -> Tag | None:
                """Finds and returns the <a> tag following the <dt> with matching label (case-insensitive)."""
                dt = soup.find("dt", string=re.compile(rf"^{label}\s*:?$", re.I))
                if not dt:
                    return None
                dd = dt.find_next_sibling("dd")
                if not dd:
                    return None
                return dd.find("a", href=True)

            source_url = find_entry("Source-Only Tarball").get("href")
            binary_url = find_entry("File").get("href")

            return source_url, binary_url


class PackageAnalyzer:
    def __init__(self, package: MySysPackage) -> None:
        self._mysys_package = package

        self._exit_stack = ExitStack()
        self._temp_dir: Path | None = None
        self._source_dir: Path | None = None
        self._package_dir: Path | None = None

    def __enter__(self) -> Self:
        self._temp_dir = Path(self._exit_stack.enter_context(tempfile.TemporaryDirectory()))
        self._source_dir = self._temp_dir.joinpath("source")
        self._source_dir.mkdir(exist_ok=True)
        self._package_dir = self._temp_dir.joinpath("package")
        self._package_dir.mkdir(exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._temp_dir = None
        self._source_dir = None
        self._package_dir = None
        return self._exit_stack.__exit__(exc_type, exc_val, exc_tb)

    def analyze_package(self) -> tuple[Package | None, dict[str, Any]]:
        """Analyzes the package and returns the parsed data"""
        if self._temp_dir is None:
            raise RuntimeError("Must be used within context manager")

        mingw_package = Package(
            package_name=self._mysys_package.package_name,
            package_version=self._mysys_package.package_version,
        )

        try:
            analyzed_package_sources = self._analyze_package_source()
            mingw_package.source_files = analyzed_package_sources
        except (zstd.ZstdError, tarfile.ReadError):
            return None, {}

        with suppress(zstd.ZstdError, tarfile.ReadError):
            analyzed_package_files, symbols = self._analyze_package_contents()
            mingw_package.package_files = analyzed_package_files

            if symbols:
                for source_file in mingw_package.source_files:
                    for function_symbol in source_file.functions:
                        # Demangled symbols to not contain return type and many compiled types do not match
                        # Such as std::string -> std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char>>
                        function_symbol.in_binary = function_symbol.qualified_symbol_name in symbols

        return mingw_package, self._as_json_dict(mingw_package)

    def _analyze_package_source(self) -> list[SourceFile]:
        if self._source_dir is None:
            raise RuntimeError("Must be used within context manager")

        with suppress_warnings(), SafeTarFile.open(fileobj=self._mysys_package.get_source(), mode="r:*") as outer_tar:
            file_list = outer_tar.getmembers()

            sub_files = [x for x in file_list if ".tar" in x.name]
            for sub_tar in sub_files:
                data = BytesIO(outer_tar.extractfile(sub_tar).read())
                with SafeTarFile.open(fileobj=data, mode="r:*") as inner_tar:
                    inner_tar.safe_extractall(self._source_dir)

            sub_files = [x for x in file_list if x.name.endswith(".zip")]
            for sub_zip in sub_files:
                data = BytesIO(outer_tar.extractfile(sub_zip).read())
                with SafeZipFile(data) as inner_zip:
                    inner_zip.safe_extractall(self._source_dir)

        dirs = [x for x in self._source_dir.iterdir() if x.is_dir()]
        source_root = dirs[0] if len(dirs) == 1 else self._source_dir

        # Process all C/C++ files
        files = [
            x for x in source_root.rglob("*")
            if x.suffix.lower() in (".c", ".cpp", ".h", ".hpp", ".tpp")
               and x.is_file()
        ]

        source_files = []
        file_progress_iter = tqdm(
            files,
            desc="Parsing Files", colour="cyan",
            unit="File",
            position=None, leave=None,
            disable=not files,
        )
        for file in file_progress_iter:
            source_file = SourceFile(
                file_path=PurePosixPath(file.relative_to(source_root)),
            )

            tree = CPPTreeParser.from_source(file.read_bytes())
            source_file.functions = [
                FunctionSymbol(
                    return_type=x.return_type,
                    symbol_name=x.symbol_name,
                    qualified_symbol_name=x.qualified_symbol_name,
                    params=x.params,
                    full_signature=x.full_signature,
                    source_text=x.source_text,
                )
                for x in tree.parse_functions()
            ]
            # Monkey-patch in pre-process defines and string literals
            # Which are not included in the database but should be passed up the chain, associated with each SourceFile
            source_file.preproc_defines = [
                PreprocessDefine(name=x.name, value=x.value)
                for x in tree.parse_preproc_defs()
            ]
            source_file.string_literals = [
                StringLiteral(value=x.value)
                for x in tree.parse_string_literals()
            ]

            source_files.append(source_file)

        return source_files

    def _analyze_package_contents(self) -> tuple[list[PackageFile], set[str]]:
        if self._package_dir is None:
            raise RuntimeError("Must be used within context manager")

        with suppress_warnings(), TarFile.open(fileobj=self._mysys_package.get_contents(), mode="r:*") as tar_file:
            tar_file.extractall(self._package_dir)

        dirs = [x for x in self._package_dir.iterdir() if x.is_dir()]
        package_root = next(
            (x for x in dirs if any(y == x.name for y in Arch)),
            self._package_dir,
        )
        files = [x for x in package_root.rglob("*") if x.is_file()]

        package_files = []
        symbols = set()
        file_progress_iter = tqdm(
            files,
            desc="Parsing Files", colour="cyan",
            unit="File",
            position=None, leave=None,
            disable=not files,
        )
        for file in file_progress_iter:
            try:
                mime_type = magic.from_file(str(file.absolute()), mime=True)
                magic_string = magic.from_file(str(file.absolute()))
            except magic.MagicException:
                mime_type = None
                magic_string = None

            # Scan the file for any symbols
            with disable_logging(), suppress(Exception):
                angr_proj = angr.Project(file, auto_load_libs=False)
                demangled_symbols = (
                    pydemumble.demangle(x.name).strip()
                    for x in angr_proj.loader.main_object.symbols
                )
                demangled_functions = (x for x in demangled_symbols if not x.startswith(self._NON_FUNCTION_PREFIXES))
                # Grab just the qualified name (without parameters) to compare as many compiled types are different from their source-code counterpart
                symbols.update(x.split("(")[0] for x in demangled_functions)

            package_file = PackageFile(
                file_path=PurePosixPath(file.relative_to(package_root)),
                mime_type=mime_type,
                magic_string=magic_string,
            )
            package_files.append(package_file)

        return package_files, symbols

    # noinspection Pydantic
    def _as_json_dict(self, package: Package) -> dict[str, Any]:
        # preproc_defines and string_literals attributes are monkey-patched onto each file
        # Since they are [currently] not part of the actual database schema
        return {
            "package_info": {
                "name": self._mysys_package.package_name,
                "version": self._mysys_package.package_version,
                "description": self._mysys_package.description,
                "package_url": self._mysys_package.package_url,
                "source_url": self._mysys_package.source_url,
                "contents_url": self._mysys_package.contents_url,
            },
            "contents": {
                str(_source_file.file_path): {
                    "functions": [x.model_dump(exclude={"id", "file_id"}) for x in _source_file.functions],
                    "preproc_defines": [x.model_dump(exclude={"id", "file_id"}) for x in _source_file.preproc_defines],
                    "string_literals": [x.model_dump(exclude={"id", "file_id"}) for x in _source_file.string_literals],
                }
                for _source_file in package.source_files
            },
        }

    _NON_FUNCTION_PREFIXES = (
        "sub_",  # Special case since we don't want anonymous functions/code sections angr finds without a name
        "vtable for",
        "typeinfo for",
        "typeinfo name for",
        "covariant return thunk for",
        "covariant return thunk to",
        "construction vtable for",
        "virtual thunk to",
        "non-virtual thunk to",
        "guard variable for",
        "transaction clone for",
        "VTT for",
        "TLS wrapper function for",
    )


@contextmanager
def suppress_warnings():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=urllib3.exceptions.InsecureRequestWarning)
        warnings.simplefilter("ignore", category=RuntimeWarning)
        yield


@contextmanager
def disable_logging(highest_level=logging.CRITICAL):
    previous_level = logging.root.manager.disable
    try:
        logging.disable(highest_level)
        yield
    finally:
        logging.disable(previous_level)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--dir",
        type=Path, default=Path.cwd(),
        help="Directory to save database + generated files to",
    )
    parser.add_argument(
        "-o", "--output",
        type=str, default="MinGWDB.db",
        help="Name of the output database file",
    )
    parser.add_argument(
        "-v", "--version",
        type=int, required=True,
        help="Version of the database",
    )
    args = parser.parse_args()

    if not args.dir.exists() or not args.dir.is_dir():
        raise FileNotFoundError(f"No such directory: {args.dir}")

    params = {"repo": Arch.MINGW_64}
    with suppress_warnings(), requests.get(PACKAGE_INDEX_URL, params=params, verify=False) as response:
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Find table in the page.
        # Unfortunate lack of clearly distinct identifiers to search for (e.g id="package_list")
        # So we need to follow the chain of tags and hope it doesn't change
        table = soup.find("table", class_="table-hover")
        tbody = table.find("tbody")

        package_list: dict[str, MySysPackage] = {}
        for row in tbody.find_all("tr"):
            cols = row.find_all("td")
            if len(cols) != 3:
                continue

            link_tag = cols[0].find("a")
            package_link = link_tag['href']
            package_name = link_tag.text.strip()
            version = cols[1].text.strip()
            description = cols[2].text.strip()

            package_list[package_name] = MySysPackage(
                package_name=package_name,
                package_version=version,
                description=description,
                package_url=package_link,
            )

    db_path = args.dir.joinpath(args.output)
    mingw_db = MinGWDatabase.create_database(db_path, exist_ok=True)
    with mingw_db.session() as session:
        # Remove any outdated packages
        with session.begin():
            to_update = more_itertools.peekable((
                package
                for package in session.exec(select(Package))
                if package.package_name in package_list
                   and package_list.get(package.package_name).package_version != package.package_version
            ))
            progress_iter = tqdm(
                to_update,
                desc="Removing outdated packages",
                unit="Package",
                position=None, leave=False,
                disable=not to_update,
            )
            session.bulk_delete(progress_iter)

            # noinspection PyTypeChecker, Pydantic
            saved_packages: set[str] = set(session.exec(select(Package.package_name)))
            to_update = [
                package_list[package_name]
                for package_name in set(package_list.keys()) - saved_packages
            ]

        json_dir: Path = args.dir.joinpath("json_dump")
        json_dir.mkdir(exist_ok=True)

        # Get new packages and add to the database
        progress_iter = tqdm(
            to_update,
            desc="Scraping Packages", colour="blue",
            unit="Package",
            position=None, leave=None,
            disable=not to_update,
        )
        for package in progress_iter:
            with PackageAnalyzer(package) as analyzer:
                mingw_package, json_dump = analyzer.analyze_package()
                if not mingw_package:
                    continue

            dump_path = json_dir.joinpath(f"{mingw_package.package_name}.json")
            with session.begin():
                session.add(mingw_package)
                with open(dump_path, "w", encoding="utf-8") as f:
                    json.dump(json_dump, f, indent="\t")

        # Reset the metadata if it already exists and set new version
        with session.begin():
            session.exec(delete(Metadata))
            session.add(Metadata(
                version=args.version,
                format="PyPI",
                timestamp=int(datetime.now().timestamp()),
            ))


if __name__ == "__main__":
    main()
