"""
This script processes the "Linux Contents" file and parses which files are added by which packages
An example of this file can be found here: http://security.ubuntu.com/ubuntu/dists/focal/Contents-amd64.gz

The result is stored in a sqlite database

The database has one table:
    "package_files"
The table has five main columns:
    file_name               - Just the name of the file
                              ex: "lib/modules/5.4.0-1009-aws/vdso/vdso32.so" -> vdso32.so
    normalized_file_name    - The filename normalized to remove version info
                              ex: "libexample-1.2.3.so" -> "libexample.so"
    file_path               - The entire path for the file
                              ex: "lib/modules/5.4.0-1009-aws/vdso/vdso32.so"
    package_name            - The short package name
                              ex: "admin/multipath-tools" -> multipath-tools
    full_package_name       - The full/long package name
                              ex: admin/multipath-tools

The file_name column is indexed for fast lookups, as the file_name is the primary value that will be searched for
"""
from __future__ import annotations

import argparse
import requests
import sqlite3
import gzip
import lzma

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from datetime import datetime
from io import BytesIO, FileIO, TextIOWrapper
from urllib.parse import urlparse
from tqdm.auto import tqdm

from typing_extensions import Self

from dapper_python.normalize import NormalizedFileName, normalize_file_name


@dataclass
class PackageInfo:
    full_package_name: str
    file_path: PurePosixPath

    @property
    def package_name(self) -> str:
        return self.full_package_name.rsplit('/', maxsplit=1)[-1]

    @property
    def file_name(self) -> str:
        return self.file_path.name

    def __post_init__(self):
        if not isinstance(self.file_path, PurePosixPath):
            self.file_path = PurePosixPath(self.file_path)

    @classmethod
    def from_linux_package_file(cls, line:str) -> Self:
        """Creates a PackageInfo object out of a single line from the linux contents file
        Uses simple parsing to split the line into package_name and file_path and then construct the PackageInfo object

        :param line: A line of text from the linux contents file
        :return: The package info for that line
        """
        file_path, full_package_name = tuple(x.strip() for x in line.rsplit(maxsplit=1))
        return cls(
            full_package_name=full_package_name,
            file_path=PurePosixPath(file_path)
        )


def read_data(uri:str|Path, *, encoding='utf-8') -> TextIOWrapper:
    """Reads a file either from disk or by downloading it from the provided URL
    Will attempt to read the provided file as a text file

    :param uri: Filepath on disk, or URL to download from
    :param encoding: The text encoding to of the file, normally utf-8
    :return: A TextIOWrapper around the file. Can iterate over lines
    """
    if isinstance(uri, Path):
        if not uri.exists():
            raise FileNotFoundError(f"File {uri} does not exist")

        return TextIOWrapper(FileIO(uri, mode='rb'), encoding=encoding)

    elif isinstance(uri, str):
        parsed_url = urlparse(uri)
        if not (parsed_url.scheme and parsed_url.netloc):
            raise ValueError(f"Invalid URL: {uri}")

        with requests.get(uri, stream=True) as web_request:
            if 'content-length' in web_request.headers:
                file_size = int(web_request.headers['content-length'])
            else:
                file_size = None

            content = BytesIO()
            progress_bar = tqdm(
                    total=file_size,
                    desc='Downloading package file', colour='blue',
                    unit='B', unit_divisor=1024, unit_scale=True,
                    position=None, leave=None,
                )
            with progress_bar:
                for chunk in web_request.iter_content(chunk_size=8*1024):
                    content.write(chunk)
                    progress_bar.update(len(chunk))
            content.seek(0)

            #Data is most commonly in a compressed gzip format, but support some others as well
            match web_request.headers.get('Content-Type', None):
                case 'application/x-gzip':
                    with gzip.open(content) as gz_file:
                        return TextIOWrapper(BytesIO(gz_file.read()), encoding=encoding)
                case 'application/x-xz':
                    with lzma.open(content) as lzma_file:
                        return TextIOWrapper(BytesIO(lzma_file.read()), encoding=encoding)
                case _:
                    #Not sure, try to read as raw text file
                    return TextIOWrapper(content)

    else:
        raise TypeError(f"Invalid input: {uri}")


def main():
    parser = argparse.ArgumentParser(
        description="Create Linux DB by parsing the Linux Contents file"
    )
    #Allow to be either a path or a URL
    parser.add_argument(
        '-i','--input',
        required=True,
        type=lambda x: str(x) if urlparse(x).scheme and urlparse(x).netloc else Path(x),
        help='Path or URL to input file',
    )
    parser.add_argument(
        '-o','--output',
        required=False,
        type=Path, default=Path('LinuxPackageDB.db'),
        help='Path of output (database) file to create. Defaults to "LinuxPackageDB.db" in the current working directory',
    )
    parser.add_argument(
        '-v', '--version',
        type=int, required=True,
        help='Version marker for the database to keep track of changes'
    )
    args = parser.parse_args()

    #Currently not set up to be able to handle resuming a previously started database
    #However it's not a high priority as the process only takes a minute or two. Can just delete the old DB and recreate
    #TODO: Potentially allow resuming in the future
    if args.output.exists():
        raise FileExistsError(f"File {args.output} already exists")

    file = read_data(args.input)
    line_count = sum(1 for _ in file)
    file.seek(0)

    with sqlite3.connect(args.output) as db:
        cursor = db.cursor()

        #Would there be any benefit to having a separate package table
        #Which the files table references as a foreign key vs directly saving the package into the files table?
        create_table_cmd = """
            CREATE TABLE package_files(
                id INTEGER PRIMARY KEY,
                file_name TEXT,
                normalized_file_name TEXT,
                file_path TEXT,
                package_name TEXT,
                full_package_name TEXT
            )
        """
        cursor.execute(create_table_cmd)

        insert_cmd = """
            INSERT INTO package_files(file_name, normalized_file_name, file_path, package_name, full_package_name)
            VALUES (?, ?, ?, ?, ?)
        """
        progress_iter = tqdm(
            file,
            total=line_count,
            desc='Processing Data', colour='green',
            unit='Entry',
        )
        for line in progress_iter:
            package = PackageInfo.from_linux_package_file(line)

            #Lower seems like it should work? As far as the OS is concerned ß.json is not the same file as ss.json
            normalized_file = normalize_file_name(package.file_name)
            match normalized_file:
                case str(name):
                    normalized_name = name.lower()
                case NormalizedFileName():
                    normalized_name = normalized_file.name.lower()
                case _:
                    raise TypeError(f"Failed to normalize file: {package.file_name}")

            cursor.execute(
                insert_cmd,
                (
                    package.file_name, normalized_name, str(package.file_path),
                    package.package_name, package.full_package_name,
                )
            )

        #Index the filename colum for fast lookups
        #Currently does not index package name as use case does not require fast lookups on package name and reduces filesize
        index_cmd = """
            CREATE INDEX idx_file_name
            ON package_files(file_name);
        """
        cursor.execute(index_cmd)
        index_cmd = """
            CREATE INDEX idx_normalized_file_name
            ON package_files(normalized_file_name);
        """
        cursor.execute(index_cmd)

        #Metadata information about table
        create_table_cmd = """
            CREATE TABLE dataset_version(
                version INTEGER PRIMARY KEY,
                format TEXT,
                timestamp INTEGER
            )
        """
        cursor.execute(create_table_cmd)
        metadata_add_cmd = """
            INSERT INTO dataset_version(version, format, timestamp)
            VALUES (?, "Linux", ?)
        """
        cursor.execute(metadata_add_cmd, (args.version, int(datetime.now().timestamp())))

if __name__ == "__main__":
    main()