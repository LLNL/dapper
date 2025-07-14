from __future__ import annotations

from pathlib import PurePosixPath, Path
from dataclasses import dataclass

from collections.abc import Generator

from dapper_python.databases.database import Database


@dataclass
class PackageFile:
    file_name: str
    normalized_file_name: str
    file_path: PurePosixPath
    mime_type: str
    magic_string: str # Magic_string is named as such as it is the result string from checking the file with libmagic


class PythonDB(Database):
    """Helper class to read and query the PyPI dataset
    
    Contains some of the most commonly used query functionality
    And also exposes the database to enable specific queries for specific use cases
    """

    def __init__(self, db_path:Path) -> None:
        super().__init__(db_path, mode='ro')

    def list_packages(self) -> Generator[str, None, None]:
        """Lists names of all packages available in the database"""
        cursor = self.cursor()
        query = """
            SELECT DISTINCT package_name
            FROM packages
            ORDER BY package_name
        """

        packages = (
            pkg
            for pkg, *_ in cursor.execute(query).fetchall_chunked()
        )
        yield from packages

    def query_import(self, import_name:str) -> list[str]:
        """Queries packages by their import name
        The package(s) often use the same name for both the package and the import, but this is not always the case.

        E.g:
        Numpy is imported as "numpy"
        Whereas BeautifulSoup[4] is imported as "bs4"
        """
        cursor = self.cursor()
        query = """
            SELECT package_name
            FROM packages JOIN package_imports ON packages.id = package_imports.package_id
            WHERE import_as = ?
        """

        packages = [
            package
            for package, *_ in cursor.execute(query, (import_name,)).fetchall_chunked()
        ]
        return packages


    def query_package_files(self, package_name:str) -> list[PackageFile]:
        """Gets a list of the files contained in the package
        Along with additional details of the file inspected with libmagic

        File lists are extracted from the published tarball for the package on PyPI
        Paths are relative to the root directory of the tarball
        """
        cursor = self.cursor()
        query = """
            SELECT (file_name, normalized_file_name, file_path, mime_type, magic_string)
            FROM packages JOIN package_files ON packages.id = package_files.package_id
            WHERE package_name = ?
        """

        package_files = [
            PackageFile(
                file_name=file_name,
                normalized_file_name=normalized_file_name,
                file_path=PurePosixPath(file_path),
                mime_type=mime_type,
                magic_string=magic_string,
            )
            for file_name, normalized_file_name, file_path, mime_type, magic_string, *_
            in cursor.execute(query, (package_name,)).fetchall_chunked()
        ]
        return package_files
