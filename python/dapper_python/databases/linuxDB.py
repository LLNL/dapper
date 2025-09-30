from __future__ import annotations

from pathlib import PurePosixPath, Path
from dataclasses import dataclass

from collections.abc import Generator

from dapper_python.normalize import normalize_file_name
from dapper_python.databases.database import Database


@dataclass
class PackageFile:
    file_name: str
    normalized_name: str
    file_path: PurePosixPath
    package_name: str
    full_package_name: str


class LinuxDB(Database):
    """Helper class to read and query the Linux dataset

    Contains some of the most commonly used query functionality
    And also exposes the database to enable specific queries for specific use cases
    """

    def __init__(self, db_path:Path) -> None:
        super().__init__(db_path, mode='ro')

    def list_packages(self) -> Generator[str]:
        """Lists names of all packages available in the database"""
        cursor = self.cursor()
        query = """
            SELECT DISTINCT package_name
            FROM package_files
            ORDER BY package_name
        """

        packages = (
            pkg
            for pkg, *_ in cursor.execute(query).fetchall_chunked()
        )
        yield from packages

    def query_filename(self, file_name:str, *, normalize=True) -> list[PackageFile]:
        if normalize:
            file_name = str(normalize_file_name(file_name))

        cursor = self.cursor()
        query = f"""
            SELECT file_name, normalized_file_name, file_path, package_name, full_package_name
            FROM package_files
            WHERE {'normalized_file_name' if normalize else 'file_name'} = ?
        """

        package_files = [
            PackageFile(
                file_name=file_name,
                normalized_name=normalized_filename,
                file_path=PurePosixPath(file_path),
                package_name=package_name,
                full_package_name=full_package_name
            )
            for file_name, normalized_filename, file_path, package_name, full_package_name, *_
            in cursor.execute(query, (file_name,)).fetchall_chunked()
        ]
        return package_files
