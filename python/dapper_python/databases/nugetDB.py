from __future__ import annotations

from pathlib import PurePosixPath, Path
from dataclasses import dataclass

from collections.abc import Generator

from dapper_python.databases.database import Database


@dataclass
class NuGetPackageFile:
    """Represents a file from a NuGet package"""
    file_name: str
    file_path: str
    package_name: str
    package_version: str


class NuGetDB(Database):
    """Helper class to read and query the NuGet dataset
    
    Contains some of the most commonly used query functionality
    And also exposes the database to enable specific queries for specific use cases
    
    Database Schema:
    - nuget_packages: id, package_name, version, description, last_edited
    - nuget_package_artifacts: id, package_id, name, fullname
    """

    def __init__(self, db_path: Path) -> None:
        super().__init__(db_path, mode='ro')

    def list_packages(self) -> Generator[str, None, None]:
        """Lists names of all packages available in the database"""
        cursor = self.cursor()
        query = """
            SELECT DISTINCT package_name
            FROM nuget_packages
            ORDER BY package_name
        """

        packages = (
            pkg
            for pkg, *_ in cursor.execute(query).fetchall_chunked()
        )
        yield from packages

    def query_filename(self, file_name: str) -> list[NuGetPackageFile]:
        """Query packages by filename (e.g., "System.Core.dll")
        
        Args:
            file_name: The filename to search for (case-insensitive)
            
        Returns:
            List of NuGetPackageFile objects matching the filename
        """
        cursor = self.cursor()
        
        # Query by joining nuget_packages and nuget_package_artifacts
        # Use COLLATE NOCASE for case-insensitive matching
        query = """
            SELECT 
                npa.name,
                npa.fullname,
                np.package_name,
                np.version
            FROM nuget_package_artifacts npa
            JOIN nuget_packages np ON npa.package_id = np.id
            WHERE npa.name = ? COLLATE NOCASE
        """

        package_files = [
            NuGetPackageFile(
                file_name=name,
                file_path=fullname,
                package_name=package_name,
                package_version=version or ""
            )
            for name, fullname, package_name, version, *_
            in cursor.execute(query, (file_name,)).fetchall_chunked()
        ]
        return package_files

    