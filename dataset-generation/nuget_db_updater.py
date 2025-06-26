import argparse
import sqlite3
from functools import wraps
from pathlib import Path
import httpx
import asyncio
from typing import Literal, Optional, List, Dict, Any, Set, Tuple
from typing import TypeVar, Callable, ParamSpec
from typing_extensions import Self
from dataclasses import dataclass, field
from tqdm.auto import tqdm
from datetime import datetime, timedelta
import json
import logging
import sys
import time
from contextlib import asynccontextmanager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("nuget_updater.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("nuget_updater")

# Queries for database operations
GET_LATEST_TIMESTAMP_QUERY = """
    SELECT MAX(last_edited) as latest_timestamp FROM nuget_packages
"""

GET_PACKAGE_QUERY = """
    SELECT id, version, description, last_edited 
    FROM nuget_packages 
    WHERE package_name = ?
"""

UPDATE_PACKAGE_QUERY = """
    UPDATE nuget_packages 
    SET version = ?, description = ?, last_edited = ? 
    WHERE id = ?
"""

INSERT_PACKAGE_QUERY = """
    INSERT INTO nuget_packages(package_name, version, description, last_edited)
    VALUES (?, ?, ?, ?)
"""

DELETE_ARTIFACTS_QUERY = """
    DELETE FROM nuget_package_artifacts
    WHERE package_id = ?
"""

INSERT_ARTIFACT_QUERY = """
    INSERT INTO nuget_package_artifacts(package_id, name, fullname)
    VALUES (?, ?, ?)
"""

GET_UPDATE_STATS_QUERY = """
    SELECT COUNT(*) as total FROM nuget_packages
"""

CREATE_TEMP_TABLE_QUERY = """
    CREATE TEMPORARY TABLE temp_package_names (
        package_name TEXT PRIMARY KEY
    )
"""

INSERT_TEMP_NAME_QUERY = """
    INSERT OR IGNORE INTO temp_package_names(package_name)
    VALUES (?)
"""

GET_MISSING_PACKAGES_QUERY = """
    SELECT t.package_name
    FROM temp_package_names t
    LEFT JOIN nuget_packages p ON t.package_name = p.package_name
    WHERE p.package_name IS NULL
"""

@dataclass
class NugetPackage:
    package_name: str
    version: Optional[str] = None
    description: Optional[str] = None
    last_edited: str = ""
    package_entries: List[Any] = field(default_factory=list)

@dataclass
class PackageDependency:
    name: str
    full_name: str

@dataclass
class UpdateStats:
    start_time: datetime
    end_time: Optional[datetime] = None
    new_packages: int = 0
    updated_packages: int = 0
    unchanged_packages: int = 0
    failed_packages: int = 0
    processed_pages: int = 0
    total_packages_db: int = 0
    
    def print_summary(self):
        """Print a summary of the update statistics"""
        duration = self.end_time - self.start_time if self.end_time else datetime.now() - self.start_time
        
        logger.info("\n================ DATABASE UPDATE SUMMARY ================")
        logger.info(f"Started:             {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Completed:           {self.end_time.strftime('%Y-%m-%d %H:%M:%S') if self.end_time else 'Incomplete'}")
        logger.info(f"Duration:            {duration}")
        logger.info(f"Catalog Pages:       {self.processed_pages}")
        logger.info(f"Database Packages:   {self.total_packages_db}")
        logger.info(f"New Packages:        {self.new_packages}")
        logger.info(f"Updated Packages:    {self.updated_packages}")
        logger.info(f"Unchanged Packages:  {self.unchanged_packages}")
        logger.info(f"Failed Packages:     {self.failed_packages}")
        
        total_processed = self.new_packages + self.updated_packages + self.unchanged_packages + self.failed_packages
        logger.info(f"Total Processed:     {total_processed}")
        
        if duration.total_seconds() > 0:
            rate = total_processed / duration.total_seconds() * 60
            logger.info(f"Processing Rate:     {rate:.1f} packages/minute")
        
        logger.info("========================================================")

T = TypeVar("T")
P = ParamSpec("P")

class NugetDatabaseUpdater:
    class TransactionCursor(sqlite3.Cursor):
        def __enter__(self) -> Self:
            self.connection.execute("BEGIN TRANSACTION")
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb) -> Literal[False]:
            if exc_type is not None:
                self.connection.rollback()
                logger.error(f"Transaction rolled back due to: {exc_type}: {exc_val}")
            else:
                self.connection.commit()
            return False
    
    @staticmethod
    def _requires_connection(func: Callable[P, T]) -> Callable[P, T]:
        @wraps(func)
        def wrapper(self, *args: P.args, **kwargs: P.kwargs) -> T:
            if self._database is None:
                raise sqlite3.ProgrammingError("Cannot operate on a closed database")
            return func(self, *args, **kwargs)
        return wrapper
    
    def __init__(
        self,
        db_path: Path,
        concurrency: int = 200,
        batch_size: int = 100,
        from_date: Optional[str] = None,
        full_refresh: bool = False
    ):
        self.db_path = db_path
        self.concurrency = concurrency
        self.batch_size = batch_size
        self.from_date = from_date
        self.full_refresh = full_refresh
        self._database: Optional[sqlite3.Connection] = None
        
        # API endpoints
        self.catalog_url = "https://api.nuget.org/v3/catalog0/index.json"
        self.semaphore = asyncio.Semaphore(concurrency)
        
        # Stats
        self.stats = UpdateStats(start_time=datetime.now())
        
        # Set of known package names for batch processing
        self.known_packages: Set[str] = set()
        self.new_packages: Set[str] = set()
    
    def __enter__(self) -> Self:
        self._database = sqlite3.connect(self.db_path)
        self._database.row_factory = sqlite3.Row
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> Literal[False]:
        if self._database is not None:
            self._database.close()
            self._database = None
        return False
    
    @_requires_connection
    def get_cursor(self) -> TransactionCursor:
        return self._database.cursor(factory=self.TransactionCursor)
    
    @_requires_connection
    def get_latest_timestamp(self) -> str:
        """Get the latest timestamp from the database"""
        with self.get_cursor() as cursor:
            result = cursor.execute(GET_LATEST_TIMESTAMP_QUERY).fetchone()
            if result and result["latest_timestamp"]:
                return result["latest_timestamp"]
            return "2010-01-01T00:00:00Z"  # Default to old date if no data
    
    @_requires_connection
    def get_database_stats(self) -> int:
        """Get total number of packages in the database"""
        with self.get_cursor() as cursor:
            result = cursor.execute(GET_UPDATE_STATS_QUERY).fetchone()
            return result["total"] if result else 0
    
    @_requires_connection
    def create_temp_package_table(self, package_names: List[str]) -> Set[str]:
        """Create temporary table with package names and return missing packages"""
        missing_packages = set()
        with self.get_cursor() as cursor:
            # Create temporary table
            cursor.execute(CREATE_TEMP_TABLE_QUERY)
            
            # Insert package names
            for name in package_names:
                cursor.execute(INSERT_TEMP_NAME_QUERY, (name,))
            
            # Find missing packages
            for row in cursor.execute(GET_MISSING_PACKAGES_QUERY):
                missing_packages.add(row["package_name"])
        
        return missing_packages
    
    @_requires_connection
    def process_package(self, package: NugetPackage) -> Tuple[bool, bool]:
        """
        Process a package - insert new or update existing
        Returns: (is_new, is_updated)
        """
        try:
            with self.get_cursor() as cursor:
                # Check if package exists
                cursor.execute(GET_PACKAGE_QUERY, (package.package_name,))
                existing = cursor.fetchone()
                
                if existing:
                    # Package exists - check if it needs updating
                    existing_edited = existing["last_edited"]
                    
                    # Compare timestamps to see if newer
                    if existing_edited and package.last_edited:
                        package_date = datetime.fromisoformat(package.last_edited.replace("Z", "+00:00"))
                        existing_date = datetime.fromisoformat(existing_edited.replace("Z", "+00:00"))
                        
                        if package_date <= existing_date:
                            # No update needed
                            return False, False
                    
                    # Update the package
                    cursor.execute(
                        UPDATE_PACKAGE_QUERY,
                        (package.version, package.description, package.last_edited, existing["id"])
                    )
                    
                    # Delete old artifacts and insert new ones
                    if package.package_entries:
                        cursor.execute(DELETE_ARTIFACTS_QUERY, (existing["id"],))
                        
                        for entry in package.package_entries:
                            cursor.execute(
                                INSERT_ARTIFACT_QUERY,
                                (existing["id"], entry.name, entry.full_name)
                            )
                    
                    return False, True
                else:
                    # Insert new package
                    cursor.execute(
                        INSERT_PACKAGE_QUERY,
                        (package.package_name, package.version, package.description, package.last_edited)
                    )
                    package_id = cursor.lastrowid
                    
                    # Insert artifacts
                    for entry in package.package_entries:
                        cursor.execute(
                            INSERT_ARTIFACT_QUERY,
                            (package_id, entry.name, entry.full_name)
                        )
                    
                    return True, False
        
        except Exception as e:
            logger.error(f"Error processing package {package.package_name}: {str(e)}")
            return False, False
    
    @asynccontextmanager
    async def _http_client(self, timeout: int = 30):
        """Context manager for HTTP client optimized for high throughput"""
        # Configure for maximum parallel connections
        limits = httpx.Limits(
            max_keepalive_connections=None,
            max_connections=None,
            keepalive_expiry=60.0
        )
        transport = httpx.AsyncHTTPTransport(
            limits=limits,
            retries=1
        )
        async with httpx.AsyncClient(
            timeout=timeout,
            transport=transport,
            headers={
                "User-Agent": "NuGet-Updater/1.0",
                "Accept": "application/json",
            },
            follow_redirects=True
        ) as client:
            yield client
    
    async def fetch_url(self, url: str, retries: int = 3) -> Optional[Dict[str, Any]]:
        """Fetch data from URL with retry logic"""
        async with self.semaphore:
            attempt = 0
            while attempt < retries:
                try:
                    async with self._http_client() as client:
                        response = await client.get(url)
                        
                        if response.status_code == 404:
                            logger.warning(f"Resource not found at {url}")
                            return None
                            
                        response.raise_for_status()
                        
                        # Try to parse JSON
                        try:
                            return response.json()
                        except json.JSONDecodeError:
                            logger.error(f"Invalid JSON response from {url}")
                            return None
                            
                except httpx.HTTPStatusError as e:
                    # Handle server errors
                    if e.response.status_code >= 500:
                        logger.warning(f"Server error {e.response.status_code} on {url}, retrying...")
                        await asyncio.sleep(1)
                    else:
                        logger.error(f"HTTP error {e.response.status_code} on {url}")
                        await asyncio.sleep(1)
                        
                except (httpx.RequestError, httpx.TimeoutException) as e:
                    logger.warning(f"Request error on {url}: {type(e).__name__}: {e}")
                
                attempt += 1
                if attempt < retries:
                    backoff_delay = 1 * (1.5 ** (attempt - 1))
                    logger.info(f"Retrying {url} in {backoff_delay:.2f} seconds (attempt {attempt+1}/{retries})...")
                    await asyncio.sleep(backoff_delay)
            
            logger.error(f"Failed to fetch {url} after {retries} attempts.")
            return None
    
    async def process_package_details(self, package_url: str) -> Optional[NugetPackage]:
        """Process package details from catalog leaf"""
        try:
            package_detail = await self.fetch_url(package_url)
            if not package_detail:
                return None
                
            package_name = package_detail.get("id")
            if not package_name:
                return None
                
            package_name_lc = package_name.lower()
            
            # Create package object
            package = NugetPackage(
                package_name=package_name_lc,
                version=package_detail.get("version"),
                description=package_detail.get("description"),
                last_edited=package_detail.get("lastEdited") or package_detail.get("commitTimeStamp")
            )
            
            # Process package entries (DLLs)
            if package_detail.get("packageEntries") is not None:
                entries = []
                for entry in package_detail.get("packageEntries"):
                    if entry["name"].endswith(".dll") and entry["fullName"].endswith(".dll") and entry["fullName"].startswith("lib/"):
                        entries.append(PackageDependency(
                            name=entry["name"],
                            full_name=entry["fullName"].removeprefix("lib/")
                        ))
                package.package_entries = entries
                
            return package
        except Exception as e:
            logger.error(f"Error processing package at {package_url}: {str(e)}")
            return None
    
    async def process_packages_batch(self, packages: List[Dict[str, Any]]) -> None:
        """Process a batch of packages concurrently"""
        # Create batch statistics
        batch_start = time.time()
        batch_new = 0
        batch_updated = 0
        batch_unchanged = 0
        batch_failed = 0
        
        # First, extract all package names from the batch for efficient filtering
        package_names = [pkg.get("id", "").lower() for pkg in packages if pkg.get("id")]
        
        # For incremental updates, we need to check what's new or needs updating
        process_all = self.full_refresh
        packages_to_process = []
        
        if not process_all and package_names:
            # Find which packages are new (not in DB)
            missing_packages = self.create_temp_package_table(package_names)
            
            # Process only new packages and those that might need updates
            for pkg in packages:
                package_name = pkg.get("id", "").lower()
                if not package_name:
                    continue
                    
                if package_name in missing_packages:
                    # This is a new package
                    packages_to_process.append(pkg)
                else:
                    # Existing package - check commitTimeStamp to see if it needs updating
                    commit_time = pkg.get("commitTimeStamp") or pkg.get("lastEdited")
                    if commit_time and commit_time > self.from_date:
                        packages_to_process.append(pkg)
                    else:
                        # Skip this package, it's unchanged
                        batch_unchanged += 1
                        self.stats.unchanged_packages += 1
        else:
            # Process all packages in batch (for full refresh)
            packages_to_process = [pkg for pkg in packages if pkg.get("id")]
        
        # Now process only the packages that need it
        tasks = []
        for pkg in packages_to_process:
            package_url = pkg.get("@id")
            if package_url:
                tasks.append(self.process_package_details(package_url))
        
        if tasks:
            # Process package details concurrently
            package_results = await asyncio.gather(*tasks)
            
            # Update database with results
            for package in package_results:
                if package:
                    # Save to database
                    is_new, is_updated = self.process_package(package)
                    
                    if is_new:
                        batch_new += 1
                        self.stats.new_packages += 1
                    elif is_updated:
                        batch_updated += 1
                        self.stats.updated_packages += 1
                    else:
                        batch_unchanged += 1
                        self.stats.unchanged_packages += 1
                else:
                    batch_failed += 1
                    self.stats.failed_packages += 1
        
        # Log batch statistics
        batch_time = time.time() - batch_start
        if batch_time > 0:
            packages_per_second = len(packages_to_process) / batch_time
        else:
            packages_per_second = 0
            
        logger.info(
            f"Batch processed in {batch_time:.2f}s "
            f"({packages_per_second:.2f} pkgs/s): "
            f"New={batch_new}, Updated={batch_updated}, "
            f"Unchanged={batch_unchanged}, Failed={batch_failed}"
        )
    
    async def process_catalog_page(self, page_url: str) -> None:
        """Process a catalog page"""
        page_result = await self.fetch_url(page_url)
        if not page_result or "items" not in page_result:
            logger.error(f"Failed to get page data from {page_url}")
            return
            
        package_list = page_result["items"]
        logger.info(f"Found {len(package_list)} packages on page")
        
        # Process in batches
        for i in range(0, len(package_list), self.batch_size):
            batch = package_list[i:i+self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1}/{(len(package_list) + self.batch_size - 1)//self.batch_size} ({len(batch)} packages)")
            await self.process_packages_batch(batch)
    
    async def update_database(self) -> None:
        """Main method to update the database"""
        logger.info("Starting NuGet package database update")
        
        # Get database stats before update
        self.stats.total_packages_db = self.get_database_stats()
        
        # If not full refresh, get the latest timestamp from the database
        if not self.full_refresh and not self.from_date:
            self.from_date = self.get_latest_timestamp()
            logger.info(f"Updating packages newer than {self.from_date}")
        
        # Get catalog index
        res = await self.fetch_url(self.catalog_url)
        if not res:
            logger.error("Failed to fetch catalog index")
            return
            
        # Get all pages from the catalog index
        pages = res.get("items", [])
        if not pages:
            logger.error("No catalog pages found")
            return
        
        # Filter pages by timestamp if needed
        if not self.full_refresh and self.from_date:
            filtered_pages = []
            for page in pages:
                page_time = page.get("commitTimeStamp", "")
                if page_time >= self.from_date:
                    filtered_pages.append(page)
            
            logger.info(f"Processing {len(filtered_pages)}/{len(pages)} catalog pages (filtered by date)")
            pages = filtered_pages
        else:
            logger.info(f"Processing all {len(pages)} catalog pages (full refresh)")
        
        # Process each catalog page
        page_bar = tqdm(total=len(pages), desc="Processing pages")
        
        for page in pages:
            try:
                page_url = page.get("@id")
                if page_url:
                    await self.process_catalog_page(page_url)
                    self.stats.processed_pages += 1
            except Exception as e:
                logger.error(f"Error processing page: {str(e)}")
            
            page_bar.update(1)
        
        # Record completion time
        self.stats.end_time = datetime.now()
        
        # Print summary
        self.stats.print_summary()

async def main():
    """Entry point for the script"""
    parser = argparse.ArgumentParser(
        description="Update NuGet packages database with new and changed packages"
    )
    parser.add_argument(
        '-o', '--db-path',
        required=False,
        type=Path, default=Path('NugetPackageDB.db'),
        help='Path to the SQLite database file'
    )
    parser.add_argument(
        '--concurrency',
        type=int, default=200,
        help='Maximum number of concurrent requests (default: 200)'
    )
    parser.add_argument(
        '--batch-size',
        type=int, default=100,
        help='Number of packages to process in each batch (default: 100)'
    )
    parser.add_argument(
        '--from-date',
        type=str, default=None,
        help='Process only packages updated after this date (ISO format, e.g., 2023-01-01T00:00:00Z)'
    )
    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help='Perform a full refresh of all packages, ignoring last update timestamp'
    )
    
    args = parser.parse_args()
    
    if not args.db_path.exists():
        logger.error(f"Database file does not exist: {args.db_path}")
        return
    
    # Create and run updater
    try:
        with NugetDatabaseUpdater(
            db_path=args.db_path,
            concurrency=args.concurrency,
            batch_size=args.batch_size,
            from_date=args.from_date,
            full_refresh=args.full_refresh
        ) as updater:
            await updater.update_database()
    except KeyboardInterrupt:
        logger.info("Update process stopped by user")
    except Exception as e:
        logger.critical(f"Unhandled exception: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())