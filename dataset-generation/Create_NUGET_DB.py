import argparse
import sqlite3
from functools import wraps
from pathlib import Path
import httpx
import asyncio
from typing import Literal, Optional, List, Dict, Any, Union
from typing import TypeVar, Callable, ParamSpec, Generator, Iterable
from typing_extensions import Self
from itertools import repeat
from dataclasses import dataclass, field
from tqdm.auto import tqdm
from datetime import datetime, timedelta
import re
import aiohttp
import zipfile
import os
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
        logging.FileHandler("nuget_scraper.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("nuget_scraper")

# Database schema
CREATE_TABLE_PACKAGES_CMD = """
    CREATE TABLE IF NOT EXISTS nuget_packages(
        id INTEGER PRIMARY KEY,
        package_name TEXT UNIQUE NOT NULL,
        version TEXT,
        description TEXT,
        last_edited TEXT
    )
"""

CREATE_TABLE_PACKAGE_ARTIFACTS_CMD = """
    CREATE TABLE IF NOT EXISTS nuget_package_artifacts (
        id INTEGER PRIMARY KEY,
        package_id INTEGER,
        name TEXT NOT NULL, 
        fullname TEXT NOT NULL,
        FOREIGN KEY (package_id) REFERENCES nuget_packages(id) ON DELETE CASCADE
    )
"""

CREATE_TABLE_JOIN_CMD = """
    CREATE VIEW IF NOT EXISTS join_table AS
    SELECT 
        npa.id as artifact_id,
        np.package_name,
        npa.name,
        npa.fullname
    FROM nuget_package_artifacts npa
    JOIN nuget_packages np ON npa.package_id = np.id
"""

CREATE_TABLE_LAST_PROCESSED_CMD = """
    CREATE TABLE IF NOT EXISTS last_processed_page (
        id INTEGER PRIMARY KEY,
        last_page INTEGER,
        last_update TEXT
    )
"""

CREATE_TABLE_FAILURES_CMD = """
    CREATE TABLE IF NOT EXISTS failed_packages (
        id INTEGER PRIMARY KEY,
        package_name TEXT NOT NULL,
        failure_reason TEXT,
        failure_time TEXT,
        retry_count INTEGER DEFAULT 0
    )
"""

CREATE_TABLE_STATS_CMD = """
    CREATE TABLE IF NOT EXISTS scraper_stats (
        id INTEGER PRIMARY KEY,
        start_time TEXT,
        end_time TEXT,
        total_packages INTEGER DEFAULT 0,
        successful_packages INTEGER DEFAULT 0,
        failed_packages INTEGER DEFAULT 0,
        updated_packages INTEGER DEFAULT 0
    )
"""

# Queries
LAST_PROCESSED_QUERY = """
    SELECT last_page, last_update FROM last_processed_page
"""

UPDATE_PROCESSED_QUERY = """
    UPDATE last_processed_page
    SET last_page = ?, last_update = ?
"""

INSERT_PACKAGE_CMD = """
    INSERT INTO nuget_packages(package_name, version, description, last_edited)
    VALUES (?,?,?,?)
    ON CONFLICT(package_name) DO UPDATE SET
        version = excluded.version,
        description = excluded.description,
        last_edited = excluded.last_edited
    WHERE datetime(excluded.last_edited) > datetime(last_edited);
"""

INSERT_PACKAGE_ARTIFACT_CMD = """
    INSERT INTO nuget_package_artifacts(package_id, name, fullname)
    VALUES (?,?,?)
"""

INSERT_INITIAL_PROCESSED_PAGE_CMD = """
    INSERT INTO last_processed_page(last_page, last_update)
    SELECT 0, datetime('now')
    WHERE NOT EXISTS (SELECT 1 FROM last_processed_page)
"""

INSERT_FAILURE_CMD = """
    INSERT INTO failed_packages(package_name, failure_reason, failure_time, retry_count)
    VALUES (?,?,?,?)
    ON CONFLICT(package_name) DO UPDATE SET
    failure_reason = excluded.failure_reason,
    failure_time = excluded.failure_time,
    retry_count = retry_count + 1
"""

GET_PACKAGE_BY_NAME_CMD = """
    SELECT * FROM nuget_packages WHERE package_name = ?
"""

GET_FAILED_PACKAGES_CMD = """
    SELECT package_name FROM failed_packages 
    WHERE retry_count < ? 
    ORDER BY failure_time ASC
"""

START_SCRAPER_STATS_CMD = """
    INSERT INTO scraper_stats(start_time, end_time)
    VALUES (datetime('now'), NULL)
"""

END_SCRAPER_STATS_CMD = """
    UPDATE scraper_stats 
    SET end_time = datetime('now'),
        total_packages = ?,
        successful_packages = ?,
        failed_packages = ?,
        updated_packages = ?
    WHERE id = ?
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

T = TypeVar("T")
P = ParamSpec("P")

class AdaptiveConcurrencyManager:
    """Manages high concurrency for maximum throughput since there's no rate limiting"""
    def __init__(
        self, 
        initial_concurrency: int = 300,
        min_concurrency: int = 100,
        max_concurrency: int = 1000,
        increase_factor: float = 1.2,
        decrease_factor: float = 0.8,
        success_threshold: int = 10,
        fail_threshold: int = 5
    ):
        self.current_concurrency = initial_concurrency
        self.min_concurrency = min_concurrency
        self.max_concurrency = max_concurrency
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        
        self.success_count = 0
        self.fail_count = 0
        
        self.success_threshold = success_threshold
        self.fail_threshold = fail_threshold
        
        self.semaphore = asyncio.Semaphore(self.current_concurrency)
        self.lock = asyncio.Lock()
        
        logger.info(f"High concurrency initialized with concurrency={initial_concurrency}")
    
    async def acquire(self):
        """Acquire the semaphore"""
        await self.semaphore.acquire()
    
    def release(self):
        """Release the semaphore"""
        self.semaphore.release()
    
    @asynccontextmanager
    async def managed_semaphore(self):
        """Context manager for using the semaphore"""
        await self.acquire()
        try:
            yield
        finally:
            self.release()
    
    async def report_success(self):
        """Report a successful request - more aggressive scaling since no rate limits"""
        async with self.lock:
            self.success_count += 1
            self.fail_count = 0  # Reset fail count on success
            
            # If we've had a streak of successes, increase concurrency more aggressively
            if self.success_count >= self.success_threshold:
                old_concurrency = self.current_concurrency
                self.current_concurrency = min(
                    self.max_concurrency,
                    int(self.current_concurrency * self.increase_factor)
                )
                
                if old_concurrency != self.current_concurrency:
                    logger.info(f"Increasing concurrency: {old_concurrency} -> {self.current_concurrency}")
                    
                    # Add more permits to the semaphore
                    for _ in range(self.current_concurrency - old_concurrency):
                        self.semaphore.release()
                    
                self.success_count = 0  # Reset counter
    
    async def report_failure(self, status_code: Optional[int] = None):
        """Report a failed request - still manage concurrency to avoid overwhelming resources"""
        async with self.lock:
            self.fail_count += 1
            self.success_count = 0  # Reset success count on failure
            
            # If we've had several failures, decrease concurrency slightly
            if self.fail_count >= self.fail_threshold:
                old_concurrency = self.current_concurrency
                self.current_concurrency = max(
                    self.min_concurrency,
                    int(self.current_concurrency * self.decrease_factor)
                )
                
                if old_concurrency != self.current_concurrency:
                    logger.info(f"Decreasing concurrency: {old_concurrency} -> {self.current_concurrency}")
                
                self.fail_count = 0  # Reset counter

class NugetDatabase:
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
    
    def __init__(self, db_path: Path):
        self._db_path = db_path
        self._database: Optional[sqlite3.Connection] = None
        self._stats_id: Optional[int] = None
        self._successful_packages = 0
        self._failed_packages = 0
        self._updated_packages = 0
        self._total_packages = 0
    
    def __enter__(self) -> Self:
        self._database = sqlite3.connect(self._db_path)
        self._database.row_factory = sqlite3.Row
        self._init_database()
        self._start_stats()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> Literal[False]:
        if self._database is not None:
            self._end_stats()
            self._database.close()
            self._database = None
        return False
    
    @_requires_connection
    def _init_database(self) -> None:
        with self.get_cursor() as cursor:
            cursor.execute(CREATE_TABLE_PACKAGES_CMD)
            cursor.execute(CREATE_TABLE_PACKAGE_ARTIFACTS_CMD) 
            cursor.execute(CREATE_TABLE_JOIN_CMD)
            cursor.execute(CREATE_TABLE_LAST_PROCESSED_CMD)
            cursor.execute(CREATE_TABLE_FAILURES_CMD)
            cursor.execute(CREATE_TABLE_STATS_CMD)
            cursor.execute(INSERT_INITIAL_PROCESSED_PAGE_CMD)
            
    @_requires_connection
    def get_cursor(self) -> TransactionCursor:
        return self._database.cursor(factory=self.TransactionCursor)
    
    @_requires_connection
    def _start_stats(self) -> None:
        with self.get_cursor() as cursor:
            cursor.execute(START_SCRAPER_STATS_CMD)
            self._stats_id = cursor.lastrowid
    
    @_requires_connection
    def _end_stats(self) -> None:
        if self._stats_id is not None:
            with self.get_cursor() as cursor:
                cursor.execute(
                    END_SCRAPER_STATS_CMD, 
                    (self._total_packages, self._successful_packages, 
                     self._failed_packages, self._updated_packages, self._stats_id)
                )
    
    @_requires_connection
    def increment_stats(self, success: bool = True, updated: bool = False) -> None:
        self._total_packages += 1
        if success:
            self._successful_packages += 1
            if updated:
                self._updated_packages += 1
        else:
            self._failed_packages += 1

    @_requires_connection
    def add_package(self, package: NugetPackage) -> Optional[int]:
        try:
            with self.get_cursor() as cursor:
                # Check if package exists and is newer
                is_update = False
                cursor.execute(GET_PACKAGE_BY_NAME_CMD, (package.package_name,))
                existing = cursor.fetchone()
                
                if existing:
                    existing_date = existing["last_edited"]
                    if existing_date and package.last_edited:
                        if datetime.fromisoformat(package.last_edited.replace("Z", "+00:00")) <= \
                           datetime.fromisoformat(existing_date.replace("Z", "+00:00")):
                            # Not newer, no need to update
                            return existing["id"]
                        is_update = True
                
                # Insert or update package
                cursor.execute(
                    INSERT_PACKAGE_CMD, 
                    (
                        package.package_name, 
                        package.version, 
                        package.description, 
                        package.last_edited
                    )
                )
                
                if cursor.lastrowid:
                    package_id = cursor.lastrowid
                else:
                    # Get ID of existing package that was updated
                    cursor.execute(GET_PACKAGE_BY_NAME_CMD, (package.package_name,))
                    package_id = cursor.fetchone()["id"]
                
                # Add package entries
                if package.package_entries:
                    for entry in package.package_entries:
                        cursor.execute(
                            INSERT_PACKAGE_ARTIFACT_CMD,
                            (package_id, entry.name, entry.full_name)
                        )
                
                self.increment_stats(success=True, updated=is_update)
                return package_id
        except Exception as e:
            logger.error(f"Error adding package {package.package_name}: {str(e)}")
            self.increment_stats(success=False)
            self.record_failure(package.package_name, str(e))
            return None
    
    @_requires_connection
    def get_last_process(self) -> tuple[int, str]:
        with self.get_cursor() as cursor:
            result = cursor.execute(LAST_PROCESSED_QUERY).fetchone()
            return result["last_page"], result["last_update"]
    
    @_requires_connection
    def update_last_process(self, page_idx: int) -> None:
        with self.get_cursor() as cursor:
            current_time = datetime.now().isoformat()
            cursor.execute(UPDATE_PROCESSED_QUERY, (page_idx, current_time))
    
    @_requires_connection
    def record_failure(self, package_name: str, reason: str) -> None:
        with self.get_cursor() as cursor:
            current_time = datetime.now().isoformat()
            cursor.execute(INSERT_FAILURE_CMD, (package_name, reason, current_time, 0))
    
    @_requires_connection
    def get_failed_packages(self, max_retries: int = 3) -> List[str]:
        with self.get_cursor() as cursor:
            results = cursor.execute(GET_FAILED_PACKAGES_CMD, (max_retries,)).fetchall()
            return [row["package_name"] for row in results]

class NugetScraper:
    def __init__(
        self, 
        db_path: Path,
        max_concurrent_requests: int = 500,
        retry_delay: int = 10,
        retry_attempts: int = 3,
        time_filter: str = "2020-01-01T00:00:000Z",
        download_repo: str = "downloads/",
        extract_base: str = "package/",
        adaptive_concurrency: bool = True,
        initial_concurrency: int = 300,
        max_concurrency: int = 1000,
        batch_size: int = 200
    ):
        self.db_path = db_path
        self.max_concurrent_requests = max_concurrent_requests
        self.retry_delay = retry_delay
        self.retry_attempts = retry_attempts
        self.time_filter = time_filter
        self.download_repo = download_repo
        self.extract_base = extract_base
        self.batch_size = batch_size
        
        # Primary API endpoints for the NuGet v3 protocol
        # The catalog endpoint is the main resource for enumerating all packages
        self.catalog_url = "https://api.nuget.org/v3/catalog0/index.json"
        
        # The flat container is used for getting versions of a specific package
        self.flat_container_url = "https://api.nuget.org/v3-flatcontainer/"
        
        # The service index that lists all available API endpoints
        self.service_index_url = "https://api.nuget.org/v3/index.json"
        
        # Setup high concurrency manager since there's no rate limiting
        self.adaptive_concurrency = adaptive_concurrency
        if adaptive_concurrency:
            self.concurrency_manager = AdaptiveConcurrencyManager(
                initial_concurrency=initial_concurrency,
                max_concurrency=max_concurrency
            )
        else:
            self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # Performance metrics
        self.start_time = None
        self.end_time = None
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        
        # Ensure directories exist
        os.makedirs(download_repo, exist_ok=True)
        os.makedirs(extract_base, exist_ok=True)
    
    @asynccontextmanager
    async def _http_client(self, timeout: int = 30):
        """Context manager for HTTP client optimized for high throughput"""
        # Configure for maximum parallel connections
        limits = httpx.Limits(
            max_keepalive_connections=None,  # Allow unlimited keep-alive connections
            max_connections=None,  # Allow unlimited connections
            keepalive_expiry=60.0  # Keep connections alive for 60 seconds
        )
        transport = httpx.AsyncHTTPTransport(
            limits=limits,
            retries=1  # We handle retries ourselves
        )
        async with httpx.AsyncClient(
            timeout=timeout,
            transport=transport,
            headers={
                "User-Agent": "NuGet-Scraper/1.0",
                "Accept": "application/json",
            },
            follow_redirects=True
        ) as client:
            yield client
    
    async def fetch_url(self, url: str, retries: int = None, delay: int = None) -> Optional[Dict[str, Any]]:
        """Fetch data from URL with retry logic and adaptive concurrency"""
        if retries is None:
            retries = self.retry_attempts
        if delay is None:
            delay = self.retry_delay
            
        # Track performance metrics
        self.total_requests += 1
        start_time = time.time()
        
        # Use the appropriate concurrency control
        if self.adaptive_concurrency:
            async with self.concurrency_manager.managed_semaphore():
                result = await self._fetch_with_retries(url, retries, delay)
        else:
            async with self.semaphore:
                result = await self._fetch_with_retries(url, retries, delay)
                
        # Record request time
        request_time = time.time() - start_time
        if request_time > 5:  # Log slow requests
            logger.warning(f"Slow request: {url} took {request_time:.2f}s")
            
        return result
        
    async def _fetch_with_retries(self, url: str, retries: int, delay: int) -> Optional[Dict[str, Any]]:
        """Internal method to fetch with retries - optimized for no rate limits"""
        attempt = 0
        while attempt < retries:
            try:
                async with self._http_client() as client:
                    response = await client.get(url)
                    
                    if response.status_code == 404:
                        logger.warning(f"Resource not found at {url}")
                        self.failed_requests += 1
                        return None
                        
                    response.raise_for_status()
                    
                    # Report success to adaptive concurrency manager
                    if self.adaptive_concurrency:
                        await self.concurrency_manager.report_success()
                    
                    # Track metrics
                    self.successful_requests += 1
                    
                    # Try to parse JSON
                    try:
                        return response.json()
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON response from {url}")
                        self.failed_requests += 1
                        return None
                        
            except httpx.HTTPStatusError as e:
                # Since there's no rate limiting, we only need to handle server errors
                if e.response.status_code >= 500:
                    logger.warning(f"Server error {e.response.status_code} on {url}, retrying...")
                    
                    if self.adaptive_concurrency:
                        await self.concurrency_manager.report_failure(e.response.status_code)
                        
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"HTTP error {e.response.status_code} on {url}")
                    
                    if self.adaptive_concurrency:
                        await self.concurrency_manager.report_failure(e.response.status_code)
                        
                    await asyncio.sleep(1)
                    
            except (httpx.RequestError, httpx.TimeoutException) as e:
                logger.warning(f"Request error on {url}: {type(e).__name__}: {e}")
                
                if self.adaptive_concurrency:
                    await self.concurrency_manager.report_failure(None)
            
            attempt += 1
            if attempt < retries:
                backoff_delay = delay * (1.5 ** (attempt - 1))  # Gentler backoff since no rate limiting
                logger.info(f"Retrying {url} in {backoff_delay:.2f} seconds (attempt {attempt+1}/{retries})...")
                await asyncio.sleep(backoff_delay)
        
        # If we get here, all retries failed
        self.failed_requests += 1
        logger.error(f"Failed to fetch {url} after {retries} attempts.")
        return None
    
    async def process_package(self, package_info: Dict[str, Any]) -> Optional[NugetPackage]:
        """Process a single package and extract relevant information"""
        package_detail_url = package_info.get('@id')
        if not package_detail_url:
            return None
            
        # Fetch package details from the catalog leaf
        package_detail = await self.fetch_url(package_detail_url)
        if not package_detail:
            return None
            
        package_name = package_detail.get("id")
        if not package_name:
            return None
            
        package_name_lc = package_name.lower()
        
        # Create package object with data from the catalog
        package = NugetPackage(
            package_name=package_name_lc,
            version=package_detail.get("version") or package_info.get("nuget:version"),
            description=package_detail.get("description"),
            last_edited=package_detail.get("lastEdited") or package_info.get("commitTimeStamp")
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
    
    async def process_packages_batch(self, package_list: List[Dict[str, Any]]) -> List[NugetPackage]:
        """Process a batch of packages concurrently with adaptive concurrency"""
        results = []
        batch_start_time = time.time()
        
        # Create tasks - concurrency is managed by fetch_url
        # We don't need an additional semaphore here since fetch_url already handles concurrency
        tasks = [self.process_package(pkg) for pkg in package_list]
        
        # Process concurrently
        packages = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and None values
        success_count = 0
        error_count = 0
        for pkg in packages:
            if pkg is not None and not isinstance(pkg, Exception):
                results.append(pkg)
                success_count += 1
            elif isinstance(pkg, Exception):
                error_count += 1
                logger.error(f"Error processing package: {str(pkg)}")
        
        # Log performance metrics
        batch_time = time.time() - batch_start_time
        packages_per_second = len(results) / batch_time if batch_time > 0 else 0
        if self.adaptive_concurrency:
            concurrency = self.concurrency_manager.current_concurrency
        else:
            concurrency = self.max_concurrent_requests
            
        logger.info(
            f"Batch completed: {len(results)}/{len(package_list)} packages processed in {batch_time:.2f}s "
            f"({packages_per_second:.2f} pkgs/s, concurrency={concurrency}, "
            f"success={success_count}, errors={error_count})"
        )
                
        return results
    
    async def process_page(self, page_url: str) -> List[NugetPackage]:
        """Process a catalog page with optimized batch processing"""
        page_result = await self.fetch_url(page_url)
        if not page_result or "items" not in page_result:
            logger.error(f"Failed to get page data from {page_url}")
            return []
            
        package_list = page_result["items"]
        logger.info(f"Found {len(package_list)} packages on page")
        
        # Process in larger batches since there's no rate limiting
        packages = []
        
        for i in range(0, len(package_list), self.batch_size):
            batch = package_list[i:i+self.batch_size]
            logger.info(f"Processing batch {i//self.batch_size + 1}/{(len(package_list) + self.batch_size - 1)//self.batch_size} ({len(batch)} packages)")
            batch_packages = await self.process_packages_batch(batch)
            packages.extend(batch_packages)
            
        return packages
    
    async def run(self) -> None:
        """Main method to run the scraper"""
        logger.info("Starting NuGet package scraper with maximum concurrency")
        self.start_time = time.time()
        
        # Get catalog index - this is the main entry point for enumerating all packages
        res = await self.fetch_url(self.catalog_url)
        if not res:
            logger.error("Failed to fetch catalog index")
            return
            
        # Connect to database
        with NugetDatabase(self.db_path) as db:
            logger.info("Connected to NuGet database")
            
            # Get last processed page
            last_process_page, last_update = db.get_last_process()
            logger.info(f"Resuming from page {last_process_page}")
            
            # Get all pages from the catalog index
            pages = res.get("items", [])
            if not pages:
                logger.error("No catalog pages found")
                return
                
            page_iterations = len(pages[last_process_page:])
            logger.info(f"Found {page_iterations} remaining pages to process")
            
            # Process each catalog page
            page_bar = tqdm(total=page_iterations, desc="Processing pages")
            
            try:
                for idx, page in enumerate(pages[last_process_page:]):
                    # Get page info
                    page_idx = last_process_page + idx
                    page_last_edited = page.get("commitTimeStamp", "")
                    
                    # Skip old pages
                    if self.time_filter > page_last_edited:
                        logger.info(f"Skipping page {page_idx} (too old)")
                        db.update_last_process(page_idx)
                        page_bar.update()
                        continue
                        
                    # Process page
                    logger.info(f"Processing page {page_idx} ({idx+1}/{page_iterations})")
                    page_url = page['@id']
                    
                    try:
                        # Process all packages in the page
                        packages = await self.process_page(page_url)
                        
                        # Save to database
                        for package in packages:
                            db.add_package(package)
                            
                        # Update last processed page
                        db.update_last_process(page_idx)
                        
                        # Print status report with estimated completion time
                        self._print_status_report(idx+1, page_iterations)
                        
                    except Exception as e:
                        logger.error(f"Error processing page {page_idx}: {str(e)}")
                    
                    # Update progress bar
                    page_bar.update()
                    
                    # No need for delay between pages since there's no rate limiting
            finally:
                # Log final stats
                self.end_time = time.time()
                elapsed = self.end_time - self.start_time
                
                logger.info("\n======================== SCRAPER STATISTICS ========================")
                logger.info(f"Total running time: {timedelta(seconds=int(elapsed))}")
                logger.info(f"Total requests: {self.total_requests}")
                logger.info(f"Successful requests: {self.successful_requests}")
                logger.info(f"Failed requests: {self.failed_requests}")
                if self.adaptive_concurrency:
                    logger.info(f"Final concurrency level: {self.concurrency_manager.current_concurrency}")
                logger.info(f"Requests per second: {self.total_requests / elapsed if elapsed > 0 else 0:.2f}")
                logger.info("=====================================================================")
            
            logger.info("Scraping completed successfully")
            
    def _print_status_report(self, completed_pages: int, total_pages: int) -> None:
        """Print a status report with estimated completion time"""
        if completed_pages <= 0:
            return
            
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        # Calculate time per page and estimate remaining time
        time_per_page = elapsed / completed_pages
        remaining_pages = total_pages - completed_pages
        estimated_remaining = time_per_page * remaining_pages
        
        # Calculate estimated completion time
        completion_time = datetime.now() + timedelta(seconds=int(estimated_remaining))
        
        # Get current concurrency
        if self.adaptive_concurrency:
            concurrency = self.concurrency_manager.current_concurrency
        else:
            concurrency = self.max_concurrent_requests
            
        # Print status report
        logger.info(
            f"Status: {completed_pages}/{total_pages} pages processed "
            f"({completed_pages/total_pages*100:.1f}%) - "
            f"Elapsed: {timedelta(seconds=int(elapsed))}, "
            f"Remaining: {timedelta(seconds=int(estimated_remaining))}, "
            f"ETA: {completion_time.strftime('%Y-%m-%d %H:%M:%S')}, "
            f"Concurrency: {concurrency}"
        )

async def main():
    """Entry point for the script"""
    parser = argparse.ArgumentParser(
        description="Create Packages DB from NuGet packages with high concurrency"
    )
    parser.add_argument(
        '-o', '--output',
        required=False,
        type=Path, default=Path('NugetPackageDB.db')
    )
    parser.add_argument(
        '--concurrency',
        type=int, default=500,
        help='Maximum number of concurrent requests (default: 500, can go higher since no rate limits)'
    )
    parser.add_argument(
        '--max-concurrency',
        type=int, default=1000,
        help='Maximum concurrency limit for adaptive mode (default: 1000)'
    )
    parser.add_argument(
        '--retry',
        type=int, default=3,
        help='Number of retry attempts for failed requests'
    )
    parser.add_argument(
        '--from-date',
        type=str, default="2020-01-01T00:00:000Z",
        help='Process only packages updated after this date (ISO format)'
    )
    parser.add_argument(
        '--retry-failed',
        action='store_true',
        help='Retry previously failed packages'
    )
    parser.add_argument(
        '--test-api',
        action='store_true',
        help='Test NuGet API endpoints before running'
    )
    parser.add_argument(
        '--fixed-concurrency',
        action='store_true',
        help='Use fixed concurrency instead of adaptive concurrency'
    )
    parser.add_argument(
        '--batch-size',
        type=int, default=200,
        help='Number of packages to process in each batch (default: 200)'
    )
    
    args = parser.parse_args()
    
    # Test API endpoints if requested
    if args.test_api:
        await test_nuget_api()
        return
    
    # Initialize and run scraper
    scraper = NugetScraper(
        db_path=args.output,
        max_concurrent_requests=args.concurrency,
        retry_attempts=args.retry,
        time_filter=args.from_date,
        adaptive_concurrency=not args.fixed_concurrency,
        initial_concurrency=args.concurrency,
        max_concurrency=args.max_concurrency,
        batch_size=args.batch_size
    )
    
    try:
        await scraper.run()
    except KeyboardInterrupt:
        logger.info("Scraper stopped by user")
    except Exception as e:
        logger.critical(f"Unhandled exception: {str(e)}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main())