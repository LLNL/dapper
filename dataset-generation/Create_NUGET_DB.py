import argparse
import sqlite3
from functools import wraps
from pathlib import Path
import httpx
import asyncio
from typing import Literal, Optional, List
from typing import TypeVar, Callable, ParamSpec, Literal, Generator, Iterable, Any
from typing_extensions import Self
from itertools import repeat
from dataclasses import dataclass
from tqdm.auto import tqdm
from datetime import datetime


CREATE_TABLE_PACAKGES_CMD = """
                    CREATE TABLE IF NOT EXISTS packages(
                        id INTEGER PRIMARY KEY,
                        package_name TEXT,
                        package_url TEXT,
                        description TEXT,
                        dependencies TEXT,
                        last_edited TEXT,
                        last_serial INTEGER
                    )
                """

CREATE_TABLE_PACKAGE_ENTRIES_CMD = """
                    CREATE TABLE IF NOT EXISTS package_entries (
                        id INTEGER PRIMARY KEY,
                        package_id INTEGER,
                        url TEXT, 
                        fullname TEXT NOT NULL,
                        FOREIGN KEY (package_id) REFERENCES packages(id) ON DELETE CASCADE
                    )
                """

CREATE_TABLE_LAST_PROCESSED_CMD = """
                    CREATE TABLE IF NOT EXISTS last_processed_page (
                        id INTEGER PRIMARY KEY,
                        last_page INTEGER
                    )

"""


PACKAGE_QUERY = """
            SELECT package_name, last_serial FROM packages
        """
LAST_PROCESSED_QUERY = """
            SELECT last_page FROM last_processed_page
        """
UPDATE_PROCESSED_QUERY = """
            UPDATE last_processed_page
            SET last_page = ?
        """

INSERT_PACKAGE_CMD = """
                INSERT INTO packages(package_name, package_url, description, dependencies,last_edited, last_serial)
                values (?,?,?,?,?,?)
            """

INSERT_ENTRIES_CMD = """
                INSERT INTO package_entries(package_id, url, fullname)
                values (?,?,?)
            """
INSERT_INITIAL_PROCEESED_PAGE_CMD = """
                INSERT INTO last_processed_page(last_page)
                SELECT 0
                WHERE NOT EXISTS (SELECT 1 FROM last_processed_page)
            """
@dataclass
class NugetPackage:
    package_name: str
    package_url: str
    description: Optional[str]
    dependencies:str
    last_edited:str
    last_serial: int|None

@dataclass
class PackageEntries:
    package_id: int
    url: str
    full_name: str

T = TypeVar("T")
P = ParamSpec("P")
class NugetDatabase:
    class TransactionCursor(sqlite3.Cursor):
        def __enter__(self) -> Self:
            self.connection.__enter__()
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb) -> Literal[False]:
            self.connection.__exit__(exc_type, exc_val, exc_tb)
            return False
        
    @staticmethod
    def _requires_connection(func:Callable[[P], T])-> Callable[[P], T]:

        @wraps(func)
        def wrapper(self, *args:P.args, **kwargs:P.kwargs) ->T:
            if self._database is None:
                raise sqlite3.ProgrammingError("Cannot operate on a closed database")
            return func(self, *args, **kwargs)
        return wrapper
    
    def __init__(self, db_path:Path):
        self._db_path = db_path
        self._database:sqlite3.Connection|None
    
    def __enter__(self) -> Self:
        self._database = sqlite3.connect(self._db_path)
        self._init_database()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> Literal[False]:
        if self._database is not None:
            self._database.close()
            self._database = None
        return False
    
    @_requires_connection
    def _init_database(self) -> None:

        with self.get_cursor() as cursor:
            with self.get_cursor() as cursor:
                cursor.execute(CREATE_TABLE_PACAKGES_CMD)
                cursor.execute(CREATE_TABLE_PACKAGE_ENTRIES_CMD) 
                cursor.execute(CREATE_TABLE_LAST_PROCESSED_CMD)
                cursor.execute(INSERT_INITIAL_PROCEESED_PAGE_CMD)
            
    @_requires_connection
    def get_cursor(self) -> TransactionCursor:
        return self._database.cursor(factory=self.TransactionCursor)
    
    @_requires_connection
    def get_processed_packages(self) -> Generator[tuple[str,int], None, None]:
        cursor = self.get_cursor()
        packages = (
            (package_name, last_serial)
            for package_name, last_serial, *_ in cursor.execute(PACKAGE_QUERY).fetchone()
        )

        yield from packages

    @_requires_connection
    def add_package(self, package_name:str, package_url, description, package_entries:str|Iterable[str],last_edited,*, serial:int) -> None:
        
        with self.get_cursor() as cursor:
            
            cursor.execute(INSERT_PACKAGE_CMD, (package_name, package_url, description,last_edited, serial))
            package_id = cursor.lastrowid
            cursor.executemany(INSERT_ENTRIES_CMD, zip(repeat(package_id), (item[0] for item in package_entries), (item[1] for item in package_entries)))
    
    @_requires_connection
    def add_packages(self, packages:Iterable[NugetPackage]) -> None:
        
        with self.get_cursor() as cursor:
            package_names = [package.package_name for package in packages]
            package_urls = [package.package_url for package in packages]
            package_descriptions = [package.description for package in packages]
            package_dependencies = [package.dependencies for package in packages]
            package_last_edited = [package.last_edited for package in packages]
            package_serials = [package.last_serial for package in packages]
            
            cursor.executemany(INSERT_PACKAGE_CMD, zip(package_names, package_urls, package_descriptions, package_dependencies, package_last_edited, package_serials))

    @_requires_connection
    def remove_packages(self, package_name:str) -> None:

        with self.get_cursor() as cursor:
            remove_package_cmd = """
                DELETE FROM packages
                WHERE package_name = ?
            """
            cursor.execute(remove_package_cmd, (package_name))
    
    @_requires_connection
    def remove_duplicates(self, ) -> None:
        with self.get_cursor() as cursor:
            remove_dupicates_cmd = """
                DELETE FROM packages
                WHERE id NOT IN (
                    SELECT MIN(id)
                    FROM packages
                    GROUP BY pcakge_name, last_serial
                )
            """
    
    @_requires_connection
    def get_last_process(self) -> int:
        with self.get_cursor() as cursor:
            result = cursor.execute(LAST_PROCESSED_QUERY).fetchone()
            return result[0]

    @_requires_connection
    def update_last_process(self, page_idx) -> None:
        with self.get_cursor() as cursor:
            cursor.execute(UPDATE_PROCESSED_QUERY, page_idx)

def multitreading(func):
    async def wrapper(*args,max_concurrent_requests=1000, **kwargs):
        
            if isinstance(args[0], str):  
                res = await func(*args, **kwargs)
                return res
        
            if isinstance(args[0], List):
                semaphore = asyncio.Semaphore(max_concurrent_requests)
                async with semaphore:
                    tasks = [fetch_url(url) for url in args[0]]
                    results = await asyncio.gather(*tasks)
                    return results
    return wrapper

@multitreading
async def fetch_url(url, retries=3, delay= 60, timeout=10):
    attempt  = 0
    while attempt < retries:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                return response.json()
        except httpx.RequestError as e:
            print(f"Request error on {url}: {e}")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", 1))
                print(f"Rate limit hit for {url}, retrying after {retry_after} seconds...")
                await asyncio.sleep(retry_after)
            else:
                print(f"HTTP error {e.response.status_code} on {url}: {e}")
        except httpx.TimeoutException as e:
            print(f"Timeout occurred for {url}: {e}")

        attempt +=1
        if attempt < retries:

            print(f"Retrying {url} in {delay:.2f} seconds (attempt {attempt}/{retries})...")
            await asyncio.sleep(delay)
    print(f"Failed to fetch {url} after {retries} attempts.")
    return None
       

async def process_data(queue, url, worker_id, stop_event):
        while not stop_event.is_set():
            print(f"Producer-{worker_id}: fetching content... ")
            content = await fetch_url(url)
            await queue.put(content)
            print(f"process-{worker_id}: Added content to queue")
        print(f"Process-{worker_id}: Stopping")

async def persist_data(queue):
    while True:
        content = await queue.get()
        if content is None:
            break
        queue.task_done()

async def main():
    parser = argparse.ArgumentParser(
        description="Create Packages DB from Nuget packages"
    )
    parser.add_argument(
        '-o-','--output',
        required=False,
        type=Path, default=Path('NugetPackageDB.db')
    )

    args = parser.parse_args()

    root_url = "https://api.nuget.org/v3/catalog0/index.json"

    res = await fetch_url(root_url)

    time_filter = "2020-01-01T00:00:000Z"

    
    with NugetDatabase(args.output) as db:
        print(f"Creationg NugetDatabase...")
        last_process_page = db.get_last_process()
        print(f"last page: {last_process_page}")
        page_items = res["items"]
        page_iterations = len(page_items[last_process_page:])
        page_bar = tqdm(total=page_iterations, desc="page progressing...")

        for idx, page_item in enumerate(page_items[last_process_page:]):
            page_url = page_item['@id']
            page_detail = await fetch_url(page_url)
            package_list = page_detail['items']

            package_processing_list= []

            for package in package_list:
                package_detail_url = package['@id']
                package_processing_list.append(package_detail_url)
                
            results = await fetch_url(package_processing_list)
            nuget_packages = []
            package_bar = tqdm(total=len(package_list), desc="package progressing...")
            
            for result in results:
                entries_str = ""
                if result.get("packageEntries") is not None:
                    package_entries = [packageEntry["@id"] for packageEntry in result.get("packageEntries")]
                    entries_str = ",".join(package_entries)
                nuget_package = NugetPackage(package_name=result.get("id"), 
                                                     package_url=result.get("@id"),
                                                     description=result.get("description"), 
                                                     dependencies=entries_str, 
                                                     last_edited=result.get("lastEdited"),
                                                     last_serial=result.get("packageHash")) 
                
                if nuget_package.last_serial is not None and nuget_package.last_edited > time_filter:
                    nuget_packages.append(nuget_package)
                package_bar.update()
            
            page_idx = last_process_page+idx
            db.add_packages(nuget_packages)
            db.update_last_process((page_idx,))
            
            page_bar.update()

        print(f"Remove duplicate records...")
        db.remove_duplicates()
        print(f"Done")

if __name__ == "__main__":
    asyncio.run(main())
    


        
