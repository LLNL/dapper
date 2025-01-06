"""
This script processes all packages listed on PyPI's index to creates a database of what package(s) correspond to each import name
The index of all listed packages can be found here: https://pypi.python.org/simple/

The result is stored in a sqlite database

The database has two tables:
    "packages" and "package_imports"
However the best method is to use the view "v_package_imports" which omits some of the columns used for tracking
Which are unnecessary for end-users
The table has two columns:
    package_name        - The name of the package                   ex: "beautifulsoup4"
    import_as           - The name used to import the package       ex: "bs4"

The import_as column is indexed for fast lookups, as the import_as is the primary value that will be searched for

Since the scraping process can take several hours to complete, this is designed to be able to stop and restart mid-process
To avoid duplicating time-consuming work should it be stopped part-way through
"""
from __future__ import annotations

import argparse
import requests
import sqlite3
import math
import time
import io
import concurrent.futures
import zipfile
import functools

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from http import HTTPStatus
from contextlib import suppress
from natsort import natsorted
from tqdm.auto import tqdm
from itertools import repeat
from more_itertools import chunked

from typing import Final, ClassVar, Literal
from typing import TypeVar, Callable, ParamSpec
from typing import Generator, Iterable, Any
from typing_extensions import Self


T = TypeVar("T")
P = ParamSpec("P")
class PyPIDatabase:
    """Handles reading from and writing to the database

    SQLite doesn't support stored procedures, so this class contains several pre-defined function (e.g add_package_imports)
    Which take their place of an API-of-sorts for an operation with several backend steps

    Connection opening/closing is handled using context manager
    So it should be used as
    with PyPIDatabase(<db_path>) as db: #Database is opened here
        <code>
    #Database is closed here
    """

    class TransactionCursor(sqlite3.Cursor):
        """A modified subclass of sqlite3.Cursor that adds support for transaction handling inside a context manager

        The PyPIDatabase class already uses the context manager to handle database opening/closing
        So it can't be used in the same way that the sqlite3.Connection class does to handle transactions

        Since the cursor class does not implement its own context manager (and is normally used to run commands anyway)
        It is used to add similar functionality

        This allows for running multiple commands inside the context which will all be treated as part of a transaction
        Then upon exiting, all changes are commited, or if an exception occurs, the transaction is rolled back and none are commited
        This helps ensure atomicity with multiple commands
        """
        def __enter__(self) -> Self:
            self.connection.__enter__()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb) -> Literal[False]:
            self.connection.__exit__(exc_type, exc_val, exc_tb)
            return False

    @staticmethod
    def _requires_connection(func:Callable[[P], T]) -> Callable[[P], T]:
        """Wrapper function which is used to decorate functions that require a database connection to work
        If a decorated function is called when the database is not open/connected, then an exception is raised
        """
        @functools.wraps(func)
        def wrapper(self:PyPIDatabase, *args:P.args, **kwargs:P.kwargs) -> T:
            if self._database is None:
                raise sqlite3.ProgrammingError("Cannot operate on a closed database")
            return func(self, *args, **kwargs)
        return wrapper

    def __init__(self, db_path:Path):
        self._db_path = db_path
        self._database:sqlite3.Connection|None = None

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
        """Initializes the database to create the required tables, indexes, and views
        """
        with self.get_cursor() as cursor:
            #The last_serial column is used for checking values when resuming from a partially-built DB or updating the DB
            create_table_cmd = """
                CREATE TABLE
                IF NOT EXISTS packages(
                    id INTEGER PRIMARY KEY,
                    package_name TEXT,
                    last_serial INTEGER
                )
            """
            cursor.execute(create_table_cmd)
            # create_index_cmd = """
            #     CREATE INDEX
            #     IF NOT EXISTS idx_package_name
            #     ON packages(package_name);
            # """
            # cursor.execute(create_index_cmd)

            create_table_cmd = """
                CREATE TABLE
                IF NOT EXISTS package_imports(
                    id INTEGER PRIMARY KEY,
                    package_id INTEGER,
                    import_as TEXT,
                    FOREIGN KEY (package_id) REFERENCES packages(id) ON DELETE CASCADE
                )
            """
            cursor.execute(create_table_cmd)
            create_index_cmd = """
                CREATE INDEX
                IF NOT EXISTS idx_package_id
                ON package_imports(package_id);
            """
            cursor.execute(create_index_cmd)
            create_index_cmd = """
                CREATE INDEX
                IF NOT EXISTS idx_import_as
                ON package_imports(import_as);
            """
            cursor.execute(create_index_cmd)

            #User-facing view which hides the backend tracking logic
            create_view_cmd = """
                CREATE VIEW
                IF NOT EXISTS v_package_imports
                AS 
                    SELECT package_name, import_as
                    FROM packages
                    JOIN package_imports
                    ON packages.id = package_imports.package_id
            """
            cursor.execute(create_view_cmd)

    @_requires_connection
    def get_cursor(self) -> TransactionCursor:
        """Gets a cursor for interacting with the database
        Uses the TransactionCursor subclass to allow for automatic transaction handling

        :return: Cursor object with additional support for use with a context manager to automatically run transactions
        """
        return self._database.cursor(factory=self.TransactionCursor)

    @_requires_connection
    def get_processed_packages(self) -> Generator[tuple[str,int], None, None]:
        """Gets a list of all the packages that have been added to the database along with their serial number

        This is mainly used for comparing what values are already in the database against any remaining values to scrape
        (In case the process was stopped mid-way)

        :return: A generator of tuples with the format (package_name, serial)
        """
        cursor = self.get_cursor()
        package_query = """
            SELECT package_name, last_serial
            FROM packages
        """
        packages = (
            (package_name, last_serial)
            for package_name, last_serial, *_ in cursor.execute(package_query).fetchall()
        )
        yield from packages

    @_requires_connection
    def add_package_imports(self, package_name:str, package_imports:str|Iterable[str], *, serial:int) -> None:
        """Adds a package and import names to the database

        :param package_name: Name of the package
        :param package_imports: Any top-level names that are imported from the package
        :param serial: A number designed to keep track of when the package was processed
                       Intended to be the last_serial field from the PyPI index, but could be a custom value
        """
        if isinstance(package_imports, str):
            package_imports = (package_imports, )

        with self.get_cursor() as cursor:
            insert_package_cmd = """
                INSERT INTO packages(package_name, last_serial)
                values (?, ?)
            """
            cursor.execute(insert_package_cmd, (package_name, serial))

            #We need the saved id in order to reference as foreign key in the package_imports table
            package_id = cursor.lastrowid

            insert_import_cmd = """
                INSERT INTO package_imports(package_id, import_as)
                values (?, ?)
            """
            cursor.executemany(insert_import_cmd, zip(repeat(package_id), package_imports))

    @_requires_connection
    def remove_packages(self, package_names:str|Iterable[str]) -> None:
        """Removes the specified package(s) from the database
        This will remove the package from the "packages" table and also associated entries in "package_imports" table

        :param package_names: Name(s) of the package(s) to remove
        """
        if isinstance(package_names, str):
            package_names = (package_names, )

        with self.get_cursor() as cursor:
            remove_package_cmd = """
            DELETE FROM packages
            WHERE package_name = ?
            """
            cursor.executemany(remove_package_cmd, ((name,) for name in package_names))

    PYPI_INDEX_URL:ClassVar[str] = 'https://pypi.python.org/simple/'


@dataclass
class PyPIPackage:
    package_name: str
    last_serial: int|None = None

    def get_package_info(self) -> dict[str, Any]:
        """Gets the information contained on the package's PyPI page in json format

        :return: JSON-formatted data retrieved from the endpoint
        """
        url = self._API_PACKAGE_URL.format(package_name=self.package_name)
        return self._web_request(url).json()


    def get_imports(self) -> list[str]|None:
        """Downloads the .whl (wheel) file for the package and attempts to determine how the package is imported
        This may differ from the name of the package itself
        eg: BeautifulSoup4 is installed as "pip install beautifulsoup4" but used in source code as "import bs4"

        :return: A list of names that can be imported from the package
                 If unable to determine, then None is returned instead
        """
        package_info = self.get_package_info()

        releases = package_info['releases']
        releases = dict(natsorted(releases.items(), reverse=True))

        #Only keep ones that have wheels and have not been yanked
        releases = {
            version:data
            for version, data in releases.items()
            if any(
                x['url'].endswith('.whl')
                and not x['yanked']
                for x in data
            )
        }
        if not releases:
            return None

        version, release_data = next(iter(releases.items()))
        release_data = next(
            x
            for x in release_data
            if x['url'].endswith('.whl')
            and not x['yanked']
        )

        with self._web_request(release_data['url']) as web_request:
            data = io.BytesIO(web_request.content)

        with zipfile.ZipFile(data) as wheel_file:
            files = tuple(PurePosixPath(x) for x in wheel_file.namelist())

            #Sometimes contains a top_level.txt file which details the top-level imports for the package
            #If this is available, then use it as it's likely to be the most reliable information
            top_level_txt = next((x for x in files if x.name == 'top_level.txt'), None)
            if top_level_txt:
                text_data = wheel_file.read(str(top_level_txt)).decode('utf-8')
                imports = list(line.strip() for line in text_data.splitlines() if line)
                #Sometimes these files can be empty
                if imports:
                    #TODO: If a top_level.txt is present, does this preclude other imports?
                    #I.e. Should we keep checking or stop here?
                    return imports

            #If it doesn't have that file or couldn't parse it
            #Then fall back on trying to check what directories are importable as modules
            top_level_paths = tuple({
                PurePosixPath(x.parents[-2] if len(x.parents) >= 2 else x)
                for x in files
            })

            #Check for any top-level python files, as these should also be importable
            importable_files = {
                file.stem
                for file in top_level_paths
                if file.name.endswith('.py')
                and not file.name.startswith('_')
            }

            #Check for any top-level paths that contain an __init__.py
            importable_dirs = {
                directory.name
                for directory in (Path(y) for y in top_level_paths)
                if any(
                    file.name == '__init__.py'
                    and file.parent == directory
                    for file in files
                )
            }

            #TODO: Any other/better methods for determining importable names?
            #This seems to produce a fair amount of correct values, but also a fair number of duplicates across packages

            #Filter out any empty strings as this will mess up later queries
            return list(x for x in importable_files | importable_dirs if x) or None

    @staticmethod
    def _web_request(url:str, *, retries:int=5) -> requests.Response:
        """Attempts to retrieve the web content from the specified URL

        From a software design perspective, this doesn't necessarily belong in this class
        But this is the only class accesses the internet, so it's a convent spot to put it

        :param url: The URL to retrieve the content from
        :param retries: Number of times to retry the request if it fails to respond with an HTTP OK status
        :return: A requests response from sending a GET request to the specified URL
        """
        for _ in range(retries+1):
            try:
                web_request = requests.get(url)

                match web_request.status_code:
                    case HTTPStatus.OK:
                        return web_request
                    case HTTPStatus.TOO_MANY_REQUESTS:
                        delay = web_request.headers.get('Retry-After', 1)
                        time.sleep(delay)
                        continue
                    case HTTPStatus.NOT_FOUND:
                        break
                    case _:
                        #TODO: How do we want to handle other status codes? For now retry
                        continue

            except requests.exceptions.ConnectionError:
                time.sleep(1)
                continue

        raise requests.exceptions.RequestException('Could not get requested data')

    _API_PACKAGE_URL:ClassVar[str] = 'https://pypi.org/pypi/{package_name}/json'


def main():
    parser = argparse.ArgumentParser(
        description="Create Python imports DB from PyPI packages"
    )
    parser.add_argument(
        '-o','--output',
        required=False,
        type=Path, default=Path('PyPIPackageDB.db'),
        help='Path of output (database) file to create. Defaults to "PyPIPackageDB.db" in the current working directory',
    )
    args = parser.parse_args()

    #Ask it to send the response as JSON
    #If we don't set the "Accept" header this way, it will respond with HTML instead of JSON
    json_headers = {
        'Accept': 'application/vnd.pypi.simple.v1+json'
    }
    with requests.get(PyPIDatabase.PYPI_INDEX_URL, headers=json_headers) as web_request:
        catalog_info = web_request.json()
        package_list = {
            entry['name']:entry['_last-serial']
            for entry in catalog_info['projects']
        }

    with PyPIDatabase(args.output) as db:
        #Remove any outdated packages
        processed_packages = dict(db.get_processed_packages())
        to_remove = [
            name
            for name, serial in package_list.items()
            if name in processed_packages
            and processed_packages[name] != serial
        ]
        db.remove_packages(to_remove)

        #Only process packages which have not already been processed
        processed_packages = dict(db.get_processed_packages())
        to_process = {
            name:serial
            for name,serial in package_list.items()
            if name not in processed_packages
        }

        #Break into chunks to process
        CHUNK_SIZE:Final[int] = 500
        chunked_entries = chunked(to_process.items(), CHUNK_SIZE) #Process 500 at a time to speed up
        chunk_count = int(math.ceil(len(to_process) / CHUNK_SIZE))

        progress_iter = tqdm(
            chunked_entries,
            total=chunk_count,
            desc='Processing package slice', colour='green',
            unit='Slice',
            position=None, leave=None,
            disable=False,
        )
        for chunk in progress_iter:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                threads = [
                    pool.submit(PyPIPackage(name).get_imports)
                    for name, serial in chunk
                ]

                progress_bar = tqdm(
                    concurrent.futures.as_completed(threads),
                    total=len(threads),
                    desc='Scraping Package', colour='blue',
                    unit='Package',
                    position=None, leave=None,
                    disable=not to_process,
                )
                list(progress_bar)

                for (package_name, last_serial), future in zip(chunk, threads):
                    with suppress(requests.exceptions.ConnectionError, requests.exceptions.RequestException):
                        import_names = future.result()
                        #If we can't figure out the import names, skip the package so we don't end up with empty values
                        if not import_names:
                            continue
                        db.add_package_imports(package_name, import_names, serial=last_serial)

if __name__ == '__main__':
    main()