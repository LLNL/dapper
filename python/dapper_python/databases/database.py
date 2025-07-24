from __future__ import annotations

import sqlite3
import functools

from pathlib import Path

from typing import Literal, Callable, TypeVar, ParamSpec
from typing import Any
from collections.abc import Generator


T = TypeVar("T")
P = ParamSpec("P")
class Database:
    """Base class for databases"""

    class Cursor(sqlite3.Cursor):
        """A modified subclass of sqlite3.Cursor that adds support for transaction handling inside a context manager

        This allows for running multiple commands inside the context which will all be treated as part of a transaction
        Upon exiting, all changes are committed, or if an exception occurs, the transaction is rolled back and none are committed
        This helps ensure atomicity with multiple commands
        Behaves the same as using the connection's context manager, but is usable on the cursor object as well
        
        Adds support for streaming_based functionality similar to fetch_all but does not read all values in memory
        Allowing for lighter-weight memory usage
        """
        #TODO: Potentially replace with typing_extensions.self
        def __enter__(self) -> Database.Cursor:
            self.connection.__enter__()
            return self

        def __exit__(self, exc_type, exc_val, exc_tb) -> Literal[False]:
            self.connection.__exit__(exc_type, exc_val, exc_tb)
            return False

        def fetchall_chunked(self, *, chunk_size:int=1024) -> Generator[Any, None, None]:
            """Streams the results of previously executed command(s)

            Behaves similarly to fetchall() in that it will retrieve all results
            But fetches them in chunks of N entries (specified by chunk_size) and yields each entry one at a time
            Instead of loading all results into memory to reduce memory overhead
            """
            while True:
                entries = self.fetchmany(chunk_size)
                if not entries:
                    return
                yield from entries

    @staticmethod
    def _requires_connection(func:Callable[[P], T]) -> Callable[[P], T]:
        """Wrapper function which is used to decorate functions that require a database connection to work
        If a decorated function is called when the database is not open/connected, then an exception is raised
        """
        @functools.wraps(func)
        def wrapper(self:Database, *args:P.args, **kwargs:P.kwargs) -> T:
            if self._db is None:
                raise sqlite3.ProgrammingError("Cannot operate on a closed database")
            return func(self, *args, **kwargs)
        return wrapper

    def __init__(self, db_path:Path, *, mode='rw') -> None:
        #Base class defaults to allow both reading and writing
        #Implementations intended for end-use should set to read-only
        self._db_path = db_path

        uri = f'file:{db_path}?mode={mode}'
        self._db = sqlite3.connect(uri, uri=True)

    def __enter__(self) -> Database:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> Literal[False]:
        return False

    @_requires_connection
    def cursor(self) -> Database.Cursor:
        #Allow user to access the cursor to perform queries outside of things we've thought of
        #End-use applications should be opened in readonly mode to prevent changes, and only used for querying
        return self._db.cursor(factory=self.Cursor)