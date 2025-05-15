import os
import sys
import platform
import sqlite3
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Union, Tuple
import tomlkit
import pandas as pd
from contextlib import contextmanager

@dataclass
class DatasetMeta:
    name: str
    version: str
    format: str
    timestamp: datetime
    categories: List[str]
    filepath: Path


class DatasetCatalog:
    """Class for discovering and loading SQLite databases"""
    @staticmethod
    def get_app_data_dir(app_name: Optional[str] = "dapper") -> str:
        """Get the platform-specific application data directory"""
        
        system = platform.system()
        
        if system == 'Linux':
            # Linux: $XDG_DATA_HOME/app_name or $HOME/.local/share/app_name
            xdg_data_home = os.environ.get('XDG_DATA_HOME')
            if xdg_data_home:
                return os.path.join(xdg_data_home, app_name)
            else:
                return os.path.join(os.path.expanduser('~'), '.local', 'share', app_name)
        
        elif system == 'Darwin':  # macOS
            # macOS: $HOME/Library/Application Support/app_name
            return os.path.join(os.path.expanduser('~'), 'Library', 'Application Support', app_name)
        
        elif system == 'Windows':
            # Windows: %APPDATA%\app_name
            appdata = os.environ.get('APPDATA')
            if appdata:
                return os.path.join(appdata, app_name)
            else:
                # Fallback if APPDATA is not defined
                return os.path.join(os.path.expanduser('~'), 'AppData', 'Roaming', app_name)
        
        else:
            # Unknown platform, use a reasonable default
            return os.path.join(os.path.expanduser('~'), f'.{app_name}')
    
    @staticmethod
    def _find_toml(app_name: Optional[str] = "dapper", file_path: Optional[str] = None) -> Path:

        """
        Look for `dataset_info.toml`. If `file_path` is given, search
        that path and its parents. Otherwise, look under the app data dir.
        """
        if file_path:
            path = Path(file_path)
            for candidate in [path, *path.parents]:
                if candidate.is_file():
                    return candidate
            raise FileNotFoundError(f"Could not find TOML at or above {file_path}")


        filename = "dataset_info.toml"
        app_dir = Path(DatasetCatalog.get_app_data_dir(app_name))  # ensure this returns a path‐like string
        candidate = app_dir / filename
        if candidate.is_file():
            return candidate

        raise FileNotFoundError(f"Could not find {filename} in {app_dir}")




    def __init__(self, app_name: Optional[str] = "dapper", file_path: Optional[str] = None):

        
        # find dataset_info.toml
        toml_path = DatasetCatalog._find_toml(app_name, file_path)   

        # load filepath from dataset_info.toml
        cfg = tomlkit.load(toml_path)

        # buld a list of dataset meta
        self.dataset_metas: List[DatasetMeta] = []

        for name, meta in cfg.get("datasets", {}).items():
            self.dataset_metas.append(DatasetMeta(
                name = name,
                version = meta["version"],
                format = meta["format"],
                timestamp = meta["timestamp"],
                categories = meta["categories"],
                filepath = Path(meta["filepath"])
            ))
    
    def list_dataset_names(self) -> List[str]:
        """Return all dataset keys (i.e. the [datasets.<name>] entries)."""
        return [meta.name for meta in self.dataset_metas]
    
    def __len__(self) -> int:
        """Total number of datasets found in the TOML."""
        return len(self.dataset_metas)
    
    def __iter__(self):
        """Iterate over DatasetMeta objects."""
        yield from self.dataset_metas

    def __getitem__(self, name: str) -> DatasetMeta:
        """Lookup metadata by dataset name, or KeyError if not present."""
        for m in self.dataset_metas:
            if m.name == name:
                return m
        raise KeyError(f"No dataset called {name!r}")
    
    def validate_filepaths(self) -> None:
        """
        Check that every metadata.filepath actually exists on disk.
        Raises FileNotFoundError listing all missing files.
        """
        missing = [m.filepath for m in self.dataset_metas if not m.filepath.exists()]
        if missing:
            raise FileNotFoundError(f"Missing database files:\n" +
                                     "\n".join(str(p) for p in missing))
    
    
    def summary(self) -> None:
        """Print a quick table of name, version, format, path, etc."""
        for m in self.dataset_metas:
            print(f"{m.name:20s} v{m.version:<3d}  {m.format:6s}  {m.filepath}")


class SQLiteReader:
    def __init__(self, catalog):
        self.catalog = catalog
        self.connections = {}

    def get_connection(self, dataset_name: str) -> sqlite3.Connection:

        # Check if we already have an open connection to this database
        if dataset_name in self.connections:
            return self.connections[dataset_name]
        
        # Get metadata for the dataset
        meta = self.catalog[dataset_name]
        
        # Ensure the database file exists
        if not meta.filepath.exists():
            raise FileNotFoundError(f"Database file not found: {meta.filepath}")
        
        # Create a new connection with read-only mode
        try:
            # URI path with read-only mode
            uri = f"file:{meta.filepath}?mode=ro"
            
            # Create connection
            conn = sqlite3.connect(uri, uri=True)
            conn.row_factory = sqlite3.Row
            
            # Cache the connection
            self.connections[dataset_name] = conn
            return conn
        except sqlite3.Error as e:
            raise sqlite3.Error(f"Error connecting to {dataset_name}: {e}")
    
    @contextmanager
    def connection(self, dataset_name: str):
      
        conn = self.get_connection(dataset_name)
        try:
            yield conn
        finally:
            # We don't close the connection here as we're caching connections
            pass

    def execute_query(self, 
                    dataset_name: str, 
                    query: str, 
                    parameters: Optional[Union[Tuple, Dict[str, Any]]] = None) -> List[sqlite3.Row]:
        """
        Execute a SQL query on the specified dataset.
        
        Args:
            dataset_name: Name of the dataset as listed in the catalog
            query: SQL query to execute
            parameters: Optional parameters for the query
            
        Returns:
            List of sqlite3.Row objects representing the query results
            
        Raises:
            KeyError: If dataset_name is not in the catalog
            sqlite3.Error: If there's an error executing the query
        """
        with self.connection(dataset_name) as conn:
            try:
                cursor = conn.cursor()
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)
                return cursor.fetchall()
            except sqlite3.Error as e:
                raise sqlite3.Error(f"Error executing query on {dataset_name}: {e}")

    def query_to_df(self, 
                  dataset_name: str, 
                  query: str, 
                  parameters: Optional[Union[Tuple, Dict[str, Any]]] = None) -> pd.DataFrame:
        """
        Execute a read-only SQL query and return the results as a pandas DataFrame.
        
        Args:
            dataset_name: Name of the dataset as listed in the catalog
            query: SQL query to execute (SELECT only)
            parameters: Optional parameters for the query
            
        Returns:
            pandas.DataFrame: Query results as a DataFrame
            
        Raises:
            KeyError: If dataset_name is not in the catalog
            sqlite3.Error: If there's an error executing the query
            ValueError: If query is not a SELECT statement
        """
        # Ensure this is a read-only operation
        query_upper = query.strip().upper()
        if not query_upper.startswith("SELECT"):
            raise ValueError("Only SELECT queries are allowed in read-only mode")
        
        with self.connection(dataset_name) as conn:
            try:
                if parameters:
                    return pd.read_sql_query(query, conn, params=parameters)
                else:
                    return pd.read_sql_query(query, conn)
            except (sqlite3.Error, pd.io.sql.DatabaseError) as e:
                raise sqlite3.Error(f"Error executing query on {dataset_name}: {e}")
    
    def get_table_names(self, dataset_name: str) -> List[str]:
        """
        Get a list of all tables in the specified dataset.
        
        Args:
            dataset_name: Name of the dataset as listed in the catalog
            
        Returns:
            List of table names in the database
            
        Raises:
            KeyError: If dataset_name is not in the catalog
            sqlite3.Error: If there's an error querying the database
        """
        query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        rows = self.execute_query(dataset_name, query)
        return [row['name'] for row in rows]
    
    def get_table_schema(self, dataset_name: str, table_name: str) -> List[Dict[str, str]]:
        """
        Get the schema for the specified table.
        
        Args:
            dataset_name: Name of the dataset as listed in the catalog
            table_name: Name of the table to get schema for
            
        Returns:
            List of column information dictionaries
            
        Raises:
            KeyError: If dataset_name is not in the catalog
            sqlite3.Error: If there's an error querying the database
        """
        query = f"PRAGMA table_info({table_name})"
        rows = self.execute_query(dataset_name, query)
        return [dict(row) for row in rows]
    
    def get_table_info(self, dataset_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get comprehensive information about a table.
        
        Args:
            dataset_name: Name of the dataset as listed in the catalog
            table_name: Name of the table
            
        Returns:
            Dictionary with table information including:
            - row_count: Number of rows
            - columns: List of column details
            - indexes: List of indexes on the table
            - sample_data: Sample rows (max 5)
            
        Raises:
            KeyError: If dataset_name is not in the catalog
            sqlite3.Error: If there's an error querying the database
        """
        result = {}
        
       # Get column information
        columns = self.get_table_schema(dataset_name, table_name)
        result['columns'] = columns
        
        # Get row count
        count_query = f"SELECT COUNT(*) as count FROM {table_name}"
        count_result = self.execute_query(dataset_name, count_query)
        result['row_count'] = count_result[0]['count']
        
        # Get index information
        index_query = f"PRAGMA index_list({table_name})"
        indexes = self.execute_query(dataset_name, index_query)
        result['indexes'] = [dict(idx) for idx in indexes]
        
        # Get sample data (max 5 rows)
        sample_query = f"SELECT * FROM {table_name} LIMIT 5"
        sample_data = self.execute_query(dataset_name, sample_query)
        result['sample_data'] = [dict(row) for row in sample_data]
        
        return result
        
    
    def get_database_summary(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get a summary of the entire database.
        
        Args:
            dataset_name: Name of the dataset as listed in the catalog
            
        Returns:
            Dictionary with database summary information including:
            - tables: List of table names
            - table_counts: Dictionary mapping table names to row counts
            - foreign_keys: List of foreign key relationships
            
        Raises:
            KeyError: If dataset_name is not in the catalog
            sqlite3.Error: If there's an error querying the database
        """
        result = {}
        
        # Get all tables
        tables = self.get_table_names(dataset_name)
        result['tables'] = tables
        
        # Get row counts for each table
        table_counts = {}
        for table in tables:
            count_query = f"SELECT COUNT(*) as count FROM {table}"
            count_result = self.execute_query(dataset_name, count_query)
            table_counts[table] = count_result[0]['count']
        result['table_counts'] = table_counts
        
        # Get foreign key relationships
        foreign_keys = []
        for table in tables:
            fk_query = f"PRAGMA foreign_key_list({table})"
            fks = self.execute_query(dataset_name, fk_query)
            for fk in fks:
                foreign_keys.append({
                    'table': table,
                    'from_column': fk['from'],
                    'to_table': fk['table'],
                    'to_column': fk['to']
                })
        result['foreign_keys'] = foreign_keys
        
        # Get database metadata
        meta = self.catalog[dataset_name]
        result['metadata'] = {
            'name': meta.name,
            'version': meta.version,
            'format': meta.format,
            'timestamp': meta.timestamp,
            'categories': meta.categories,
            'filepath': str(meta.filepath)
        }
        
        return result
    
    def close_all_connections(self) -> None:
        """
        Close all open database connections.
        
        Should be called when the reader is no longer needed.
        """
        for name, conn in self.connections.items():
            try:
                conn.close()
            except sqlite3.Error:
                pass  # Ignore errors when closing connections
        self.connections.clear()
    




        
    
  
    


    
    




