#!/usr/bin/env python3
"""
SQLite Database dataloader - A dataloader for discovering and loading SQLite databases from XDG directories
"""

import os
import sqlite3
import logging
import xdg.BaseDirectory
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('sqlite_db_dataloader')

class DatasetLoader:
    """dataloader for discovering and loading SQLite databases"""
    
    def __init__(self, app_name: str):
        self.app_name = app_name
        self.databases: Dict[str, str] = {}  # Maps database name to path
        
    def discover_databases(self) -> List[Path]:
        """Discover SQLite database files in XDG data directories"""
        database_paths = []
        
        # Look in all XDG data directories
        data_dirs = xdg.BaseDirectory.load_data_paths(self.app_name)
        
        for data_dir in data_dirs:
            data_dir_path = Path(data_dir)
            
            # Find all potential SQLite database files
            for file_path in data_dir_path.glob('**/*'):
                if file_path.is_file() and self._is_sqlite_database(file_path):
                    database_paths.append(file_path)
        
        logger.info(f"Discovered {len(database_paths)} SQLite databases")
        return database_paths
    
    def _is_sqlite_database(self, file_path: Path) -> bool:
        """Check if a file is a SQLite database"""
        # Check file header for SQLite signature
        try:
            with open(file_path, 'rb') as f:
                header = f.read(16)
                return header.startswith(b'SQLite format 3')
        except Exception:
            return False
        
        return False
    
    def load_databases(self) -> int:
        """Load discovered databases into the dataloader"""
        database_paths = self.discover_databases()
        loaded_count = 0
        
        for path in database_paths:
            db_name = path.stem
            
            # Skip already loaded databases
            if db_name in self.databases:
                logger.debug(f"Skipping already loaded database: {db_name}")
                continue
            
            # Add to our database registry
            self.databases[db_name] = str(path)
            loaded_count += 1
            logger.info(f"Loaded database: {db_name} from {path}")
        
        return loaded_count
    
    def list_databases(self) -> List[str]:
        """List all available databases"""
        return list(self.databases.keys())
    
    def get_database_tables(self, db_name: str) -> List[str]:
        """Get list of tables in a database"""
        if db_name not in self.databases:
            logger.error(f"Database '{db_name}' not found")
            return []
        
        try:
            conn = sqlite3.connect(self.databases[db_name])
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            return tables
        except sqlite3.Error as e:
            logger.error(f"Error accessing database '{db_name}': {str(e)}")
            return []
    
    def query_database(self, db_name: str, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a query against a database"""
        if db_name not in self.databases:
            logger.error(f"Database '{db_name}' not found")
            return []
        
        try:
            conn = sqlite3.connect(self.databases[db_name])
            cursor = conn.execute(query, params or ())
            
            # Get column names
            columns = [description[0] for description in cursor.description]
            
            # Convert to list of dictionaries
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            
            conn.close()
            return results
            
        except sqlite3.Error as e:
            logger.error(f"Query error on database '{db_name}': {str(e)}")
            return []

# Example usage
def main():
    # Initialize dataloader
    dataloader = DatasetLoader('dapper')
    
    # Load all databases
    dataloader.load_databases()
    
    # List available databases
    databases = dataloader.list_databases()
    print(f"Available databases: {databases}")
    
    # If databases are found, show tables and sample data
    if databases:
        sample_db = databases[0]
        tables = dataloader.get_database_tables(sample_db)
        print(f"Tables in '{sample_db}': {tables}")
        
        if tables:
            sample_table = tables[0]
            results = dataloader.query_database(
                sample_db, 
                f"SELECT * FROM {sample_table} LIMIT 5"
            )
            print(f"Sample data from '{sample_db}.{sample_table}':")
            for row in results:
                print(row)

if __name__ == "__main__":
    main()