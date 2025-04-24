"""
dataset_loader.py - A module for discovering and loading SQLite databases from XDG directories
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
logger = logging.getLogger('dataset_loader')

class DatasetLoader:
    """Class for discovering and loading SQLite databases"""
    
    def __init__(self, app_name: str, db_path: Optional[str] = None):
        """Initialize the DatasetLoader.
        
        Args:
            app_name: The application name used for XDG directory lookup
            db_path: Optional path to a specific database file. If None,
                     databases will be discovered in XDG directories
        """
        self.app_name = app_name
        self.connection = None
        self.db_path = db_path
        self.databases = {}  # Maps database name to path
        
        # If no specific db_path is provided, use default in XDG directory
        if self.db_path is None:
            try:
                # Get primary XDG data directory for the app
                xdg_data_home = xdg.BaseDirectory.save_data_path(app_name)
                # Use a default database file in the XDG data directory
                self.db_path = os.path.join(xdg_data_home, f"{app_name}.db")
            except Exception as e:
                logger.warning(f"Could not get XDG data path: {str(e)}")
                # Fallback to a local path
                self.db_path = f"{app_name}.db"
    
    def initialize(self):
        """Initialize the database connection"""
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
            
            # Connect to the database
            self.connection = sqlite3.connect(self.db_path)
            
            # Create metadata table if it doesn't exist
            self.connection.execute('''
                CREATE TABLE IF NOT EXISTS _dataset_metadata (
                    name TEXT PRIMARY KEY,
                    table_name TEXT,
                    source_path TEXT,
                    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            self.connection.commit()
            
            # Load existing metadata
            cursor = self.connection.execute('SELECT name, table_name FROM _dataset_metadata')
            self.databases = {row[0]: row[1] for row in cursor.fetchall()}
            
            logger.info(f"Initialized database at {self.db_path}")
            return self
        except sqlite3.Error as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise
    
    def _is_sqlite_database(self, file_path: Path) -> bool:
        """Check if a file is a SQLite database"""
        # First check file extension as a quick filter
        sqlite_extensions = ['.db', '.sqlite', '.sqlite3', '.db3']
        
        if file_path.suffix.lower() in sqlite_extensions:
            # For files with SQLite extensions, verify they have the SQLite header
            try:
                with open(file_path, 'rb') as f:
                    header = f.read(16)
                    return header.startswith(b'SQLite format 3')
            except Exception:
                return False
        
        # For files without standard SQLite extensions, check header anyway
        else:
            try:
                with open(file_path, 'rb') as f:
                    header = f.read(16)
                    return header.startswith(b'SQLite format 3')
            except Exception:
                return False
        
        return False
    
    def discover_databases(self) -> List[Path]:
        """Discover SQLite database files in XDG data directories"""
        database_paths = []
        
        # Look in all XDG data directories
        try:
            data_dirs = xdg.BaseDirectory.load_data_paths(self.app_name)
            
            # Add current database if it exists and is valid
            if self.db_path and os.path.exists(self.db_path) and self._is_sqlite_database(Path(self.db_path)):
                database_paths.append(Path(self.db_path))
            
            datasets_dir_name = 'datasets'
            
            for data_dir in data_dirs:
                data_dir_path = Path(data_dir)
                
                # Look in datasets directory if it exists
                datasets_dir = data_dir_path / datasets_dir_name
                if datasets_dir.exists() and datasets_dir.is_dir():
                    # Find all potential SQLite database files
                    for file_path in datasets_dir.glob('**/*'):
                        if file_path.is_file() and self._is_sqlite_database(file_path):
                            database_paths.append(file_path)
                
                # Also check the data directory itself for .db files
                for file_path in data_dir_path.glob('*.db'):
                    if file_path.is_file() and self._is_sqlite_database(file_path):
                        database_paths.append(file_path)
                
        except Exception as e:
            logger.error(f"Error discovering databases: {str(e)}")
        
        logger.info(f"Discovered {len(database_paths)} SQLite databases")
        return database_paths
    
    def load_databases(self) -> int:
        """Load discovered databases into the dataloader"""
        database_paths = self.discover_databases()
        loaded_count = 0
        
        for path in database_paths:
            db_name = path.stem
            
            # Skip already loaded databases
            if db_name in self.databases:
                logger.debug(f"Database {db_name} already loaded.")
                continue
            
            # Add database to registry
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
    
    def close(self):
        """Close database connection"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed")
            except sqlite3.Error as e:
                logger.error(f"Error closing database connection: {str(e)}")

# Example usage
def main():
    # Initialize dataset loader
    loader = DatasetLoader('myapp').initialize()
    
    # Load all databases
    loader.load_databases()
    
    # List available databases
    databases = loader.list_databases()
    print(f"Available databases: {databases}")
    
    # If databases are found, show tables and sample data
    if databases:
        sample_db = databases[0]
        tables = loader.get_database_tables(sample_db)
        print(f"Tables in '{sample_db}': {tables}")
        
        if tables:
            sample_table = tables[0]
            results = loader.query_database(
                sample_db, 
                f"SELECT * FROM {sample_table} LIMIT 5"
            )
            print(f"Sample data from '{sample_db}.{sample_table}':")
            for row in results:
                print(row)
    
    # Clean up
    loader.close()

if __name__ == "__main__":
    main()