"""
dataset_loader.py - A module for discovering and loading SQLite databases from XDG directories

This module provides both a library interface and a command line interface.
"""

import os
import sys
import sqlite3
import logging
import argparse
import shutil
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
    
    def add_database(self, source_path: str, destination_name: Optional[str] = None) -> bool:
        """Add a database file to the XDG data directory
        
        Args:
            source_path: Path to the source database file
            destination_name: Optional name for the database in the XDG directory
                             If not provided, the original filename will be used
        
        Returns:
            bool: True if the database was successfully added, False otherwise
        """
        try:
            # Check if the source file exists and is a valid SQLite database
            source_path = os.path.abspath(source_path)
            if not os.path.exists(source_path):
                logger.error(f"Source file does not exist: {source_path}")
                return False
            
            if not self._is_sqlite_database(Path(source_path)):
                logger.error(f"Source file is not a valid SQLite database: {source_path}")
                return False
            
            # Get XDG data directory for datasets
            xdg_data_home = xdg.BaseDirectory.save_data_path(self.app_name)
            datasets_dir = os.path.join(xdg_data_home, 'datasets')
            os.makedirs(datasets_dir, exist_ok=True)
            
            # Determine destination filename
            if destination_name:
                # Ensure destination has .db extension
                if not destination_name.lower().endswith('.db'):
                    destination_name = f"{destination_name}.db"
            else:
                # Use original filename
                destination_name = os.path.basename(source_path)
            
            # Create full destination path
            destination_path = os.path.join(datasets_dir, destination_name)
            
            # Copy the database file
            shutil.copy2(source_path, destination_path)
            logger.info(f"Added database from {source_path} to {destination_path}")
            
            # Load the new database
            self.load_databases()
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding database: {str(e)}")
            return False
    
    def remove_database(self, db_name: str, delete_file: bool = False) -> bool:
        """Remove a database from the registry and optionally delete the file
        
        Args:
            db_name: Name of the database to remove
            delete_file: If True, the database file will be deleted
        
        Returns:
            bool: True if the database was successfully removed, False otherwise
        """
        # First load databases to ensure we have the current registry
        self.load_databases()
        
        if db_name not in self.databases:
            logger.error(f"Database '{db_name}' not found")
            return False
        
        try:
            file_path = self.databases[db_name]
            
            # Remove from registry
            del self.databases[db_name]
            logger.info(f"Removed database '{db_name}' from registry")
            
            # Delete file if requested
            if delete_file and os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted database file: {file_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error removing database: {str(e)}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("Database connection closed")
            except sqlite3.Error as e:
                logger.error(f"Error closing database connection: {str(e)}")

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Dataset Loader - Manage SQLite databases in XDG directories')
    
    # Required parameter for app name
    parser.add_argument('--app-name', '-a', type=str, default='myapp',
                        help='Application name for XDG directory lookup')
    
    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List available databases')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add a database to the XDG directory')
    add_parser.add_argument('source', help='Path to the source database file')
    add_parser.add_argument('--name', '-n', help='Name for the database in the XDG directory')
    
    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove a database from the registry')
    remove_parser.add_argument('name', help='Name of the database to remove')
    remove_parser.add_argument('--delete', '-d', action='store_true',
                              help='Delete the database file from the XDG directory')
    
    # Info command
    info_parser = subparsers.add_parser('info', help='Show information about a database')
    info_parser.add_argument('name', help='Name of the database')
    
    # Query command
    query_parser = subparsers.add_parser('query', help='Execute a query against a database')
    query_parser.add_argument('name', help='Name of the database')
    query_parser.add_argument('sql', help='SQL query to execute')
    
    return parser.parse_args()

def main():
    """Main function for command line interface"""
    args = parse_arguments()
    
    # Initialize dataset loader
    loader = DatasetLoader(args.app_name).initialize()
    
    try:
        # Process commands
        if args.command == 'list':
            # Load databases first
            loader.load_databases()
            
            # List available databases
            databases = loader.list_databases()
            if databases:
                print(f"Available databases:")
                for db_name in databases:
                    tables = loader.get_database_tables(db_name)
                    table_count = len(tables)
                    print(f"  - {db_name} ({table_count} tables)")
                    for table in tables:
                        # Get row count
                        results = loader.query_database(db_name, f"SELECT COUNT(*) as count FROM {table}")
                        count = results[0]['count'] if results else 0
                        print(f"      * {table} ({count} rows)")
            else:
                print("No databases available")
        
        elif args.command == 'add':
            # Add a database
            success = loader.add_database(args.source, args.name)
            if success:
                print(f"Successfully added database from {args.source}")
            else:
                print(f"Failed to add database from {args.source}")
        
        elif args.command == 'remove':
            # Remove a database
            success = loader.remove_database(args.name, args.delete)
            if success:
                print(f"Successfully removed database '{args.name}'")
                if args.delete:
                    print("Database file was deleted")
            else:
                print(f"Failed to remove database '{args.name}'")
        
        elif args.command == 'info':
            # Load databases first
            loader.load_databases()
            
            # Show info about a database
            if args.name in loader.databases:
                path = loader.databases[args.name]
                tables = loader.get_database_tables(args.name)
                print(f"Database: {args.name}")
                print(f"Path: {path}")
                print(f"Tables: {len(tables)}")
                for table in tables:
                    # Get row count
                    results = loader.query_database(args.name, f"SELECT COUNT(*) as count FROM {table}")
                    count = results[0]['count'] if results else 0
                    print(f"  - {table} ({count} rows)")
                    
                    # Get column info
                    results = loader.query_database(args.name, f"PRAGMA table_info({table})")
                    print(f"    Columns:")
                    for col in results:
                        print(f"      * {col['name']} ({col['type']})")
            else:
                print(f"Database '{args.name}' not found")
        
        elif args.command == 'query':
            # Load databases first
            loader.load_databases()
            
            # Execute a query
            if args.name in loader.databases:
                results = loader.query_database(args.name, args.sql)
                if results:
                    # Print column headers
                    columns = list(results[0].keys())
                    header = ' | '.join(columns)
                    separator = '-' * len(header)
                    print(header)
                    print(separator)
                    
                    # Print rows
                    for row in results:
                        values = [str(row[col]) for col in columns]
                        print(' | '.join(values))
                    
                    print(f"\n{len(results)} rows returned")
                else:
                    print("No results returned")
            else:
                print(f"Database '{args.name}' not found")
        
        else:
            # No command specified, show help
            print("No command specified. Use --help for usage information.")
    
    finally:
        # Clean up
        loader.close()

if __name__ == "__main__":
    main()