#!/usr/bin/env python3
"""
test_sqlite_db_dataloader.py - Test suite for the SQLite database dataloader
"""

import os
import sys
import tempfile
import sqlite3
import shutil
import pytest
from pathlib import Path

# Add parent directory to path to import the module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dapper_python.dataset_loader import DatasetLoader

@pytest.fixture
def sqlite_test_environment():
    """Create a test environment with SQLite databases"""
    # Create temporary directory for XDG data
    temp_dir = tempfile.mkdtemp()
    app_name = 'testapp'
    
    # Mock XDG base directory
    import xdg.BaseDirectory
    original_data_dirs = xdg.BaseDirectory.load_data_paths
    xdg.BaseDirectory.load_data_paths = lambda app_name: [temp_dir]
    
    # Create test databases
    db_paths = create_test_databases(temp_dir)
    
    # Initialize dataloader
    dataloader = DatasetLoader(app_name)
    
    # Return test environment
    yield {
        'temp_dir': temp_dir,
        'app_name': app_name,
        'db_paths': db_paths,
        'dataloader': dataloader
    }
    
    # Clean up
    xdg.BaseDirectory.load_data_paths = original_data_dirs
    shutil.rmtree(temp_dir)

def create_test_databases(base_dir):
    """Create test SQLite databases and non-database files"""
    db_paths = {}
    
    # Create a valid SQLite database
    db1_path = os.path.join(base_dir, 'test_db1.db')
    conn = sqlite3.connect(db1_path)
    conn.execute('CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)')
    conn.execute('INSERT INTO test_table VALUES (1, "Test 1")')
    conn.execute('INSERT INTO test_table VALUES (2, "Test 2")')
    conn.commit()
    conn.close()
    db_paths['test_db1'] = db1_path
    
    # Create another valid SQLite database with non-standard extension
    db2_path = os.path.join(base_dir, 'test_db2.custom')
    conn = sqlite3.connect(db2_path)
    conn.execute('CREATE TABLE another_table (id INTEGER PRIMARY KEY, value REAL)')
    conn.execute('INSERT INTO another_table VALUES (1, 10.5)')
    conn.commit()
    conn.close()
    db_paths['test_db2'] = db2_path
    
    # Create a nested directory with a database
    nested_dir = os.path.join(base_dir, 'nested')
    os.makedirs(nested_dir, exist_ok=True)
    db3_path = os.path.join(nested_dir, 'nested_db.db')
    conn = sqlite3.connect(db3_path)
    conn.execute('CREATE TABLE nested_table (id INTEGER PRIMARY KEY)')
    conn.commit()
    conn.close()
    db_paths['nested_db'] = db3_path
    
    # Create a text file (should be ignored)
    text_path = os.path.join(base_dir, 'not_a_db.txt')
    with open(text_path, 'w') as f:
        f.write("This is a text file, not a database")
    
    # Create a file with .db extension but not a SQLite database
    fake_db_path = os.path.join(base_dir, 'fake.db')
    with open(fake_db_path, 'w') as f:
        f.write("This looks like a database but isn't")
    
    return db_paths

def test_is_sqlite_database(sqlite_test_environment):
    """Test SQLite database detection logic"""
    dataloader = sqlite_test_environment['dataloader']
    db_paths = sqlite_test_environment['db_paths']
    temp_dir = sqlite_test_environment['temp_dir']
    
    # Test valid databases
    assert dataloader._is_sqlite_database(Path(db_paths['test_db1'])), "Should identify .db file as SQLite database"
    assert dataloader._is_sqlite_database(Path(db_paths['test_db2'])), "Should identify custom extension file as SQLite database"
    assert dataloader._is_sqlite_database(Path(db_paths['nested_db'])), "Should identify nested database file"
    
    # Test non-database files
    assert not dataloader._is_sqlite_database(Path(os.path.join(temp_dir, 'not_a_db.txt'))), "Should not identify text file as database"
    assert not dataloader._is_sqlite_database(Path(os.path.join(temp_dir, 'fake.db'))), "Should not identify fake .db file as database"
    
    # Test non-existent file
    assert not dataloader._is_sqlite_database(Path(os.path.join(temp_dir, 'does_not_exist.db'))), "Should not identify non-existent file as database"

def test_discover_databases(sqlite_test_environment):
    """Test database discovery functionality"""
    dataloader = sqlite_test_environment['dataloader']
    db_paths = sqlite_test_environment['db_paths']
    
    # Run discovery
    discovered_dbs = dataloader.discover_databases()
    
    # Convert paths to strings for easier comparison
    discovered_paths = [str(path) for path in discovered_dbs]
    
    # Verify all real databases were found
    assert db_paths['test_db1'] in discovered_paths, "Should discover standard .db file"
    assert db_paths['test_db2'] in discovered_paths, "Should discover database with custom extension"
    assert db_paths['nested_db'] in discovered_paths, "Should discover database in nested directory"
    
    # Verify only real databases were found (not text or fake db files)
    assert len(discovered_dbs) == 3, "Should discover exactly 3 databases"

def test_load_databases(sqlite_test_environment):
    """Test loading discovered databases"""
    dataloader = sqlite_test_environment['dataloader']
    
    # Load databases
    loaded_count = dataloader.load_databases()
    
    # Verify count
    assert loaded_count == 3, "Should load 3 databases"
    
    # Verify they're in the dataloader's registry
    assert len(dataloader.databases) == 3, "Should have 3 databases in registry"
    assert 'test_db1' in dataloader.databases, "test_db1 should be in registry"
    assert 'test_db2' in dataloader.databases, "test_db2 should be in registry"
    assert 'nested_db' in dataloader.databases, "nested_db should be in registry"
    
    # Test loading again (should not add duplicates)
    second_load_count = dataloader.load_databases()
    assert second_load_count == 0, "Second load should add 0 new databases"
    assert len(dataloader.databases) == 3, "Should still have 3 databases after second load"

def test_list_databases(sqlite_test_environment):
    """Test listing available databases"""
    dataloader = sqlite_test_environment['dataloader']
    
    # Before loading any databases
    initial_list = dataloader.list_databases()
    assert len(initial_list) == 0, "Should list 0 databases before loading"
    
    # Load databases
    dataloader.load_databases()
    
    # After loading
    db_list = dataloader.list_databases()
    assert len(db_list) == 3, "Should list 3 databases after loading"
    assert 'test_db1' in db_list, "test_db1 should be in list"
    assert 'test_db2' in db_list, "test_db2 should be in list"
    assert 'nested_db' in db_list, "nested_db should be in list"

def test_get_database_tables(sqlite_test_environment):
    """Test getting tables from a database"""
    dataloader = sqlite_test_environment['dataloader']
    
    # Load databases
    dataloader.load_databases()
    
    # Get tables from test_db1
    tables = dataloader.get_database_tables('test_db1')
    assert 'test_table' in tables, "Should find test_table in test_db1"
    
    # Get tables from test_db2
    tables = dataloader.get_database_tables('test_db2')
    assert 'another_table' in tables, "Should find another_table in test_db2"
    
    # Get tables from non-existent database
    tables = dataloader.get_database_tables('non_existent')
    assert len(tables) == 0, "Should return empty list for non-existent database"

def test_query_database(sqlite_test_environment):
    """Test querying a database"""
    dataloader = sqlite_test_environment['dataloader']
    
    # Load databases
    dataloader.load_databases()
    
    # Query test_db1
    results = dataloader.query_database('test_db1', "SELECT * FROM test_table")
    assert len(results) == 2, "Should return 2 rows from test_table"
    assert results[0]['name'] == 'Test 1', "First row should have name 'Test 1'"
    assert results[1]['name'] == 'Test 2', "Second row should have name 'Test 2'"
    
    # Query with filter
    results = dataloader.query_database('test_db1', "SELECT * FROM test_table WHERE id = ?", (1,))
    assert len(results) == 1, "Should return 1 row with filter"
    assert results[0]['id'] == 1, "Should return row with id=1"
    
    # Query test_db2
    results = dataloader.query_database('test_db2', "SELECT * FROM another_table")
    assert len(results) == 1, "Should return 1 row from another_table"
    assert results[0]['value'] == 10.5, "Should return correct value"
    
    # Query non-existent database
    results = dataloader.query_database('non_existent', "SELECT 1")
    assert len(results) == 0, "Should return empty list for non-existent database"
    
    # Query with invalid SQL
    results = dataloader.query_database('test_db1', "SELECT * FROM non_existent_table")
    assert len(results) == 0, "Should return empty list for invalid query"

def test_load_resource_databases(sqlite_test_environment):
    """Test loading any SQLite databases present in the resources directory"""
    # Import required modules at the function level
    import xdg.BaseDirectory
    from pathlib import Path
    
    dataloader = sqlite_test_environment['dataloader']
    
    # Path to the resources directory
    resources_dir = os.path.join(os.path.dirname(__file__), "resources")
    
    # Verify the resources directory exists
    assert os.path.exists(resources_dir), f"Resources directory not found at {resources_dir}"
    
    # Temporarily redirect XDG to include the resources directory
    original_load_data_paths = xdg.BaseDirectory.load_data_paths
    try:
        # Mock the XDG function to return our resources directory
        xdg.BaseDirectory.load_data_paths = lambda app_name: [resources_dir]
        
        # Discover databases in the resources directory
        discovered_dbs = dataloader.discover_databases()
        print(f"Discovered databases in resources: {discovered_dbs}")
        
        # Verify at least one database was discovered
        assert len(discovered_dbs) > 0, "Should discover at least one database in resources directory"
        
        # Load all discovered databases
        loaded_count = dataloader.load_databases()
        print(f"Loaded {loaded_count} databases")
        assert loaded_count > 0, "Should load at least one database"
        
        # Get list of loaded databases
        databases = dataloader.list_databases()
        print(f"Available databases: {databases}")
        assert len(databases) > 0, "Should have at least one database in the list"
        
        # Test each loaded database
        for db_name in databases:
            print(f"\nTesting database: {db_name}")
            
            # Get tables from the database
            tables = dataloader.get_database_tables(db_name)
            print(f"Tables in {db_name}: {tables}")
            
            # Test query functionality on each table
            for table in tables:
                print(f"Examining table: {table}")
                
                # Get a count of rows
                count_results = dataloader.query_database(
                    db_name,
                    f"SELECT COUNT(*) as count FROM {table}"
                )
                
                if count_results and 'count' in count_results[0]:
                    count = count_results[0]['count']
                    print(f"Table {table} has {count} rows")
                    
                    # If there's data, retrieve a sample
                    if count > 0:
                        sample_results = dataloader.query_database(
                            db_name,
                            f"SELECT * FROM {table} LIMIT 3"
                        )
                        print(f"Sample data from {table}:")
                        for row in sample_results:
                            print(row)
    finally:
        # Always restore the original XDG paths
        xdg.BaseDirectory.load_data_paths = original_load_data_paths