"""
test_dataset_loader.py - Test suite for the dataset_loader module
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
    if hasattr(dataloader, 'connection') and dataloader.connection:
        dataloader.close()
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
    
    # Create a datasets directory with a database
    datasets_dir = os.path.join(base_dir, 'datasets')
    os.makedirs(datasets_dir, exist_ok=True)
    db4_path = os.path.join(datasets_dir, 'dataset_db.db')
    conn = sqlite3.connect(db4_path)
    conn.execute('CREATE TABLE dataset_table (id INTEGER PRIMARY KEY, data TEXT)')
    conn.execute('INSERT INTO dataset_table VALUES (1, "Dataset Data")')
    conn.commit()
    conn.close()
    db_paths['dataset_db'] = db4_path
    
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
    
    # The fake.db file has the right extension but wrong content
    # Our improved implementation should catch this
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
    
    # Verify real databases were found
    assert db_paths['test_db1'] in discovered_paths, "Should discover standard .db file"
    assert db_paths['dataset_db'] in discovered_paths, "Should discover database in datasets directory"
    
    # Verify only valid databases were found
    fake_db_path = os.path.join(sqlite_test_environment['temp_dir'], 'fake.db')
    assert fake_db_path not in discovered_paths, "Should not discover fake.db file"
    
    text_file_path = os.path.join(sqlite_test_environment['temp_dir'], 'not_a_db.txt')
    assert text_file_path not in discovered_paths, "Should not discover text file"

def test_load_databases(sqlite_test_environment):
    """Test loading discovered databases"""
    dataloader = sqlite_test_environment['dataloader']
    
    # Load databases
    loaded_count = dataloader.load_databases()
    
    # Verify count
    assert loaded_count > 0, "Should load at least one database"
    
    # Verify they're in the dataloader's registry
    assert hasattr(dataloader, 'databases'), "Should have databases attribute"
    assert len(dataloader.databases) > 0, "Should have at least one database in registry"
    assert 'test_db1' in dataloader.databases, "test_db1 should be in registry"
    assert 'dataset_db' in dataloader.databases, "dataset_db should be in registry"
    
    # Test loading again (should not add duplicates)
    second_load_count = dataloader.load_databases()
    assert second_load_count == 0, "Second load should add 0 new databases"
    assert len(dataloader.databases) > 0, "Should still have databases after second load"

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
    assert len(db_list) > 0, "Should list databases after loading"
    assert 'test_db1' in db_list, "test_db1 should be in list"
    assert 'dataset_db' in db_list, "dataset_db should be in list"

def test_get_database_tables(sqlite_test_environment):
    """Test getting tables from a database"""
    dataloader = sqlite_test_environment['dataloader']
    
    # Load databases
    dataloader.load_databases()
    
    # Get tables from test_db1
    tables = dataloader.get_database_tables('test_db1')
    assert 'test_table' in tables, "Should find test_table in test_db1"
    
    # Get tables from dataset_db
    tables = dataloader.get_database_tables('dataset_db')
    assert 'dataset_table' in tables, "Should find dataset_table in dataset_db"
    
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
    
    # Query dataset_db
    results = dataloader.query_database('dataset_db', "SELECT * FROM dataset_table")
    assert len(results) == 1, "Should return 1 row from dataset_table"
    assert results[0]['data'] == 'Dataset Data', "Should return correct data"
    
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
        
        # Create a new DatasetLoader specifically for the resources test
        resource_loader = DatasetLoader(sqlite_test_environment['app_name'])
        
        # Load all discovered databases
        loaded_count = resource_loader.load_databases()
        print(f"Loaded {loaded_count} databases")
        assert loaded_count > 0, "Should load at least one database"
        
        # Get list of loaded databases
        databases = resource_loader.list_databases()
        print(f"Available databases: {databases}")
        
        # There should be at least one database available
        assert len(databases) > 0, "Should have at least one database in the list"
        
        # Test querying from the first database found
        if databases:
            db_name = databases[0]
            tables = resource_loader.get_database_tables(db_name)
            print(f"Tables in {db_name}: {tables}")
            
            if tables:
                first_table = tables[0]
                results = resource_loader.query_database(
                    db_name,
                    f"SELECT * FROM {first_table} LIMIT 3"
                )
                print(f"Sample data from {first_table}:")
                for row in results:
                    print(row)
    finally:
        # Restore original XDG paths
        xdg.BaseDirectory.load_data_paths = original_load_data_paths

def test_xdg_default_path():
    """Test that DatasetLoader uses XDG directories as the default path"""
    import os
    import tempfile
    import xdg.BaseDirectory
    import sqlite3
    import shutil
    from pathlib import Path
    from dapper_python.dataset_loader import DatasetLoader
    
    # Save original XDG functions to restore later
    original_data_home = xdg.BaseDirectory.save_data_path
    original_data_dirs = xdg.BaseDirectory.load_data_paths
    
    try:
        # Create a temporary directory to use as mock XDG data home
        temp_dir = tempfile.mkdtemp()
        
        # Mock the XDG functions to return our temp directory
        def mock_save_data_path(app_name):
            app_dir = os.path.join(temp_dir, app_name)
            os.makedirs(app_dir, exist_ok=True)
            return app_dir
        
        def mock_load_data_paths(app_name):
            return [temp_dir]
        
        xdg.BaseDirectory.save_data_path = mock_save_data_path
        xdg.BaseDirectory.load_data_paths = mock_load_data_paths
        
        # Create a DatasetLoader
        app_name = 'testapp'
        dataloader = DatasetLoader(app_name)
        
        # Expected path in the XDG directory
        expected_db_path = os.path.join(temp_dir, app_name, f"{app_name}.db")
        
        # Test that the DatasetLoader is using the correct path
        assert dataloader.db_path == expected_db_path, f"Expected {expected_db_path}, got {dataloader.db_path}"
        print(f"DatasetLoader is using the correct XDG path: {dataloader.db_path}")
        
        # Create a datasets directory in the temp XDG path
        datasets_dir = os.path.join(temp_dir, 'datasets')
        os.makedirs(datasets_dir, exist_ok=True)
        
        # Create a test SQLite database
        db_path = os.path.join(datasets_dir, 'test.db')
        conn = sqlite3.connect(db_path)
        conn.execute('CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)')
        conn.execute('INSERT INTO test VALUES (1, "Test data")')
        conn.commit()
        conn.close()
        print(f"Created test database at: {db_path}")
        
        # Discover databases in the XDG path
        discovered_dbs = dataloader.discover_databases()
        print(f"Discovered databases: {discovered_dbs}")
        
        # Check that our test database was discovered
        assert len(discovered_dbs) > 0, "Should discover at least one database"
        assert any("test.db" in str(path) for path in discovered_dbs), "Should discover test.db"
        
        # Test loading databases
        loaded_count = dataloader.load_databases()
        print(f"Loaded {loaded_count} databases")
        assert loaded_count > 0, "Should load at least one database"
        
        # Check available databases
        databases = dataloader.list_databases()
        print(f"Available databases: {databases}")
        assert "test" in databases, "Should find 'test' database in the list"
        
    finally:
        # Restore original XDG functions
        xdg.BaseDirectory.save_data_path = original_data_home
        xdg.BaseDirectory.load_data_paths = original_data_dirs
        
        # Clean up temp directory
        shutil.rmtree(temp_dir)

def test_command_line_interface():
    """Test the command line interface of the dataset loader"""
    import subprocess
    import os
    import shutil
    import xdg.BaseDirectory
    from pathlib import Path
    
    # Path to the source database in tests/resources
    source_db = os.path.join(os.path.dirname(__file__), "resources", "NuGet-20200101.db")
    
    # Verify the source database exists
    assert os.path.exists(source_db), f"Source database not found at {source_db}"
    
    # Define test app name
    app_name = 'test_cli_app'
    
    # Find the XDG data directory for the test app
    xdg_data_home = xdg.BaseDirectory.save_data_path(app_name)
    datasets_dir = os.path.join(xdg_data_home, 'datasets')
    
    # Clear any existing test data
    if os.path.exists(datasets_dir):
        shutil.rmtree(datasets_dir)
    os.makedirs(datasets_dir, exist_ok=True)
    
    try:
        # Test the 'add' command
        add_cmd = [
            'python', 
            '-m', 
            'dapper_python.dataset_loader', 
            '--app-name', 
            app_name, 
            'add', 
            source_db, 
            '--name', 
            'test_nuget_db'
        ]
        
        print(f"Executing command: {' '.join(add_cmd)}")
        add_result = subprocess.run(add_cmd, capture_output=True, text=True)
        
        print(f"Command output:")
        print(add_result.stdout)
        if add_result.stderr:
            print(f"Error output:")
            print(add_result.stderr)
        
        # Check the command succeeded
        assert add_result.returncode == 0, "Command failed"
        assert "Successfully added database" in add_result.stdout, "Database wasn't added successfully"
        
        # Verify the database file was copied to the XDG directory
        dest_db_path = os.path.join(datasets_dir, 'test_nuget_db.db')
        assert os.path.exists(dest_db_path), "Database file wasn't copied to XDG directory"
        
        # Test the 'list' command
        list_cmd = [
            'python', 
            '-m', 
            'dapper_python.dataset_loader', 
            '--app-name', 
            app_name, 
            'list'
        ]
        
        print(f"Executing command: {' '.join(list_cmd)}")
        list_result = subprocess.run(list_cmd, capture_output=True, text=True)
        
        print(f"List command output:")
        print(list_result.stdout)
        
        # Check the command succeeded and our database is listed
        assert list_result.returncode == 0, "List command failed"
        assert "test_nuget_db" in list_result.stdout, "Added database not found in list"
        
        # Test the 'info' command
        info_cmd = [
            'python', 
            '-m', 
            'dapper_python.dataset_loader', 
            '--app-name', 
            app_name, 
            'info', 
            'test_nuget_db'
        ]
        
        print(f"Executing command: {' '.join(info_cmd)}")
        info_result = subprocess.run(info_cmd, capture_output=True, text=True)
        
        print(f"Info command output:")
        print(info_result.stdout)
        
        # Check the command succeeded
        assert info_result.returncode == 0, "Info command failed"
        assert "Database: test_nuget_db" in info_result.stdout, "Database info not displayed"
        
        # Test 'remove' command
        remove_cmd = [
            'python', 
            '-m', 
            'dapper_python.dataset_loader', 
            '--app-name', 
            app_name, 
            'remove', 
            'test_nuget_db', 
            '--delete'
        ]
        
        print(f"Executing command: {' '.join(remove_cmd)}")
        remove_result = subprocess.run(remove_cmd, capture_output=True, text=True)
        
        print(f"Remove command output:")
        print(remove_result.stdout)
        
        # Check the command succeeded
        assert remove_result.returncode == 0, "Remove command failed"
        assert "Successfully removed database" in remove_result.stdout, "Database wasn't removed successfully"
        assert not os.path.exists(dest_db_path), "Database file wasn't deleted"
        
        print("Command line interface test passed!")
        
    finally:
        # Clean up
        if os.path.exists(datasets_dir):
            shutil.rmtree(datasets_dir)