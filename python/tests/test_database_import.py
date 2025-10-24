import pytest


def test_database_import():
    """Test that the Database class can be imported successfully"""
    from dapper_python.databases.database import Database

    assert Database is not None


def test_paramspec_import():
    """Test that ParamSpec is imported and available"""
    from dapper_python.databases.database import P

    assert P is not None
    # Verify it's a ParamSpec by checking its string representation
    assert "P" in str(P)


def test_database_subclasses_import():
    """Test that all database subclasses can be imported"""
    from dapper_python.databases.linuxDB import LinuxDB
    from dapper_python.databases.nugetDB import NuGetDB
    from dapper_python.databases.pythonDB import PythonDB

    assert LinuxDB is not None
    assert NuGetDB is not None
    assert PythonDB is not None
