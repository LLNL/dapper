import platform
import os
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import tomlkit
import sqlite3


@dataclass
class DatasetMeta:
    """Dataset metadata matching Rust Dataset struct"""

    version: int
    format: str
    timestamp: datetime
    categories: List[str]
    filepath: Path


class DatasetCatalog:
    """Class for managing SQLite databases via dataset_info.toml"""

    def __init__(self, app_name: Optional[str] = "dapper", file_path: Optional[str] = None):
        self.app_name = app_name
        self.dataset_metas: Dict[str, DatasetMeta] = {}

        self._load_from_dataset_info_toml(file_path)

    def _load_from_dataset_info_toml(self, file_path: Optional[str] = None):
        """Load installed datasets from dataset_info.toml"""
        try:
            toml_path = self._find_dataset_info_toml(file_path)
            with open(toml_path, "r") as f:
                config = tomlkit.load(f)

            datasets_dict = config.get("datasets", {})
            for name, dataset_data in datasets_dict.items():
                self.dataset_metas[name] = DatasetMeta(
                    version=int(dataset_data["version"]),
                    format=dataset_data["format"],
                    timestamp=datetime.fromisoformat(
                        dataset_data["timestamp"].replace("Z", "+00:00")
                    ),
                    categories=dataset_data["categories"],
                    filepath=Path(dataset_data["filepath"]),
                )

            print(f"dataset Loaded {len(self.dataset_metas)} datasets from dataset_info.toml")

        except FileNotFoundError:
            print("No dataset_info.toml found - starting with empty catalog")
        except Exception as e:
            print(f"Error loading dataset_info.toml: {e}")

    def _find_dataset_info_toml(self, file_path: Optional[str] = None) -> Path:
        if file_path:
            # If directory provided, append filename
            path = Path(file_path)
            if path.is_dir():
                candidate = path / "dataset_info.toml"
                if candidate.exists():
                    return candidate
            # If file provided directly
            elif path.is_file():
                return path
            raise FileNotFoundError(f"Could not find dataset_info.toml at {file_path}")

        # Default: look in current directory first, then app data
        current_dir = Path(".") / "dataset_info.toml"
        if current_dir.exists():
            return current_dir

        # Fallback to app data directory
        app_dir = Path(self.get_app_data_dir(self.app_name))
        candidate = app_dir / "dataset_info.toml"
        if candidate.exists():
            return candidate

        raise FileNotFoundError("Could not find dataset_info.toml")

    @staticmethod
    def get_app_data_dir(app_name: Optional[str] = "dapper") -> str:
        """Get the platform-specific application data directory"""

        system = platform.system()

        if system == "Linux":
            xdg_data_home = os.environ.get("XDG_DATA_HOME")
            if xdg_data_home:
                return os.path.join(xdg_data_home, app_name)
            else:
                return os.path.join(os.path.expanduser("~"), ".local", "share", app_name)

        elif system == "Darwin":
            return os.path.join(os.path.expanduser("~"), "Library", "Application Support", app_name)

        elif system == "Windows":
            appdata = os.environ.get("LOCALAPPDATA")
            if appdata:
                return os.path.join(appdata, app_name, "data")
            else:
                return os.path.join(os.path.expanduser("~"), "AppData", "Local", app_name, "data")

        else:
            return os.path.join(os.path.expanduser("~"), f".{app_name}")

    def get_available_datasets(self, category: Optional[str] = None) -> List[str]:
        """Return list of dataset names, optionally filtered by category"""
        if not category:
            return list(self.dataset_metas.keys())
        return [name for name, meta in self.dataset_metas.items() if category in meta.categories]

    def get_dataset_path(self, dataset_name: str) -> Optional[Path]:
        """Get path to dataset file for loading/querying"""
        if dataset_name in self.dataset_metas:
            return self.dataset_metas[dataset_name].filepath
        return None

    def get_dataset_info(self, dataset_name: str) -> Optional[DatasetMeta]:
        """Get full metadata for a dataset"""
        return self.dataset_metas.get(dataset_name)

    def load_dataset(self, dataset_name: str) -> sqlite3.Connection:
        """Load/open a dataset database for READ-ONLY querying"""
        db_path = self.get_dataset_path(dataset_name)
        if not db_path or not db_path.exists():
            raise FileNotFoundError(f"Dataset '{dataset_name}' not found")

        # Open in read-only mode
        uri = f"file:{db_path}?mode=ro"
        return sqlite3.connect(uri, uri=True)
