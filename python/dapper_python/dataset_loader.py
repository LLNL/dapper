import os
import sys
import platform
import sqlite3
import re
import argparse
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Union
import tomlkit



# Optional dependencies for HuggingFace integration
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False



@dataclass
class DatasetMeta:
    """Dataset metadata matching Rust Dataset struct"""
    version: int  # Changed from str to int to match Rust
    format: str
    timestamp: datetime
    categories: List[str]
    filepath: Path
    # Removed HuggingFace-specific fields to match Rust struct


class DatasetCatalog:
    """Class for managing SQLite databases via dataset_info.toml"""
    
    def __init__(self, 
                 app_name: Optional[str] = "dapper", 
                 file_path: Optional[str] = None,
                 hf_repo_url: Optional[str] = None,
                 auto_discover: bool = False,
                 hf_token: Optional[str] = None):
        
        self.app_name = app_name
        self.hf_repo_url = hf_repo_url
        self.hf_token = hf_token or os.environ.get('HF_TOKEN') or os.environ.get('HUGGINGFACE_TOKEN')
        self.dataset_metas: Dict[str, DatasetMeta] = {}  # Changed to dict for easier lookup
        
        # Always try to load from local dataset_info.toml first
        self._load_from_dataset_info_toml(file_path)
        
        # Auto-discover from Hugging Face if requested and no local data
        if auto_discover and hf_repo_url and not self.dataset_metas:
            print("📭 No local datasets found, attempting auto-discovery...")
            self._discover_and_install_from_huggingface(hf_repo_url)
        elif auto_discover and hf_repo_url:
            print("🔍 Auto-discovery requested - refreshing from HuggingFace...")
            self._discover_and_install_from_huggingface(hf_repo_url)
    
    def _load_from_dataset_info_toml(self, file_path: Optional[str] = None):
        """Load installed datasets from dataset_info.toml"""
        try:
            toml_path = self._find_dataset_info_toml(file_path)
            with open(toml_path, 'r') as f:
                config = tomlkit.load(f)
            
            datasets_dict = config.get("datasets", {})
            for name, dataset_data in datasets_dict.items():
                self.dataset_metas[name] = DatasetMeta(
                    version=int(dataset_data["version"]),
                    format=dataset_data["format"],
                    timestamp=datetime.fromisoformat(dataset_data["timestamp"].replace('Z', '+00:00')),
                    categories=dataset_data["categories"],
                    filepath=Path(dataset_data["filepath"])
                )
            
            print(f"dataset Loaded {len(self.dataset_metas)} datasets from dataset_info.toml")
            
        except FileNotFoundError:
            print("No dataset_info.toml found - starting with empty catalog")
        except Exception as e:
            print(f"Error loading dataset_info.toml: {e}")
    
    def _find_dataset_info_toml(self, file_path: Optional[str] = None) -> Path:
        """Find dataset_info.toml file"""
        if file_path:
            path = Path(file_path)
            if path.is_file():
                return path
            # Check if it's a directory containing dataset_info.toml
            candidate = path / "dataset_info.toml"
            if candidate.exists():
                return candidate
            raise FileNotFoundError(f"Could not find dataset_info.toml at {file_path}")

        # Look in app data directory
        app_dir = Path(self.get_app_data_dir(self.app_name))
        candidate = app_dir / "dataset_info.toml"
        if candidate.exists():
            return candidate

        raise FileNotFoundError(f"Could not find dataset_info.toml in {app_dir}")
    
    def save_to_dataset_info_toml(self, file_path: Optional[str] = None):
        """Save current catalog to dataset_info.toml"""
        if file_path:
            toml_path = Path(file_path)
        else:
            app_dir = Path(self.get_app_data_dir(self.app_name))
            app_dir.mkdir(parents=True, exist_ok=True)
            toml_path = app_dir / "dataset_info.toml"
        
        # Create TOML structure matching Rust format
        config = tomlkit.document()
        config["schema_version"] = 1
        
        datasets_table = tomlkit.table()
        for name, meta in self.dataset_metas.items():
            dataset_table = tomlkit.table()
            dataset_table["version"] = meta.version
            dataset_table["format"] = meta.format
            dataset_table["timestamp"] = meta.timestamp.isoformat().replace('+00:00', 'Z')
            dataset_table["categories"] = meta.categories
            dataset_table["filepath"] = str(meta.filepath)
            datasets_table[name] = dataset_table
        
        config["datasets"] = datasets_table
        
        # Write to file
        with open(toml_path, 'w') as f:
            tomlkit.dump(config, f)
        
        print(f"File Saved catalog to {toml_path}")
    
    def discover_databases(self) -> List[Path]:
        """Get list of installed database files from dataset_info.toml"""
        return [meta.filepath for meta in self.dataset_metas.values()]
    
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
            # Windows: %APPDATA%\\app_name
            appdata = os.environ.get('APPDATA')
            if appdata:
                return os.path.join(appdata, app_name)
            else:
                # Fallback if APPDATA is not defined
                return os.path.join(os.path.expanduser('~'), 'AppData', 'Roaming', app_name)
        
        else:
            # Unknown platform, use a reasonable default
            return os.path.join(os.path.expanduser('~'), f'.{app_name}')
    
    def _discover_and_install_from_huggingface(self, repo_url: str):
        """Discover datasets from HuggingFace and install them to catalog"""
        if not HAS_REQUESTS:
            print("Error: requests library required for HuggingFace integration")
            return
        
        try:
            org_name = repo_url.rstrip('/').split('/')[-1]
            hf_datasets = self._scan_hf_organization(org_name)
            
            if not hf_datasets:
                print("No datasets found in repository")
                return
            
            # Convert discovered datasets to local catalog format
            new_count = 0
            for hf_data in hf_datasets:
                dataset_name = hf_data['name']
                
                # Skip if already exists
                if dataset_name in self.dataset_metas:
                    continue
                
                # Create local dataset entry
                local_filename = hf_data['huggingface_filename']
                local_path = Path(self.get_app_data_dir(self.app_name)) / local_filename
                
                self.dataset_metas[dataset_name] = DatasetMeta(
                    version=1,  # Default version
                    format='sqlite',
                    timestamp=datetime.fromisoformat(hf_data['release_date'].replace('Z', '+00:00')),
                    categories=hf_data['categories'],
                    filepath=local_path
                )
                new_count += 1
            
            if new_count > 0:
                # Save updated catalog to dataset_info.toml
                self.save_to_dataset_info_toml()
                print(f"Added {new_count} datasets to local catalog")
            else:
                print("ℹNo new datasets found")
                
        except Exception as e:
            print(f"Error discovering from HuggingFace: {e}")
    
    def _scan_hf_organization(self, org_name: str) -> List[Dict[str, Any]]:
        """Scan HuggingFace organization for dataset repositories"""
        headers = {'User-Agent': 'DAPper Dataset Scanner/1.0'}
        if self.hf_token:
            headers['Authorization'] = f'Bearer {self.hf_token}'
        
        try:
            print(f"Scanning HuggingFace organization: {org_name}")
            
            # Get all dataset repositories for this organization
            datasets_url = f"https://huggingface.co/api/datasets?author={org_name}"
            response = requests.get(datasets_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            repositories = response.json()
            print(f"Found {len(repositories)} dataset repositories")
            
            all_datasets = []
            
            # For each repository, scan for dataset files
            for repo in repositories:
                repo_id = repo.get('id', '')
                repo_name = repo_id.split('/')[-1] if '/' in repo_id else repo_id
                
                print(f"   🔍 Scanning repository: {repo_name}")
                
                # Get files in this repository
                try:
                    repo_api_url = f"https://huggingface.co/api/datasets/{repo_id}/tree/main"
                    repo_response = requests.get(repo_api_url, headers=headers, timeout=30)
                    repo_response.raise_for_status()
                    
                    files_data = repo_response.json()
                    
                    # Filter for dataset files (NO file globbing, just check extensions)
                    dataset_extensions = ['.db', '.sqlite', '.sqlite3', '.db.gz', '.sqlite.gz']
                    exclude_patterns = ['test', 'sample', 'demo', 'readme', 'license']
                    
                    for file_info in files_data:
                        file_path = file_info.get('path', '')
                        file_name = Path(file_path).name.lower()
                        
                        # Check if it's a dataset file
                        is_dataset = any(file_path.lower().endswith(ext) for ext in dataset_extensions)
                        is_excluded = any(pattern in file_name for pattern in exclude_patterns)
                        
                        if is_dataset and not is_excluded:
                            metadata = self._extract_hf_metadata(file_info, repo_id, org_name)
                            all_datasets.append(metadata)
                            print(f"Filesystem Found dataset: {file_path}")
                
                except Exception as e:
                    print(f"      ⚠️ Error scanning {repo_id}: {e}")
                    continue
            
            print(f"Total datasets discovered: {len(all_datasets)}")
            return all_datasets
            
        except requests.RequestException as e:
            print(f"Error accessing HuggingFace organization: {e}")
            return []
        except Exception as e:
            print(f"Error processing organization data: {e}")
            return []
    
    def _extract_hf_metadata(self, file_info: Dict, repo_id: str, org_name: str) -> Dict[str, Any]:
        """Extract metadata from HuggingFace file info"""
        file_path = file_info.get('path', '')
        file_name = Path(file_path).name
        
        # Handle repo_id which might be "org/repo" or just "repo"
        if '/' in repo_id:
            _, repo_name = repo_id.split('/', 1)
        else:
            repo_name = repo_id
        
        # Generate dataset name combining repo and file
        base_name = Path(file_name).stem
        
        # Remove compression extensions
        if base_name.endswith('.db'):
            base_name = base_name[:-3]
        elif base_name.endswith('.sqlite'):
            base_name = base_name[:-7]
        
        # Create dataset name
        dataset_name = f"{repo_name}_{base_name}".lower()
        dataset_name = re.sub(r'[^a-zA-Z0-9_-]', '_', dataset_name)
        dataset_name = re.sub(r'_+', '_', dataset_name).strip('_')
        
        # Detect categories
        categories = self._detect_categories(file_name.lower(), repo_name.lower())
        
        # Build download URL for later use
        download_url = f"https://huggingface.co/datasets/{repo_id}/resolve/main/{file_path}"
        
        return {
            'name': dataset_name,
            'categories': categories,
            'download_url': download_url,
            'size_mb': round(file_info.get('size', 0) / (1024 * 1024), 1),
            'huggingface_repo': repo_id,
            'huggingface_filename': file_name,
            'file_path': file_path,
            'release_date': file_info.get('lastModified', datetime.now().isoformat() + 'Z')
        }
    
    def _detect_categories(self, filename_lower: str, repo_name_lower: str) -> List[str]:
        """Detect categories from filename and repository name"""
        categories = []
        text_to_check = f"{filename_lower} {repo_name_lower}"
        
        # Package manager categories
        if any(term in text_to_check for term in ['nuget', 'dotnet', 'csharp', '.net']):
            categories.extend(['nuget', 'dotnet', 'csharp', 'packages', 'dev'])
        elif any(term in text_to_check for term in ['npm', 'node', 'javascript']):
            categories.extend(['npm', 'javascript', 'nodejs', 'packages', 'dev'])
        elif any(term in text_to_check for term in ['python', 'pypi', 'pip']):
            categories.extend(['python', 'pypi', 'packages', 'dev'])
        elif any(term in text_to_check for term in ['ubuntu', 'debian']):
            categories.extend(['linux', 'system', 'packages'])
            if 'ubuntu' in text_to_check:
                categories.append('ubuntu')
        
        # Default if none detected
        if not categories:
            categories = ['packages', 'data']
        
        return sorted(list(set(categories)))
    
    def install_dataset(self, dataset_name: str, file_path: Path, 
                       version: int = 1, format: str = "sqlite", 
                       categories: List[str] = None) -> bool:
        """Install a dataset into the catalog"""
        if categories is None:
            categories = ['data']
        
        self.dataset_metas[dataset_name] = DatasetMeta(
            version=version,
            format=format,
            timestamp=datetime.now(timezone.utc),
            categories=categories,
            filepath=file_path
        )
        
        self.save_to_dataset_info_toml()
        print(f"Installed dataset '{dataset_name}' to catalog")
        return True
    
    def download_dataset(self, dataset_name: str) -> bool:
        """Download a dataset that's in the catalog but not on disk"""
        if dataset_name not in self.dataset_metas:
            print(f"Error dataset '{dataset_name}' not found in catalog")
            available = list(self.dataset_metas.keys())
            print(f"Available datasets: {', '.join(available[:5])}")
            return False
        
        dataset = self.dataset_metas[dataset_name]
        
        # Check if already downloaded
        if dataset.filepath.exists():
            print(f"Dataset '{dataset_name}' already exists at {dataset.filepath}")
            return True
        
        # For this implementation, we need to find the download URL
        # This would require storing HF metadata separately or re-discovering
        print(f"Error: Download functionality requires HF URL - use refresh to rediscover")
        return False
    
    def refresh_from_huggingface(self, repo_url: Optional[str] = None) -> bool:
        """Refresh catalog by rediscovering from HuggingFace"""
        repo_url = repo_url or self.hf_repo_url
        if not repo_url:
            print("Error: No HuggingFace repository URL provided")
            return False
        
        self._discover_and_install_from_huggingface(repo_url)
        return True
    
    def list_dataset_names(self) -> List[str]:
        """Return all dataset names in the catalog"""
        return list(self.dataset_metas.keys())
    
    def __len__(self) -> int:
        """Total number of datasets in the catalog"""
        return len(self.dataset_metas)
    
    def __iter__(self):
        """Iterate over DatasetMeta objects"""
        yield from self.dataset_metas.values()

    def __getitem__(self, name: str) -> DatasetMeta:
        """Lookup metadata by dataset name"""
        if name not in self.dataset_metas:
            raise KeyError(f"No dataset called {name!r}")
        return self.dataset_metas[name]
    
    def validate_filepaths(self) -> None:
        """Check that every dataset filepath actually exists on disk"""
        missing = [meta.filepath for meta in self.dataset_metas.values() if not meta.filepath.exists()]
        if missing:
            raise FileNotFoundError(f"Missing database files:\n" +
                                     "\n".join(str(p) for p in missing))
    
    def summary(self) -> None:
        """Print a summary of the dataset catalog"""
        print(f"\n Dataset Catalog Summary ({len(self.dataset_metas)} datasets):")
        print("=" * 80)
        
        for name, meta in self.dataset_metas.items():
            status = "Success" if meta.filepath.exists() else "Error"
            size_info = ""  # Size info not stored in TOML format
            
            print(f"{status} {name:25s} v{meta.version:<4} {meta.format:6s} {size_info}")
            print(f"    Categories: {', '.join(meta.categories)}")
            print(f"    Path: {meta.filepath}")
            print()



class CLI:
    """Command-line interface for dataset management"""
    
    def __init__(self):
        self.parser = self._create_parser()
    
    def _create_parser(self):
        """Create and configure argument parser"""
        parser = argparse.ArgumentParser(description="DAPper Dataset Management CLI")
        
        parser.add_argument("--list-datasets", action="store_true", 
                          help="List installed datasets from dataset_info.toml")
        parser.add_argument("--download-dataset", 
                          help="Download a dataset (requires it to be in catalog)")
        parser.add_argument("--refresh", action="store_true", 
                          help="Discover and add datasets from HuggingFace to catalog")
        parser.add_argument("--repo-url", default="https://huggingface.co/dapper-datasets", 
                          help="Hugging Face repository URL")
        parser.add_argument("--hf-token", 
                          help="Hugging Face token for private repos")
        parser.add_argument("--install-dataset", 
                          help="Install a local dataset file to catalog")
        parser.add_argument("--dataset-file", 
                          help="Path to dataset file for installation")
        parser.add_argument("--dataset-categories", 
                          help="Comma-separated categories for dataset installation")
        
        return parser
    
    def run(self):
        """Execute CLI commands"""
        args = self.parser.parse_args()
        
        try:
            if args.list_datasets:
                self._handle_list_datasets(args)
            elif args.install_dataset:
                self._handle_install_dataset(args)
            elif args.download_dataset:
                self._handle_download_dataset(args)
            elif args.refresh:
                self._handle_refresh(args)
            else:
                self.parser.print_help()
        
        except KeyboardInterrupt:
            print("\n⏸ Operation cancelled by user")
            sys.exit(1)
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)
    
    def _handle_list_datasets(self, args):
        """Handle --list-datasets command"""
        catalog = DatasetCatalog(
            hf_repo_url=args.repo_url,
            auto_discover=False,
            hf_token=args.hf_token
        )
        
        print(f"Dataset catalog from dataset_info.toml")
        catalog.summary()
        
        if len(catalog) == 0:
            print("\n No datasets installed. To add datasets:")
            print(f"   cargo run -- --refresh")
            print(f"   cargo run -- --install-dataset <n> --dataset-file <path>")
        else:
            print(f"\n To discover more datasets:")
            print(f"   cargo run -- --refresh")
    
    def _handle_install_dataset(self, args):
        """Handle --install-dataset command"""
        if not args.dataset_file:
            print("Error: --dataset-file required when installing a dataset")
            sys.exit(1)
        
        dataset_file = Path(args.dataset_file)
        if not dataset_file.exists():
            print(f"Error: Dataset file not found: {dataset_file}")
            sys.exit(1)
        
        categories = []
        if args.dataset_categories:
            categories = [cat.strip() for cat in args.dataset_categories.split(',')]
        
        catalog = DatasetCatalog()
        success = catalog.install_dataset(
            dataset_name=args.install_dataset,
            file_path=dataset_file,
            categories=categories or ['data']
        )
        
        if success:
            print(f"Dataset '{args.install_dataset}' installed successfully")
            catalog.summary()
        else:
            sys.exit(1)
    
    def _handle_download_dataset(self, args):
        """Handle --download-dataset command"""
        catalog = DatasetCatalog()
        success = catalog.download_dataset(args.download_dataset)
        if not success:
            sys.exit(1)
    
    def _handle_refresh(self, args):
        """Handle --refresh command"""
        catalog = DatasetCatalog(hf_token=args.hf_token)
        success = catalog.refresh_from_huggingface(args.repo_url)
        
        if success:
            print("Dataset catalog refreshed successfully")
            catalog.summary()
        else:
            print("Failed to refresh dataset catalog")
            sys.exit(1)


def main():
    """CLI entry point"""
    cli = CLI()
    cli.run()


if __name__ == "__main__":
    main()
