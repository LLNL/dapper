# Getting Started

## System Prerequisites

DAPper is written in Rust and requires the Rust toolchain. It is recommended to use the latest stable Rust version (1.88.0 or newer). You can install through your package manager or through [rustup.rs](https://www.rust-lang.org/tools/install).

## Dataset Setup
DAPper uses pre-built datasets that map file names to packages for various ecosystems. These datasets are hosted on Hugging Face and need to be downloaded and placed in the appropriate XDG directory.

### Available Datasets
The following datasets are available at https://huggingface.co/dapper-datasets:

- dapper-datasets/pypi - Python Package Index mappings
- dapper-datasets/debian-bookworm - Debian Bookworm packages
- dapper-datasets/debian-bullseye - Debian Bullseye packages
- dapper-datasets/debian-buster - Debian Buster packages
- dapper-datasets/ubuntu-noble - Ubuntu Noble packages
- dapper-datasets/ubuntu-jammy - Ubuntu Jammy packages
- dapper-datasets/ubuntu-focal - Ubuntu Focal packages
- dapper-datasets/NuGet-dataset - .NET NuGet packages

### Automated Dataset Set up
1. List available datasets
   ```bash
   cargo run -- . --list-datasets
   ```
2. Install datasets

   - A) Install a dataset 
   ```bash
   cargo run -- . --install <dataset_name>
   ```
   - B) Install all available datasets
   ```bash
   cargo run -- . --install all
   ```
3. Update datasets
   - A) Update a dataset
   ```bash
   cargo run -- . --update <dataset_name>
   ```
   - B) Update all datasets
   ```bash
   cargo run -- . --update all
   ```
4. Uninstall a dataset
   ```bash
   cargo run -- . --uninstall <dataset_name>
   ```


### Manual Dataset Set up 
1. Create data directory: 
Linux/macOS:
```bash
mkdir -p ${XDG_DATA_HOME:-$HOME/Library/Application Support}/dapper/
```
Windows (Command Prompt):
```bash
mkdir "%LOCALAPPDATA%\dapper\"
```
2. Download the datasets into the DAPper folder


## Installation

### For Users:
#### Cargo Install (Recommended for users):
This installs DAPper without needing to clone the repository

```bash
cargo install dapper@<version>
```
#### Build from Source (Currently supported):

For ease of use, we recommend using [rustup.rs](https://www.rust-lang.org/tools/install) which is a Rust installer and version management tool. Install `rustup` by following [their installation instructions](https://www.rust-lang.org/tools/install).

1. Clone the DAPper git repository using `git`

```bash
git clone https://github.com/LLNL/dapper.git
cd dapper
``` 

2. Build DAPper

```bash
cargo build --release
```

3. Run DAPper

```bash
# Using cargo
cargo run <source code directory or file>
```

#### Precompiled Binaries (Coming soon):
We plan to provide precompiled binaries on the GitHub releases page for easier installation across different platforms. 

### Installation Verification

To verify your installation works correctly:

1. Create a simple test C++ file OR use a test file from the repository `dapper/tests/test_files`:
   ```cpp
   // test.cpp
   #include <iostream>
   #include <vector>
   
   int main() {
       std::cout << "Hello World" << std::endl;
       return 0;
   }
   ```

2. Run DAPper on the test file:
   ```bash
   cargo run <test.cpp>
   ```

3. You should see output showing possible implicit dependencies within the source code. In the example, the output will be the #included files `vector` and `iostream` from `test.cpp`.



### For Developers:
1. Clone with development branch (if available):
   ```bash
   git clone https://github.com/LLNL/dapper.git
   cd dapper
   ```

2. Install development formatting and linting tools:
   ```bash
   # Install rustfmt for code formatting
   rustup component add rustfmt
   
   # Install clippy for linting
   rustup component add clippy
   ```

3. Build in debug mode:
   ```bash
   cargo build
   ```

4. Run tests:
   ```bash
   cargo test
   ```

5. Run DAPper

```bash
# Using cargo
cargo run <source code directory or file>
```

### Development Workflow

- **Format code:** `cargo fmt`
- **Lint code:** `cargo clippy`
- **Run in debug mode:** `cargo run <arguments>`

