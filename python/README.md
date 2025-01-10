# dapper-python

dapper-python is a Python package for working with DAPper datasets. It provides helper functions for normalizing shared library file names similar to the Rust implementation in the DAPper project, and other methods for helping developers access the DAPper datasets.

## Installation

You can install the `dapper-python` package from PyPI using pip:

```bash
pip install dapper-python
```

## Usage

Here is an example of how to use the `dapper-python` package:

```python
from dapper_python.normalize import normalize_file_name

# Example usage
file_name = "libexample-1.2.3.so.1.2"
normalized_name = normalize_file_name(file_name)
print(normalized_name)
```

## Tests

The `dapper-python` package includes tests to help ensure the normalization function matches the Rust implementation.

You can run the tests using the following command:

```bash
python -m pytest
```

## License

DAPper is released under the MIT license. See the [LICENSE](../LICENSE)
and [NOTICE](../NOTICE) files for details. All new contributions must be made
under this license.

SPDX-License-Identifier: MIT

LLNL-CODE-871441
