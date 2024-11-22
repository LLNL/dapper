# DAPper

DAPper helps identify the software packages installed on a system, and expose implicit dependencies in source code.

The main tool for end users parses source code to determine packages that a C/C++ codebase depends.
In addition, datasets mapping file names to packages that install them for various ecosystems are provided.
The tools used to create those datasets are also available in this repository.

## Getting Started

### Installation

For now, the main way to install DAPper is compiling from source.

1. Clone DAPper

```bash
git clone git@github.com:LLNL/dapper.git
```

2. Compile DAPper:

```bash
cargo build
```

3. Run DAPper:

```bash
cargo run <source code directory or file>
```

### Usage

Run `./dapper <source code directory or file>`. The output will be the #included files from each C/C++ source code file found.

## Support

Full user guides for DAPper are available [online](https://dapr.readthedocs.io) and in the [docs](./docs) directory.

For questions or support, please create a new discussion on [GitHub Discussions](https://github.com/LLNL/dapper/discussions/categories/q-a), or [open an issue](https://github.com/LLNL/dapper/issues/new/choose) for bug reports and feature requests.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

For more information on contributing see the [CONTRIBUTING](./CONTRIBUTING.md) file.

## License

DAPper is released under the MIT license. See the [LICENSE](./LICENSE)
and [NOTICE](./NOTICE) files for details. All new contributions must be made
under this license.

SPDX-License-Identifier: MIT

LLNL-CODE-871441
