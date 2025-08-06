# Basic Usage Guide

DAPper (Dependency Analysis Project) is a tool that discovers compile or build time dependencies based on header files. This guide covers the basic usage patterns and common workflows.

## Overview

DAPper helps you:
- **Discover header dependencies**: Find all `#include` statements in your C/C++ code and map header files to the packages that provide them.
- **Identify system packages**: Can help determine the package that most likely provided a given filename.
- **Analyze large codebases**: Process entire directories recursively (e.g., analyze complex software projects spanning multiple subdirectories and components)
- **Understand implicit dependencies**: Reveal dependencies that aren't immediately obvious, such as spawning a subprocess. 

## Command Line Interface

### Basic Syntax

```bash
# Analyze a single file
dapper <source_file>

# Analyze a directory
dapper <source_directory>

# Using cargo 
cargo run -- <source file or directory>
```

## Understanding the Output

### Basic Output Format

DAPper outputs the `#include` files found in each C/C++ source file. Here's what to expect:

**Example input file (`test.cpp`):**
```cpp
#include <iostream>
#include <nlohmann/json_fwd.hpp>
#include "test.h"

using namespace std; 

int main(int argc, char* argv[])
{
    cout << "Hello World!" << endl;
    return 0;
}
```

**Example DAPper output:**
```
CPP(UserInclude("test.h")):
        ["libcgal-dev", "asterisk-dev", "libboost1.67-dev", "libcxxtools-dev", "libwolfssl-dev", ...]

CPP(SystemInclude("nlohmann/json_fwd.hpp")):
        ["nlohmann-json3-dev", "paraview-dev", "libqgis-dev"]
```

### Output Interpretation

- Components:
    - **CPP**: Indicates this is a C++ source file being analyzed
    - **UserInclude("test.h")**: Shows DAPper found a local/user include directive #include "test.h". Since it is a user include file it is likely just a local header file in the project but DAPper still checks for matches. Since this is a pretty general file name it seems to appear in many packages.
    - **SystemInclude("nlohmann/json_fwd.hpp")**: Shows DAPper found a system include directive #include <nlohmann/json_fwd.hpp>. This means the user is including code from third party libraries, which appears in multiple system packages.
    - **[package list]**: Array of Debian packages that potentially provide files named "test.h" or "nlohmann/json_fwd.hpp".
    > Note: output depends on the datasets a user has downloaded


