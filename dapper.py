# Copyright 2024 Lawrence Livermore National Security, LLC
# See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

# To use, must pip install tree_sitter and tree_sitter_cpp
import os
import sys
from multiprocessing import Pool, cpu_count
from tree_sitter import Language, Parser
import tree_sitter_cpp as tscpp

# Load the C++ language grammar for tree-sitter
CPP_LANGUAGE = Language(tscpp.language())

def is_source_code(file_path):
    extensions = {"h", "hpp", "c", "cc", "hh", "cpp", "h++", "c++", "cxx", "hxx", "ixx", "cppm", "ccm", "c++m", "cxxm"}
    _, ext = os.path.splitext(file_path)
    return ext[1:].lower() in extensions

def extract_includes(file_path):
    parser = Parser(CPP_LANGUAGE)

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            source_code = file.read()
    except:
        print(f"====FAILED ({file_path})====")
        return []

    tree = parser.parse(bytes(source_code, 'utf8'))
    root_node = tree.root_node

    query = CPP_LANGUAGE.query("""
    (preproc_include
        (system_lib_string) @system_include
    )
    (preproc_include
        (string_literal) @user_include
    )
    """)

    captures = query.captures(root_node)
    results = []
    for capture_name, nodes in captures.items():
        for node in nodes:
            include_name = source_code[node.start_byte + 1:node.end_byte - 1]
            results.append(f"{capture_name}: {include_name}")
    return results

def process_file(file_path):
    if is_source_code(file_path):
        print(f"Processing {file_path}")
        includes = extract_includes(file_path)
        return (file_path, includes)
    return None

def main():
    args = sys.argv
    arg_path = args[1]

    file_paths = []
    if os.path.isfile(arg_path):
        file_paths.append(arg_path)
    elif os.path.isdir(arg_path):
        for root, _, files in os.walk(arg_path):
            for file in files:
                file_path = os.path.join(root, file)
                if is_source_code(file_path):
                    file_paths.append(file_path)

    # Use a pool of workers to process files in parallel
    with Pool(processes=cpu_count()) as pool:
        results = pool.map(process_file, file_paths)

    # Print results
    for result in results:
        if result:
            file_path, includes = result
            print(f"Includes in {file_path}:")
            for include in includes:
                print(f"  {include}")

if __name__ == "__main__":
    main()