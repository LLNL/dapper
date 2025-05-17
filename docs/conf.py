import os
import sys

import requests

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "DAPper"
# pylint: disable-next=redefined-builtin
copyright = "2025, Lawrence Livermore National Security"
author = "Ryan Mast, Micaela Gallegos, Steven West, Monwen Shen"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.githubpages",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "images.toml"]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_theme_options = {
    "description": "Dependency Analysis Project",
    "github_user": "LLNL",
    "github_repo": "DAPper",
    "github_button": "true",
    "github_banner": "true",
    "badge_branch": "main",
    "fixed_sidebar": "false",
}

# -- Extension configuration -------------------------------------------------

# Napoleon settings for NumPy and Google style docstrings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
#html_logo = "./logos/dapper-logo-light.png"
#html_favicon = html_logo
html_sidebars = {"**": ["globaltoc.html", "relations.html", "searchbox.html"]}
html_static_path = ["_static"]
