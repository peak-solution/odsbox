# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

from __future__ import annotations

# -- Project information -----------------------------------------------------

project = "ODSBox"
copyright = "2026, Peak Solution"
author = "Peak Solution"

# The full version, including alpha/beta/rc tags
release = "0.0.0"  # overridden by sphinx-build -D release=...
try:
    import odsbox

    release = odsbox.__version__
except ImportError:
    pass


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "autoapi.extension",
    "sphinx.ext.napoleon",  # to render Google format docstrings
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.ifconfig",
    "sphinx.ext.viewcode",  # Add links to highlighted source code
    "sphinx.ext.githubpages",
    "sphinx_sitemap",
    "sphinx_copybutton",
    "myst_parser",
    "sphinx_design",
    "nbsphinx",  # Jupyter notebook support
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "furo"

html_context = {"google_site_verification": "M-YV4bEhpyyWVOBQB9VLsSCjKfqO_UpvTBMJ7DS5t_U"}

# for sitemap
html_baseurl = "https://peak-solution.github.io/odsbox/"
sitemap_url_scheme = "{link}"

templates_path = ["_templates"]

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ["_static"]

# Napoleon settings
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = True

# AutoAPI settings — auto-discovers all modules from source
autoapi_type = "python"
autoapi_dirs = ["../src"]
autoapi_options = ["members", "undoc-members", "show-inheritance", "show-module-summary", "imported-members"]
autoapi_ignore = ["*/proto/*", "*_pb2*"]
autoapi_keep_files = True
autoapi_add_toctree_entry = False
autoapi_python_class_content = "both"

# Autodoc settings (used by autoapi for type hint rendering)
autodoc_typehints = "description"

# Suppress warnings for multiple targets (e.g., ConI available both as odsbox.ConI and odsbox.con_i.ConI)
suppress_warnings = ["ref.python"]

# NBSphinx settings
nbsphinx_execute = "never"  # Don't execute notebooks during build (they contain connection examples)
nbsphinx_allow_errors = True  # Allow errors in notebook cells (useful for examples that need server connection)
