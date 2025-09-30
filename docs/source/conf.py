# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import sys
from pathlib import Path

import collections
def remove_namedtuple_attrib_docstring(app, what, name, obj, skip, options):
    if type(obj) is collections._tuplegetter:
        return True
    if obj is object:
        return True
    return skip


def setup(app):
    app.connect('autodoc-skip-member', remove_namedtuple_attrib_docstring)

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'quazydb'
copyright = '2025, Andrey Aseev'
author = 'Andrey Aseev'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = []

templates_path = ['_templates']
exclude_patterns = []

extensions = ['sphinx.ext.autodoc', 'sphinx.ext.napoleon', 'sphinx.ext.viewcode']
sys.path.insert(0, str(Path('..', '..').resolve()))
autodoc_member_order = 'bysource'

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['_static']
