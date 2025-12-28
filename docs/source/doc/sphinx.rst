Generating Sphinx Documentation
================================

This section illustrates how to generate the project documentation using Sphinx.

Installing Required Packages
----------------------------

First, install Sphinx and the useful extensions:

.. code-block:: bash

   pip install sphinx
   pip install sphinx-autodoc-typehints sphinx-autosummary sphinxcontrib-mermaid

Creating the Basic Sphinx Structure
-----------------------------------

1. Create the `docs` folder:

   .. code-block:: bash

      mkdir docs
      cd docs

2. Initialize Sphinx using the `sphinx-quickstart` command:

   .. code-block:: bash

      sphinx-quickstart

Configuring Sphinx
------------------

Open the `source/conf.py` file and add the following lines:

.. code-block:: python

   import os
   import sys
   sys.path.insert(0, os.path.abspath('../..'))

   extensions = [
       "sphinx.ext.autodoc",
       "sphinx.ext.napoleon",
       "sphinx.ext.viewcode",
       "sphinx.ext.autosummary",
   ]

   autosummary_generate = True
   autodoc_member_order = "bysource"
   autodoc_typehints = "description"

Configuring the Index
---------------------

Open the `index.rst` file and add the following section to include the modules:

.. code-block:: rst

   .. toctree::
      :maxdepth: 2
      :caption: Contents

      modules

Generating Documentation Files
------------------------------

1. Navigate to the `docs` folder:

   .. code-block:: bash

      cd docs

2. Generate the `.rst` files for the Python modules:

   .. code-block:: bash

      sphinx-apidoc -o source/ ../apps_logging_app

Building the HTML Documentation
-------------------------------

To generate the final HTML documentation:

.. code-block:: bash

   make html

Once the process is complete, the documentation will be available in the `_build/html` folder.
