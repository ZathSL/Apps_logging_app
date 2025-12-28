CREATE A DOCKER ENVIRONMENT

move example/configs-dev to apps_logging_app directory and rename it as configs

move example/docker to root directory of the project

cd on root directory of the project (not apps_loggin_app folder program)

To build the single image of the application
    sudo docker build -f docker/Dockerfile -t apps-logging-app .

To see a list of container running and stopped
    sudo docker ps -a

To remove containers
    sudo docker rm -f container-name

To build all the test project
    cd inside the apps_loggin_app folder program
    sudo docker compose -f ../docker/docker-compose.yaml up --build

To see inside the container
    sudo docker exec -it apps-logging-app /bin/sh


To generate sphinx doc
pip install sphinx
pip install sphinx-autodoc-typehints sphinx-autosummary sphinxcontrib-mermaid

mkdir docs
cd docs
sphinx-quickstart

inside source/conf.py insert these rows
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

inside index.rst insert this row
.. toctree::
   :maxdepth: 2
   :caption: Contenuti

   modules

cd inside docs folder and execute following commands

sphinx-apidoc -o source/ ../apps_logging_app

make html 