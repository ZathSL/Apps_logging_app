Example Run with Dummy Configurations
=====================================

This section shows how to run the application using example configurations and a Docker environment.

Creating the Docker Environment
-------------------------------

1. **Move the example configurations**  

   Copy the `example/configs-dev` folder into the `apps_logging_app` directory and rename it to `configs`:

   .. code-block:: bash

      mv example/configs-dev apps_logging_app/configs

2. **Move the Docker configuration**  

   Copy the `example/docker` folder to the project root:

   .. code-block:: bash

      mv example/docker .

3. **Navigate to the project root**  

   Make sure you are in the main project directory (not inside `apps_logging_app`):

   .. code-block:: bash

      cd /path/to/project/root

Building the Application Image
------------------------------

To build a single Docker image for the application:

.. code-block:: bash

   sudo docker build -f docker/Dockerfile -t apps-logging-app .

Managing Docker Containers
--------------------------

- **List all containers (running and stopped):**

  .. code-block:: bash

     sudo docker ps -a

- **Remove a container:**

  .. code-block:: bash

     sudo docker rm -f <container-name>

Running the Full Test Project
-----------------------------

1. Navigate to the `apps_logging_app` folder:

   .. code-block:: bash

      cd apps_logging_app

2. Build and start all services defined in the `docker-compose.yaml`:

   .. code-block:: bash

      sudo docker compose -f ../docker/docker-compose.yaml up --build

Accessing the Container
-----------------------

To enter the container and explore the filesystem:

.. code-block:: bash

   sudo docker exec -it apps-logging-app /bin/sh
