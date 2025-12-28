Databases configuration
=======================

File: ``databases.yaml``

Overview
--------

The ``databases.yaml`` file defines the configuration of all database connections
used by the framework to enrich or validate data during flow execution.

Databases are optional components within a flow and are accessed asynchronously
by agents when a database destination is defined in a data connection.

---

General structure
-----------------

The file contains a list of database definitions. Each database is uniquely
identified by a **type** and a **name**.

.. note::

   Databases are fully pluggable and optional components within flows. You can implement
   a new database by:

   1. Defining a new configuration model in `config.py`
   2. Implementing a new database class in `database.py`
   3. Creating custom query handling or transformation logic following the base interfaces in `base.py`

   Everything shown in this documentation illustrates a specific implementation
   using Oracle. The framework supports adding new database types or schemas
   without modifying the core flow logic.


.. code-block:: yaml

    databases:
    - type: oracle
      name: sasdb_ciexpit_owner
      max_retries: 5
      username: system
      password: oracle

Fields
~~~~~~

``type``
  Logical identifier of the database implementation. It is resolved at runtime
  through the database registry and mapped to a concrete Python class.

``name``
  Unique name of the database instance. Multiple instances of the same database
  type may be defined with different configurations.

``max_retries`` *(optional)*
  Maximum number of retry attempts in case of connection or query execution
  failures.

``username``
  Username used to authenticate against the database.

``password``
  Password used to authenticate against the database.

Primary connection configuration
--------------------------------

Each database definition includes one or more connection endpoints. The primary
endpoint is used by default for query execution.

.. code-block:: yaml

    primary:
      host: oracle
      port: 1521
      service_name: XEPDB1


Fields
~~~~~~

``host``
  Hostname or IP address of the database server.

``port``
  Network port used to establish the database connection.

``service_name``
  Database service identifier used by the client to connect to the database
  instance.

Secondary connection configuration (fallback)
---------------------------------------------

Each database definition includes one or more connection endpoints. The primary
endpoint is used by default for query execution.

.. code-block:: yaml

    replica:
      host: oracle2
      port: 1521
      service_name: XEPDB1

Fields
~~~~~~

``host``
  Hostname or IP address of the database server.

``port``
  Network port used to establish the database connection.

``service_name``
  Database service identifier used by the client to connect to the database
  instance.


Retry and fault handling
------------------------

The max_retries parameter defines the retry strategy applied when database
operations fail due to transient errors.

Retries are handled internally by the framework and are transparent to the
agent logic. If all retry attempts fail, the framework applies fallback
mechanisms according to its error-handling strategy.


Usage within flows
------------------

Databases are not executed autonomously. They are invoked exclusively by agents
when a data connection defines a destination_ref section.

.. code-block:: yaml

    destination_ref:
      type: oracle
      name: sasdb_ciexpit_owner
      query: SELECT * FROM dual

In this case, the agent submits the query for asynchronous execution and
continues processing other tasks while awaiting the result.


Security consideration
----------------------

Credentials are defined in plain text within the configuration file. In
production environments, it is recommended to externalize sensitive values
using environment variables or secret management systems and reference them
within the configuration.


See :doc:`agents` for information on how databases are referenced within flows.
