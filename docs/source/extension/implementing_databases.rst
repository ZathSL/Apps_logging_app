Implementing Databases
======================

This section explains how to implement a new database client in the framework
by extending the base abstractions and configuring the flow.

Overview
--------

Databases in the framework are responsible for:

- Managing connections (primary and optional replica)
- Executing queries asynchronously
- Returning results via `Future` objects
- Handling retries and safe reconnections

Database clients are defined via configuration files and implemented as Python classes
that extend the `BaseDatabase` class. The configuration is validated using Pydantic models.

---

Database Configuration
----------------------

Each database is configured via a YAML definition and validated through a `BaseDatabaseConfig` model.
A typical configuration includes:

- `type`: logical type identifier of the database (e.g., postgres, mysql)
- `name`: unique instance name
- `username` / `password`: authentication credentials
- `primary`: primary connection details
- `replica` (optional): read-replica connection
- `max_retries`: maximum number of retry attempts for connection failures
- `max_workers`: maximum number of concurrent query executions

Example:

.. code-block:: yaml

    databases:
      - type: postgres
        name: analytics-db
        username: myuser
        password: mypassword
        primary:
          host: db.example.com
          port: 5432
          service_name: analytics
        replica:
          host: replica.example.com
          port: 5432
        max_retries: 5
        max_workers: 10

> Note: All fields are validated using Pydantic `BaseModel` classes and `field_validator`s
> to ensure correctness before runtime.

---

Extending the BaseDatabase
--------------------------

To implement a custom database client, create a new Python class that extends `BaseDatabase`:

.. code-block:: python

    from myproject.databases.base import BaseDatabase, BaseDatabaseConfig

    class MyCustomDatabase(BaseDatabase):

        def _connect(self):
            # Implement the logic to connect to your database
            pass

        def _query(self, query: str, params: dict = None) -> dict:
            # Implement the logic to execute a query and return results
            return {}

Key points:

- Implement `_connect` to establish the connection to your database
- Implement `_query` to execute queries with optional parameters
- The framework automatically dispatches queries using a queue and `ThreadPoolExecutor`
- Each query returns a `Future` object, which allows non-blocking execution

---

Query Dispatch and Execution
----------------------------

The `BaseDatabase` handles query execution asynchronously:

1. When `execute(query, params)` is called, a `QueryTask` is created and put into a queue
2. The `_dispatch` method picks tasks from the queue and submits them to a thread pool
3. `_safe_query` wraps `_query` and automatically reconnects in case of failure
4. Results are set on the task's `Future` object
5. Retries are handled automatically according to `max_retries`

> This design allows agents to execute database queries concurrently without blocking the main workflow.

---

Shutting Down the Database
--------------------------

To gracefully shut down a database client:

.. code-block:: python

    db_client.shutdown()

This method:

- Stops the dispatcher thread
- Waits for all queued tasks to finish
- Shuts down the thread pool executor
- Logs the shutdown event

---

Tips for Custom Database Clients
--------------------------------

- Use Pydantic validators to enforce input correctness
- Start with one connection to simplify debugging
- Ensure `_connect` establishes the connection and raises exceptions on failure
- Ensure `_query` returns a dictionary (even empty) to maintain compatibility with agents
- Leverage the existing `_safe_query` and `_safe_connect` methods for automatic retries

---

Next Steps
----------

After implementing your database client, you can proceed to:

.. toctree::
   :maxdepth: 1

   implementing_producers

This section describes how to extend the framework to support custom producers
that agents can use to send processed messages.
