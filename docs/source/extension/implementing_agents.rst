Implementing Agents
===================

This section explains how to implement a new agent in the framework by extending the
base abstractions and configuring the flow.

Overview
--------

Agents are responsible for:

- Reading data from one or more input sources (files, streams, etc.)
- Filtering and transforming data using regex and custom logic
- Optionally querying databases for enrichment
- Sending processed messages to one or more producers

Agents are defined through configuration files (``agents.yaml``) and implemented as Python classes
that extend the ``BaseAgent`` class. The configuration is validated using Pydantic models.

---

Agent Configuration
-------------------

Each agent is configured via a YAML definition and validated through a `BaseAgentConfig` model.
A typical configuration includes:

- `type`: logical type identifier of the agent
- `name`: unique instance name
- `buffer_rows` (optional): number of rows read from input files in each batch
- `path_files`: list of input files to read
- `producer_connections`: list of producers with associated data connections
- `pool_interval`: interval (seconds) between agent execution cycles

Example:

.. code-block:: yaml

    agents:
      - type: sasdm
        name: sasdm-agent
        buffer_rows: 500
        path_files:
          - name: onprem_direct
            path: /app/apps_logging_app/logs/prova.log
        producer_connections:
          - type: kafka_handler
            name: kafka-handler-hulk
            data_connections:
              - name: error_pattern
                is_error: true
                source_ref:
                  path_file_name: onprem_direct
                  regex_pattern: "ERROR|FATAL|EXCEPTION"

> Note: All fields are validated using Pydantic ``BaseModel`` classes and ``field_validator`` methods to ensure correctness before runtime.

---

Extending the BaseAgent
-----------------------

To implement a custom agent, create a new Python class that extends ``BaseAgent``:

.. code-block:: python

    from myproject.agents.base import BaseAgent, BaseAgentConfig

    class MyCustomAgent(BaseAgent):

        def _data_connections_transformation_and_filtering(self, working_data_connections):
            # Implement custom transformations and filtering logic
            return working_data_connections

Key points:

- Implement `_data_connections_transformation_and_filtering` to define how extracted data
  should be processed
- The agent will automatically read files in batches, detect rotations, and execute queries
  for any configured databases
- Messages will be sent to configured producers

---

Working Data Flow
-----------------

The agent processes each file as follows:

1. Reads a batch of lines from each configured file (`_read_batch_log`)
2. Detects file rotations and reads remaining lines from previous files if needed (`_read_remaining_old_file`)
3. Matches data using regex patterns defined in `DataConnectionConfig` (`_data_connections_match_regex`)
4. Applies transformations and filtering (`_data_connections_transformation_and_filtering`)
5. Executes database queries for any data connections with `destination_ref` (`_data_connections_execute_queries`)
6. Sends resulting messages to producers (`_send_messages_to_producers`)
7. Cleans expired working data connections (`_clean_working_data_connections`)

> This flow ensures that agents run asynchronously, handle multiple files, and can parallelize database queries and message sending.

---

Tips for Custom Agents
----------------------

- Use Pydantic validators to enforce input correctness and avoid runtime errors
- Start with one producer and one data connection to simplify debugging
- Always respect the `_data_connections_transformation_and_filtering` contract
- Logs are automatically created for each agent instance to trace file processing,
  regex matching, and message sending

---

Next Steps
----------

After implementing your agent, you can proceed to:

.. toctree::
   :maxdepth: 1

   implementing_databases
   implementing_producers

These sections describe how to extend the framework to support custom databases and producers
that your agent can interact with.
