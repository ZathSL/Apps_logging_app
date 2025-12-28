Agents configuration
====================

File: ``agents.yaml``

Overview
--------

The ``agents.yaml`` file defines the configuration of all agents participating
in the execution flows. Each agent represents the core orchestration component
responsible for:

- reading data from configured sources
- filtering and transforming extracted information
- optionally querying external databases
- producing messages through one or more producers

Agents are declared declaratively and instantiated dynamically by the framework
based on their configuration.

---

General structure
-----------------

The file contains a list of agent definitions. Each agent is uniquely identified
by a **type** and a **name**, and can be configured with multiple data sources
and producer connections.

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
        - name: info_pattern
            is_error: false
            source_ref: 
            path_file_name: onprem_direct
            regex_pattern: 'responseJSON:\s*(?P<response_json>\{[\s\S]*?\})'
        - name: info_pattern_engagement_all_messages
            is_error: false
            expired_time: 1440
            source_ref: 
            path_file_name: onprem_direct
            regex_pattern: 'responseJSON:\s*(?P<response_json>\{[\s\S]*?\})'
            destination_ref:
            type: oracle
            name: sasdb_ciexpit_owner
            sql: SELECT * FROM dual
        - name: info_pattern_engagement_single_message
            is_error: false
            expired_time: 1440
            source_ref: 
            path_file_name: onprem_direct
            regex_pattern: "INFO|DEBUG|TRACE"
            destination_ref:
            type: oracle
            name: sasdb_ciexpit_owner
            sql: SELECT * FROM dual
        - name: normal_query
            is_error: false
            expired_time: 1440
            destination_ref:
            type: oracle
            name: sasdb_ciexpit_owner
            sql: SELECT * FROM dual

Agent definition
----------------

.. code-block:: yaml

    agents:
    - type: sasdm
      name: sasdm-agent
      buffer_rows: 500


Fields
~~~~~~

``type``
  Logical identifier of the agent implementation. It is resolved at runtime
  through the agent registry and mapped to a concrete Python class.

``name``
  Unique name of the agent instance. Multiple agents of the same type can be
  declared with different configurations.

``buffer_rows`` *(optional)*
  Number of rows buffered by the agent before processing. This parameter is used
  to optimize incremental file reading and batching behavior.


File sources configuration
--------------------------

Agents can be configured to read data incrementally from one or more file
sources.

.. code-block:: yaml

    path_files:
    - name: onprem_direct
      path: /app/apps_logging_app/logs/prova.log



Fields
~~~~~~

``name``
  Logical identifier of the file source. It is used as a reference by data
  extraction rules.

``path``
 Absolute or relative path to the file to be monitored and read incrementally.

Producer connections
--------------------

Each agent can be connected to one or more producers. Producer connections define
how processed data is routed and transformed into messages.

.. code-block:: yaml

    producer_connections:
    - type: kafka_handler
      name: kafka-handler-hulk


Fields
~~~~~~

``type``
  Logical producer type resolved through the producer registry.

``name``
 Instance name of the producer configuration.

Data connections
----------------

Data connections define how information is extracted from sources, optionally
enriched through database queries, and transformed into messages.

.. code-block:: yaml

    data_connections:
    - name: error_pattern
      is_error: true
      source_ref:
      path_file_name: onprem_direct
      regex_pattern: "ERROR|FATAL|EXCEPTION"

Fields
~~~~~~

``is_error``
  Flag indicating whether the extracted data represents an error condition.

``name``
 Logical identifier of the data connection.

``expired_time`` *(optional)*
 Time-to-live (in minutes) used to control message expiration or aggregation.


Source reference
----------------

The source_ref section defines how data is extracted from a specific source.

.. code-block:: yaml

    source_ref:
      path_file_name: onprem_direct
      regex_pattern: "INFO|DEBUG|TRACE"


Fields
~~~~~~

``path_file_name``
  Reference to a previously defined file source.

``regex_pattern``
 Regular expression used to extract or match data from each read line. Named
 capturing groups can be used to build structured key-value data.


Optional database destination
-----------------------------

Data connections may optionally define a database destination. When present, the
extracted data is used to parameterize and execute a database query. If the destination_ref section is omitted, the transformed source data is
sent directly to the producer as the message payload.

.. code-block:: yaml

    destination_ref:
      type: oracle
      name: sasdb_ciexpit_owner
      sql: SELECT * FROM dual

Fields
~~~~~~

``type``
  Logical database type resolved through the database registry.

``name``
 Configured database instance name.

``query``
 Configured database instance name.


Message creation behavior
-------------------------

Depending on the configuration of each data connection:

    - if a destination_ref is defined, the database query result is used as the message payload
    - otherwise, the transformed data extracted from the source is used directly

This behavior allows agents to support both lightweight log forwarding and
database-enriched message production within the same flow.


For information about producers, see :doc:`producers`.
For database configuration, see :doc:`databases`.
