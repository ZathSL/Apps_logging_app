Architecture Overview
=====================

Apps Logging App is a configuration-driven framework designed around a
pipeline-based architecture.

The behavior of the system is defined declaratively through configuration files,
which describe how data is collected, processed and forwarded to external
messaging systems.

Design principles
-----------------

The architecture of Apps Logging App is based on the following principles:

- configuration over code
- loose coupling between components
- extensibility through well-defined contracts
- isolation of execution flows

Core components
---------------

The framework is composed of three main types of components:

- **Agents**
- **Databases**
- **Producers**

These components are instantiated and wired together at runtime based on the
configuration provided by the developer. This approach allows the integration
of new data sources and processing logic without requiring changes to the core
framework.

Architecture diagram
--------------------

.. mermaid::

    graph LR
        %% Sources
        FileSources[File Sources]

        %% Core components
        Agent[Agent]
        Orchestrator[Orchestrator]
        ExternalDB[(External Database)]
        Producer[Producer]
        Brokers[Message Brokers]

        %% Queues
        QueryQueue[(Query Queue)]
        MessageQueue[(Message Queue)]

        %% File ingestion
        FileSources --> Agent

        %% Query flow
        Agent -->|Create Queries| QueryQueue
        QueryQueue -->|Consume Queries| Orchestrator
        Orchestrator -->|Execute Query| ExternalDB
        ExternalDB -->|Query Result| Orchestrator
        Orchestrator -->|Return Result| Agent

        %% Message production
        Agent -->|Produce Messages| MessageQueue

        %% Message distribution
        MessageQueue --> Producer
        Producer --> Brokers


Parallel flows
--------------

Apps Logging App supports the execution of multiple independent flows running
in parallel. Each flow represents an isolated pipeline that defines:

- one or more data sources
- optional data transformation or filtering steps
- one or more output producers

Flows are defined declaratively and can be extended by implementing concrete
classes derived from the abstract base components provided by the framework.

Typical execution flow
----------------------

A typical execution flow follows these steps:

1. An **Agent** incrementally reads data from one or more sources, such as
   application log files or system files.
2. The collected data can be:
   
   - filtered or transformed using configurable rules (e.g. regular expressions)
   - used as input parameters for database queries
3. **Databases** execute parameterized or static queries against external data
   sources to retrieve additional information or monitoring metrics.
4. The resulting data, whether coming from file sources or database queries, is
   forwarded to **Producers**.
5. **Producers** send messages to asynchronous messaging systems, such as
   message queues or streaming platforms.

Scope and responsibilities
--------------------------

Apps Logging App is responsible exclusively for:

- data collection
- data filtering and transformation
- message production

Message consumption, visualization and user-facing components are intentionally
out of scope and are delegated to external systems. Any platform capable of
consuming messages from a messaging system (e.g. Kafka) and exposing them
through dashboards or monitoring tools (such as Grafana) can be integrated with
the framework.


For a detailed description of the execution logic, see :doc:`flow`.
For information on extending the framework, see :doc:`../extension/index`.
