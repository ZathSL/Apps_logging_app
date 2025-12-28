Logical Flow
============

Execution lifecycle
-------------------

The execution of a flow follows a well-defined lifecycle:

1. Agent configuration is loaded from YAML files.
2. The Agent Factory creates a new agent instance based on the configured type.
3. During initialization, the agent requests the creation of the required
   Database and Producer instances from their respective factories.
4. Each factory resolves the requested component type by consulting the
   corresponding registry and instantiates the concrete implementation.
5. The agent incrementally reads data from a configured file source.
6. Each read entry is filtered and transformed into a structured key-value
   representation.
7. The extracted data can be used to parameterize a database query. In this
   case, the agent enqueues a query execution request to the database
   orchestrator and continues processing other flows while awaiting the result.
8. Once available, the result of the database query (or the transformed file
   data) becomes the payload of a message.
9. The message is enqueued to the producer, which is responsible for sending it
   to the external asynchronous messaging system.

Example execution
-----------------

Given the following flow configuration:

- agent: ``sasdm_agent``
- agent-factory: ``agents_factory``
- agent-registry: ``agents_registry``

- database: ``oracledb``
- database-factory: ``database_factory``
- database-registry: ``database_registry``

- producer: ``kafka_producer``
- producer-factory: ``producer_factory``
- producer-registry: ``producer_registry``

The execution proceeds as follows:

1. The configuration for ``sasdm_agent`` is loaded from the YAML configuration
   files.
2. ``AgentsFactory`` creates a new instance of the ``sasdm_agent`` by resolving
   its type through the ``AgentsRegistry``.
3. During initialization, ``sasdm_agent`` requests an instance of the
   ``oracledb`` database and the ``kafka_producer`` producer.
4. ``DatabaseFactory`` and ``ProducerFactory`` consult their respective
   registries and instantiate the concrete ``OracleDatabase`` and
   ``KafkaProducer`` implementations.
5. ``sasdm_agent`` starts reading a configured log file incrementally.
6. Each log entry is filtered and transformed into a structured dictionary.
7. The extracted values are used to parameterize a query, which is enqueued for
   execution by the database orchestrator. While awaiting the response,
   ``sasdm_agent`` continues processing other data.
8. Once the database response is available, the result becomes the message
   payload.
9. The message is enqueued to ``KafkaProducer``, which sends it to the external
   messaging system.

Sequence diagram
----------------

.. mermaid::

   %%{init: {
     "theme": "dark",
     "themeVariables": {
       "fontSize": "18px",
       "lineColor": "#00e5ff",
       "primaryTextColor": "#ffffff"
     }
   }}%%

   sequenceDiagram
       participant Agent
       participant Database
       participant Producer
       participant MessagingSystem

       Agent->>Agent: read file line
       Agent->>Agent: filter / transform

       alt Database query enabled
           Agent->>Database: enqueue query (async)
           Database-->>Agent: query result
           Agent->>Producer: create message from DB result
       else Database query disabled
           Agent->>Producer: create message from transformed line
       end

       Producer->>MessagingSystem: send message

