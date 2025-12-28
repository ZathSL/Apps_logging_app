Adding a New Flow
=================

This guide explains how to add a new flow to the framework, step by step. 
A flow defines a complete ETL-like pipeline from agents, optional database interactions, 
to producers sending messages to external systems.

---

Overview
--------

A flow consists of:

- **Agents:** responsible for extracting and transforming data from files or external sources
- **Databases (optional):** used for enrichment, queries, or storing intermediate results
- **Producers:** responsible for sending messages to external systems

Adding a new flow involves:

1. Declaring the flow components in configuration files (`agents.yaml`, `databases.yaml`, `producers.yaml`)
2. Implementing any missing concrete classes extending the base abstractions
3. Registering the new flow in the registry to make it available to the framework

---

Step 1: Define Components in YAML
---------------------------------

Start by defining your new flow components in the appropriate YAML files:

- ``agents.yaml``: declare the new agent(s) and their configuration
- ``databases.yaml``: (optional) define database connections or new models
- ``producers.yaml``: declare the producer(s) that will handle the flow output

Example structure:

.. code-block:: yaml

   agents:
     - type: my_new_agent_type
       name: my_new_agent
       buffer_rows: 500
       producer_connections:
         - type: my_new_producer_type
           name: my_new_producer

   databases:
     - type: my_new_database_type
       name: my_new_database

   producers:
     - type: my_new_producer_type
       name: my_new_producer

> Note: YAML definitions should reference concrete classes implementing the base abstractions.

---

Step 2: Implement Missing Classes
---------------------------------

For any component not already implemented, create a new class by extending the framework's base classes:

- **Agents:** extend `BaseAgent`  
- **Databases:** extend `BaseDatabase`  
- **Producers:** extend `BaseProducer`

.. toctree::
   :hidden:

   implementing_agents
   implementing_databases
   implementing_producers

> Each implementation should override the required abstract methods and provide any custom logic needed for your flow.

---

Step 3: Register the Flow
-------------------------

After implementing your components, register the new flow so that it can be instantiated by the framework:

- Update the registry with the new agent, database, and producer types
- Ensure that the factories can resolve your new types and instantiate them correctly

Example:

.. code-block:: python

   from myproject.registry import agent_registry, database_registry, producer_registry

   agent_registry.register("my_new_agent_type", MyNewAgent)
   database_registry.register("my_new_database_type", MyNewDatabase)
   producer_registry.register("my_new_producer_type", MyNewProducer)

---

Next Steps
----------

After completing the above steps, your flow is fully integrated with the framework. 
For detailed instructions on implementing each type of component, see:

.. toctree::
   :maxdepth: 1

   implementing_agents
   implementing_databases
   implementing_producers

---

Tips
----

- Validate your YAML files with the framework's Pydantic models before running the flow
- Start with one agent and one producer to ensure your flow works correctly
- Use descriptive names for types and instances to avoid conflicts
