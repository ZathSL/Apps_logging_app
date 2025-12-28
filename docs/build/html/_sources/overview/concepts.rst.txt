Core Concepts
=============

Configuration-driven design
----------------------------

Apps Logging App follows a declarative, configuration-driven design.
The behavior of the system is defined through a set of configuration files:

- ``agents.yaml``
- ``databases.yaml``
- ``producers.yaml``

These files are used to declare both the components that participate in a flow
and the way in which those components are composed and configured.

Each **Agent**, **Database**, and **Producer** is identified by:

- a **type**, which defines the concrete implementation through a specific
  Python class extending a base component class
- a **name**, which represents a configured instance of that implementation

Multiple instances of the same type can coexist, each with different
configuration parameters.

Extensibility is achieved by allowing users to implement new concrete classes
that extend the base component classes provided by the framework and register
them for use within the configuration files.

---

Factory-driven design
---------------------

The system is implemented following a factory-driven design.

Concrete components declared in the configuration files are instantiated
automatically by the framework through dedicated factories. Each factory is
responsible for creating and initializing a specific category of components
(e.g. agents, databases, producers).

When an **Agent** is instantiated, it uses its configuration to determine which
**Databases** and **Producers** participate in its execution flow. The agent
then requests the creation or retrieval of the required component instances
from the corresponding factories.

Once instantiated, each component operates independently. Databases and
Producers are isolated from each other and from the agent logic, ensuring
loose coupling between components.

Parallelism, flow isolation and fault tolerance are achieved through
asynchronous communication mechanisms and fallback strategies implemented at
the framework level.

---

Registry-driven design
----------------------

Apps Logging App also adopts a registry-driven design.

Each component type is associated with a registry entry that maps a logical
type identifier to a concrete Python implementation. Registries are consulted
exclusively by the factories during the instantiation process.

This approach allows the framework to dynamically resolve component
implementations at runtime, while keeping configuration files decoupled from
direct Python imports and implementation details.

---

Flow
----

A **flow** represents the core execution unit of the framework and defines the
lifecycle of a piece of information or a set of related data.

From an architectural perspective, Apps Logging App follows an ETL-like model,
where each flow is responsible for:

- extracting data from one or more sources
- filtering or transforming the collected data
- optionally parameterizing and executing queries against external databases
- producing messages to external asynchronous messaging systems

Each flow is fully isolated from other flows and executes independently.
The progression speed of a flow is influenced by asynchronous communication
patterns and by the consumption rates of the message queues connecting agents to
databases and producers and communication between databases and producers with their own 
related system (e.g. remote databases, remote brokers)