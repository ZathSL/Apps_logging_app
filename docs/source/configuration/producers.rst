Producers configuration
=======================

File: ``producers.yaml``

Overview
--------

The ``producers.yaml`` file defines the configuration of all message producers
used to forward processed data to external asynchronous messaging systems.

Producers represent the final stage of a flow and are responsible for ensuring
reliable message delivery according to the configured reliability and
performance parameters.

---

General structure
-----------------

The file contains a list of producer definitions. Each producer is uniquely
identified by a **type** and a **name**.

.. note::

   Producers (and databases) are fully pluggable components and can be implemented
   with any configuration. You can create a new producer by:

   1. Defining a new configuration model in `config.py`
   2. Implementing a new producer class in `producer.py`
   3. Creating a new pipeline definition following the general flow implemented in `base.py`

   Everything shown on this page illustrates a specific implementation using a
   Kafka producer. The framework allows you to extend it with other producer types
   without modifying the core logic.


.. code-block:: yaml

    producers:
    - type: kafka_handler
      name: kafka-handler-hulk
      max_retries: 5
      brokers:
        - kafka:9092
      seceurity_protocol: PLAINTEXT
      ssl_cafile: /path/to/ca.pem
      ssl_certfile: /path/to/service.cert
      ssl_keyfile: /path/to/service.key
      ssl_password: your_ssl_password
      topic: oracle_topic
      acks: all
      batch_size: 16384
      linger_ms: 5
      buffer_memory: 33554432

Producer definition
-------------------

Each database definition includes one or more connection endpoints. The primary
endpoint is used by default for query execution.

.. code-block:: yaml

    producers:
    - type: kafka_handler
      name: kafka-handler-hulk
      max_retries: 5



Fields
~~~~~~

``type``
  Logical identifier of the producer implementation. It is resolved at runtime
  through the producer registry and mapped to a concrete Python class.

``name``
  Unique name of the producer instance. Multiple producers of the same type may
  be declared with different configurations.

``max_retries`` *(optional)*
  Maximum number of retry attempts in case of message delivery failures.

Broker configuration
--------------------

Each database definition includes one or more connection endpoints. The primary
endpoint is used by default for query execution.

.. code-block:: yaml

    brokers:
    - kafka:9092

Fields
~~~~~~

``brokers``
  List of broker endpoints used to establish the connection with the messaging
  system.

Security configurations
-----------------------

Each database definition includes one or more connection endpoints. The primary
endpoint is used by default for query execution.

.. code-block:: yaml

    security_protocol: PLAINTEXT
    security_protocol: PLAINTEXT
    ssl_cafile: /path/to/ca.pem
    ssl_certfile: /path/to/service.cert
    ssl_keyfile: /path/to/service.key
    ssl_password: your_ssl_password

Fields
~~~~~~

``security_protocol``
  Protocol used to communicate with the messaging system (e.g. PLAINTEXT,
  SSL, SASL_SSL).

``ssl_cafile`` *(optional)*
  Path to the Certificate Authority (CA) file.

``ssl_certfile`` *(optional)*
  Path to the client certificate file.

``ssl_keyfile`` *(optional)*
  Path to the client private key file.

``ssl_password`` *(optional)*
  Password used to decrypt the private key, if required.

Security parameters are optional and depend on the selected security protocol.


Topic and delivery configuration
--------------------------------

.. code-block:: yaml

    topic: oracle_topic
    acks: all

Fields
~~~~~~

``topic``
  Destination topic where messages will be published.

``acks``
  Acknowledgment level required for message delivery (e.g. all, 1, 0).

Performance tuning
------------------

.. code-block:: yaml

    batch_size: 16384
    linger_ms: 5
    buffer_memory: 33554432

Fields
~~~~~~

``batch_size``
  Maximum size (in bytes) of a message batch before it is sent.

``linger_ms``
  Time (in milliseconds) the producer waits before sending a batch.

``buffer_memory``
  Total memory (in bytes) available to buffer unsent messages.

These parameters allow fine-grained control over throughput and latency.


Retry and fault handling
------------------------

The max_retries parameter defines the retry strategy applied when message
delivery fails due to transient errors.

Retries are managed internally by the producer implementation and are
transparent to the agent logic.


usage within flows
------------------

Producers are not invoked directly. They are referenced by agents through
producer_connections defined in agents.yaml.

.. code-block:: yaml

    producer_connections:
    - type: kafka_handler
      name: kafka-handler-hulk

When a message is enqueued, the producer asynchronously delivers it to the
configured messaging system.

See :doc:`agents` for information on how producers are referenced within flows.
