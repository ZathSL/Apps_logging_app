Implementing Producers
======================

This section explains how to implement a new producer in the framework
by extending the base abstractions and configuring the message flow.

Overview
--------

Producers in the framework are responsible for:

- Sending messages to an underlying service (e.g., Kafka, RabbitMQ, REST endpoint)
- Handling errors and retries
- Operating asynchronously via a queue and worker thread
- Supporting graceful shutdown

Producer clients are defined via configuration files and implemented as Python classes
that extend the `BaseProducer` class. The configuration is validated using Pydantic models.

---

Producer Configuration
----------------------

Each producer is configured via a YAML definition and validated through a `BaseProducerConfig` model.
A typical configuration includes:

- `type`: logical type identifier of the producer (e.g., kafka, rabbitmq)
- `name`: unique instance name
- `max_retries`: maximum number of retry attempts for failed message deliveries

Example:

.. code-block:: yaml

    producers:
      - type: kafka
        name: analytics-producer
        max_retries: 5

> Note: All fields are validated using Pydantic `BaseModel` and `field_validator` methods to ensure correctness before runtime.

---

Extending the BaseProducer
--------------------------

To implement a custom producer client, create a new Python class that extends `BaseProducer`:

.. code-block:: python

    from myproject.producers.base import BaseProducer, BaseProducerConfig

    class MyCustomProducer(BaseProducer):

        def _connect(self):
            # Implement the logic to connect to your service
            pass

        def _send(self, is_error: bool, message: dict):
            # Implement the logic to send a message
            pass

        def _close(self):
            # Implement the logic to close the connection
            pass

Key points:

- `_connect` should establish a connection to the underlying service
- `_send` should send the message (handle both error and normal messages)
- `_close` should cleanly close the connection
- The framework automatically handles a queue, retries, and asynchronous execution

---

Producing Messages
------------------

To produce messages:

.. code-block:: python

    producer = MyCustomProducer(config)
    producer.produce(is_error=False, message={"data": "example"})

Flow:

1. `produce` puts the message into an internal queue
2. The worker thread consumes messages from the queue
3. `_send` is called for each message
4. If `_send` fails:
    - The message is retried up to `max_retries`
    - If retries exceed the limit, the connection is reset and retries start again

---

Worker Thread and Error Handling
--------------------------------

The worker thread `_worker` handles:

- Connection to the underlying service using `_safe_connect`
- Fetching messages from the queue
- Sending messages via `_send`
- Retrying messages up to `max_retries`
- Reconnecting on failures and waiting 60 seconds if maximum retries are exceeded

This design ensures that the producer operates asynchronously and robustly in the face of transient errors.

---

Shutting Down the Producer
--------------------------

To gracefully shut down a producer:

.. code-block:: python

    producer.shutdown(timeout=10)

This method:

- Signals the worker thread to stop
- Calls `_close` to shut down the underlying connection
- Waits for the worker thread to finish (optionally with a timeout)
- Logs the shutdown process

---

Tips for Custom Producer Clients
--------------------------------

- Use Pydantic validators to enforce configuration correctness
- Start with a single connection for simplicity
- Ensure `_connect` raises exceptions if the connection fails
- Ensure `_send` handles all message types and raises exceptions on failure
- Use `_close` to release resources gracefully
- Leverage `_safe_connect` and the retry logic to automatically handle transient failures

---

Next Steps
----------

After implementing your producer client, you can integrate it with agents and pipelines
to send messages asynchronously. You can also combine multiple producers for different
message types or destinations.
