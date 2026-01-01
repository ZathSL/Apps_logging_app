from ..orchestration.orchestrator import BaseOrchestrator
from .base import BaseProducer

class ProducerOrchestrator(BaseOrchestrator):
    """
    Orchestrator class for managing a producer instance.

    The `ProducerOrchestrator` ensures that the underlying producer connection is active,
    handles automatic retries, and provides a consistent interface for connecting and
    closing the producer.

    It extends `BaseOrchestrator` and leverages its connection retry logic, 
    while delegating the actual connection and closure operations to the `BaseProducer` instance.

    Parameters
    ----------
    producer : BaseProducer
        The producer instance to be managed by the orchestrator.

    Methods
    -------
    _connect()
        Establishes a connection to the underlying producer service. 
        Implements the abstract `_connect` method from `BaseOrchestrator`.
    close()
        Closes the producer and flushes any pending messages. Implements the abstract
        `close` method from `BaseOrchestrator`.

    Notes
    -----
    - The orchestrator automatically uses the producer's `max_retries` for its retry logic.
    - This class is typically used internally by the `ProducerFactory` to manage producer
      lifecycle and ensure reliable message delivery.
    """

    def __init__(self, producer: BaseProducer):
        """
        Initializes a ProducerOrchestrator object.

        :param producer: The BaseProducer instance to be used for sending messages.
        :raises: Any exceptions raised by the producer when connecting or closing.
        """
        super().__init__(
            name=f"Producer-{producer.config.type}-{producer.config.name}",
            max_retries=producer.config.max_retries
        )
        self.producer = producer

    def _connect(self) -> None:
        """
        Connect the producer.

        This method will connect the producer to the underlying service, such as a message broker.

        :raises: Any exceptions raised by the producer when connecting.
        """
        self.producer.connect()

    def close(self) -> None:
        """
        Close the producer.

        This method will flush any pending messages to be sent and then close the producer.
        """
        self.producer.close()
