from ..orchestration.orchestrator import BaseOrchestrator
from .base import BaseProducer

class ProducerOrchestrator(BaseOrchestrator):
    """Orchestrates the lifecycle and connectivity of a producer instance.

    This orchestrator adapts a :class:`BaseProducer` to the orchestration
    framework defined by :class:`BaseOrchestrator`. It delegates connection
    management and lifecycle operations (connect, status check, close) to
    the underlying producer while leveraging the retry and orchestration
    mechanisms provided by the base orchestrator.

    The orchestrator name and retry behavior are automatically derived from
    the producer's configuration, ensuring consistent identification and
    fault-tolerance behavior across different producer implementations.

    This class is intended to be used as an internal coordination layer and
    does not introduce producer-specific logic beyond delegation.

    Attributes:
        producer (BaseProducer): The producer instance being orchestrated,
            responsible for implementing the actual connection and resource
            management logic.

    See Also:
        BaseOrchestrator: Defines the orchestration lifecycle and retry logic.
        BaseProducer: Defines the producer interface and configuration contract.
    """
    def __init__(self, producer: BaseProducer):
        """
        Initialize the orchestrator with a producer instance.

        This constructor binds a :class:`BaseProducer` to the orchestrator and
        configures the base orchestration behavior using the producer's
        configuration. In particular, the orchestrator name is derived from the
        producer type and name, and the maximum number of retries is inherited
        from the producer's configuration.

        Args:
            producer (BaseProducer): The producer instance to be orchestrated.
                It must expose a ``config`` object containing ``type``, ``name``,
                and ``max_retries`` attributes, and implement the producer
                connection lifecycle methods.

        Attributes:
            producer (BaseProducer): Reference to the producer managed by this
                orchestrator.

        Raises:
            AttributeError: If the provided producer does not expose the expected
                configuration attributes.
        """
        super().__init__(
            name=f"Producer-{producer.config.type}-{producer.config.name}",
            max_retries=producer.config.max_retries
        )
        self.producer = producer

    def _is_connected(self) -> bool:
        """
        Check whether the producer is currently connected.

        This method delegates the connectivity check to the underlying
        :class:`BaseProducer` instance. It is invoked by the base orchestrator
        as part of the orchestration lifecycle to determine whether a connection
        attempt is required.

        Returns:
            bool: ``True`` if the producer is currently connected, ``False``
            otherwise.

        See Also:
            BaseProducer.is_connected: Producer-level connectivity check.
        """
        return self.producer.is_connected()

    def _connect(self) -> None:
        """
        Establish a connection using the underlying producer.

        This method delegates the connection logic to the managed
        :class:`BaseProducer` instance. It is called by the base orchestrator
        when a connection is required and is typically executed within the
        retry logic defined by :class:`BaseOrchestrator`.

        Any exceptions raised by the producer during connection are propagated
        to the orchestrator, which is responsible for handling retries and
        failure behavior.

        Returns:
            None
        """
        self.producer.connect()

    def close(self) -> None:
        """
        Close the producer and release associated resources.

        This method delegates the shutdown and cleanup logic to the underlying
        :class:`BaseProducer` instance. It should be invoked when the orchestrator
        is no longer needed, ensuring that any open connections or allocated
        resources held by the producer are properly released.

        This operation is not retried and is expected to be idempotent, depending
        on the producer implementation.

        Returns:
            None
        """
        self.producer.close()
