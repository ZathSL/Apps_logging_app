from abc import ABC, abstractmethod
import logging
import time
import threading

class BaseOrchestrator(ABC):
    """
    Base class for orchestrators that manage connections to external services.

    This abstract class provides a common interface and connection handling logic
    for services such as databases, message queues, or producers. It ensures that
    connections are established, retried on failure, and safely marked as disconnected
    when necessary.

    The class implements:
      - Thread-safe connection management.
      - Retry logic with configurable maximum retries and delay.
      - Logging for connection attempts and failures.

    Subclasses must implement the following abstract methods:
      - :meth:`_connect` : How to establish the actual connection.
      - :meth:`close` : How to close the connection.

    :param name: A descriptive name for the orchestrator (used in logging).
    :type name: str
    :param max_retries: Maximum number of connection retries before giving up.
    :type max_retries: int
    :param retry_delay: Delay in seconds between retries. Default is 5 seconds.
    :type retry_delay: int
    """

    def __init__(self, name: str, max_retries: int, retry_delay: int = 5):
        """
        Initializes a BaseOrchestrator object.

        :param name: The name of the producer.
        :param max_retries: The maximum number of retries before giving up.
        :param retry_delay: The time to wait in seconds before retrying after a failure.
        """
        self.name = name
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logging.getLogger("__main__." + __name__)

        self._connected = False
        self._lock = threading.Lock()

    def ensure_connected(self) -> None:
        """
        Ensures that the connection to the underlying service is active.

        This method should be called before any other method to ensure that the connection is active.
        If the connection is not active, it will try to connect with a retry mechanism.

        :return: None
        """
        if self._connected:
            return

        with self._lock:
            if self._connected:
                return

            self.logger.warning(f"{self.name}: Connection not active, trying to connect")
            self._connect_with_retry()

    def _connect_with_retry(self):
        """
        Tries to connect to the underlying service with a retry mechanism.

        This method will try to connect up to the specified maximum number of retries.
        If the connection fails after the maximum number of retries, it will wait for 120 seconds and then retry.

        :return: None
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                self._connect()
                self._connected = True
                self.logger.info(f"{self.name}: Connected")
                return
            except Exception as e:
                self.logger.error(
                    f"{self.name}: Connection failed (attempt {attempt}/{self.max_retries}): {e}"
                )
                time.sleep(self.retry_delay)

        self.logger.error(f"{self.name}: Connection failed after {self.max_retries} attempts")
        time.sleep(120)
        self._connect_with_retry()


    def mark_disconnected(self):
        """
        Marks the connection as disconnected.

        This method can be used to manually mark the connection as disconnected, which
        will prevent any further attempts to connect or use the connection.

        :return: None
        """
        self.logger.warning(f"{self.name}: Marked as disconnected")
        self._connected = False

    @abstractmethod
    def _connect(self) -> None:
        """
        Establishes a connection to the underlying service.

        This method should be implemented by subclasses to define how to connect to the underlying service.

        :return: None
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Closes the connection to the underlying service.

        This method should be implemented by subclasses to define how to close the connection to the underlying service.

        :return: None
        """
        pass
