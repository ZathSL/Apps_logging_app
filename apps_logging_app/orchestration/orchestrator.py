from abc import ABC, abstractmethod
import logging
import time
import threading

class BaseOrchestrator(ABC):
    """
    Abstract base class for orchestrating connection-managed resources with retry logic.

    This class provides a framework for managing connections to resources such as databases,
    message queues, or other services that require reliable connectivity. 
    It handles:
    - Thread-safe connection attempts
    - Automatic retries with configurable delay
    - Logging of connection status and errors


    Subclasses must implement the abstract methods `_is_connected`, `_connect`, and `close`
    to provide resource-specific connection handling.
    
    Attributes:
        name (str): Human-readable name of the orchestrator, used in logs.
        max_retries (int): Maximum number of attempts to establish a connection.
        retry_delay (int): Delay in seconds between retry attempts (default is 5).
        logger (logging.Logger): Logger instance for connection events and errors.
        _connected (bool): Internal flag indicating whether the resource is currently connected.
        _lock (threading.Lock): Thread lock to ensure safe concurrent access during connection attempts.
    
        
    """
    def __init__(self, name: str, max_retries: int, retry_delay: int = 5):
        """
        Initializes the BaseOrchestrator with connection settings and logging.

        Sets up the orchestrator's name, maximum retry attempts, retry delay, logger,
        and internal state for managing connections. Also initializes a thread lock
        to ensure thread-safe connection operations.

        Args:
            name (str): Human-readable name of the orchestrator, used in log messages.
            max_retries (int): Maximum number of times to retry a failed connection.
            retry_delay (int, optional): Number of seconds to wait between retries. Defaults to 5.
        
        """
        self.name = name
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logging.getLogger("__main__." + __name__)

        self._connected = False
        self._lock = threading.Lock()

    def ensure_connected(self) -> None:
        """
        Ensures that the resource is connected, establishing a connection if needed.

        This method checks the internal `_connected` flag and, if the resource is not
        connected, acquires a thread lock to safely attempt a connection. It calls
        `_connect_with_retry` to handle retries and logging.

        Thread Safety:
            Uses a lock to prevent concurrent connection attempts by multiple threads.

        Logs:
            - Warning if the connection is not active and a retry will be attempted.

        Raises:
            Exception: Propagates any exceptions raised by `_connect_with_retry` if all retry attempts fail.
        
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
        Attempts to establish a connection with retry logic.

        Tries to connect up to `max_retries` times by calling the abstract `_connect` method.
        Logs each attempt, including successes and failures. Waits `retry_delay` seconds
        between failed attempts. If all retries fail, waits 120 seconds and recursively
        retries until a connection is established.

        Behavior:
            - Sets `_connected` to True upon a successful connection.
            - Logs info on successful connection and errors on failure.
            - Uses a recursive retry strategy after exhausting all attempts.

        Raises:
            Exception: Any exceptions raised by `_connect` are logged but retried. This method may block indefinitely if the connection cannot be established.
        
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
        Marks the resource as disconnected if it is no longer active.

        Checks the current connection status using `_is_connected()`. If the resource
        is still connected, logs an informational message and does nothing. Otherwise,
        sets the internal `_connected` flag to False and logs a warning.

        Logs:
            - Info: When the connection is still active and no disconnection occurs.
            - Warning: When the resource is marked as disconnected.

        Side Effects:
            - Updates the `_connected` attribute to False if the resource is not connected.
        
        """
        if self._is_connected():
            self.logger.info(f"{self.name}: Connection is still active, not marking as disconnected")
            return 
        
        self.logger.warning(f"{self.name}: Marked as disconnected")
        self._connected = False

    @abstractmethod
    def _is_connected(self) -> bool:
        """
        Checks whether the resource is currently connected.

        This abstract method must be implemented by subclasses to return the
        connection status of the underlying resource.

        Returns:
            bool: `True` if the resource is connected, `False` otherwise.
        
        """
        return False

    @abstractmethod
    def _connect(self) -> None:
        """
        Establishes a connection to the resource.

        This abstract method must be implemented by subclasses to handle the
        specific logic required to connect to the underlying resource. It is called
        by `_connect_with_retry` during connection attempts.

        Raises:
            Exception: Any exception raised during the connection attempt should be propagated to allow retry logic to handle it.
        
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Closes the connection to the resource and releases any associated resources.

        This abstract method must be implemented by subclasses to perform the
        necessary cleanup when the orchestrator is finished using the resource.
        It ensures that connections are properly terminated and resources are freed.

        Raises:
            Exception: Any exception raised during closure should be propagated.
        
        """
        pass
