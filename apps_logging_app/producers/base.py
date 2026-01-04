from abc import ABC, abstractmethod
import logging
from queue import Queue, Empty
from threading import Thread, Event
import random 
import time

from .model import BaseProducerConfig
from .data import Message

class BaseProducer(ABC):
    """
    Abstract base class for message producers with asynchronous processing and retry logic.

    `BaseProducer` provides a framework for sending messages to external systems
    in a reliable, thread-safe manner. It manages an internal message queue,
    processes messages in a background worker thread, and integrates with a
    connection orchestrator to ensure that the producer is connected before sending.

    Features:
        - Threaded message processing with a worker thread.
        - Queue-based message buffering.
        - Automatic retries with exponential backoff for failed messages.
        - Abstract methods for connection management and message sending, allowing
          concrete subclasses to implement specific producer behavior.
        - Logging of all important events, errors, and retry attempts.

    Attributes:
        config (BaseProducerConfig): Configuration object containing producer parameters.
        logger (logging.Logger): Logger for reporting events and errors.
        _queue (Queue): Internal queue holding messages to be sent.
        _stop_event (Event): Event used to signal the worker thread to stop.
        _worker_thread (Thread): Background thread that processes messages from the queue.
        orchestrator: Optional orchestrator used to ensure reliable connectivity.

    Methods:
        start(): Starts the background worker thread for message processing.
        enqueue_message(message): Adds a message to the internal queue.
        stop(timeout=None): Stops the worker thread and closes the producer.
        is_connected(): Abstract method to check connection status.
        connect(): Abstract method to establish a connection.
        close(): Abstract method to cleanly close the producer.
        _send(message): Abstract method to send a message to the target system.
    
    """
    def __init__(self, config: BaseProducerConfig):
        """
        Initializes the BaseProducer with the given configuration.

        Sets up the internal message queue, stop event, and background worker thread
        for asynchronous message processing. Also initializes the logger and
        prepares the producer for integration with an orchestrator.

        Args:
            config (BaseProducerConfig): Configuration object containing producer
                parameters such as type, name, and maximum retries.

        Behavior:
            - Creates a thread-safe queue for storing messages to be sent.
            - Creates an Event used to signal the worker thread to stop.
            - Initializes a daemon thread that runs the `_worker` method for message processing.
            - Sets the `orchestrator` attribute to None (can be assigned later).
            - Logs an informational message indicating the producer has been initialized.
        
        """
        self.config = config
        self.logger = logging.getLogger("__main__." +__name__)
        self._queue = Queue()
        self._stop_event = Event()
        self._worker_thread = Thread(
            target=self._worker,
            name=f"{self.config.type}-{self.config.name}-producer-worker",
            daemon=True
        )
        self.orchestrator = None
        self.logger.info(f"Initialized producer: {self.config.type}-{self.config.name}")


    # PUBLIC API

    def start(self) -> None:
        """
        Starts the background worker thread for processing queued messages.

        Behavior:
            - Launches the `_worker_thread` which continuously consumes messages
                from the internal queue and sends them using the `_send` method.
            - Must be called before enqueueing messages for processing.

        Raises:
            RuntimeError: If the thread fails to start.
        
        """
        self._worker_thread.start()

    def enqueue_message(self, message: Message) -> None:
        """
        Adds a message to the producer's internal queue for asynchronous processing.

        Args:
            message (Message): The message object to be sent by the producer.

        Behavior:
            - Puts the message into the thread-safe internal queue `_queue`.
            - Logs an informational message indicating the message has been queued.

        Raises:
            Exception: Propagates any exception raised while adding the message to the queue,
                such as a full queue or other unexpected errors. Logs an error before raising.
        
        """
        try:
            self._queue.put(message)
            self.logger.info(f"Putting message in queue: {message}")
        except Exception as e:
            self.logger.error(f"Producer {self.config.type}-{self.config.name}: Queue full or error putting message: {e}")
            raise

    def stop(self, timeout: float | None = None) -> None:
        """
        Stops the producer by signaling the worker thread to terminate and closing resources.

        Args:
            timeout (float | None): Maximum time in seconds to wait for the worker thread
                to finish. If `None`, waits indefinitely.

        Behavior:
            - Sets the `_stop_event` to signal the worker thread to stop processing messages.
            - Joins the `_worker_thread`, waiting up to `timeout` seconds for it to finish.
            - Calls the `close()` method to cleanly release resources.
            - Logs an informational message indicating that the producer has been shut down.

        Notes:
            - Any messages remaining in the queue may not be processed if the timeout is reached.
        
        """
        self._stop_event.set()
        self._worker_thread.join(timeout=timeout)
        self.close()
        self.logger.info(f"Producer {self.config.type}-{self.config.name}: Producer shut down")

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Checks whether the producer is currently connected to its target system.

        This method must be implemented by subclasses to provide a mechanism for
        verifying the producer's connection status.

        Returns:
            bool: `True` if the producer is connected and ready to send messages,
            `False` otherwise.

        Notes:
            - The default implementation returns `False`.
            - Subclasses should implement proper connection checks specific to
                the underlying system (e.g., Kafka, HTTP, database).
        
        """
        return False

    @abstractmethod
    def connect(self) -> None:
        """
        Establishes a connection to the producer's target system.

        This method must be implemented by subclasses to initialize any required
        connections or resources needed for sending messages.

        Behavior:
            - Should prepare the producer to start sending messages.
            - May raise exceptions if the connection cannot be established.

        Raises:
            Exception: If the connection attempt fails.
        
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Closes the producer and releases any resources or connections.

        This method must be implemented by subclasses to perform any necessary
        cleanup, such as closing network connections, flushing buffers, or
        terminating threads.

        Behavior:
            - Ensures that the producer is properly shut down.
            - Should be idempotent, allowing multiple calls without adverse effects.

        Raises:
            Exception: If an error occurs during the cleanup process.
        
        """
        pass

    # INTERNALS

    def _worker(self) -> None:
        """
        Background worker method that processes messages from the internal queue.

        Continuously runs in a separate thread, fetching messages from `_queue`,
        ensuring the producer is connected via the orchestrator, and sending messages
        using the `_send` method. Implements automatic retries with exponential
        backoff for failed messages.

        Behavior:
            - Checks connection status using `self.orchestrator.ensure_connected()`.
            - Retrieves messages from `_queue` with a 0.5 second timeout.
            - Attempts to send each message; on failure, logs a warning and marks the
                orchestrator as disconnected.
            - Retries failed messages up to `config.max_retries` using exponential
                backoff with a random jitter.
            - Raises an exception if the maximum retry count is reached.
            - Calls `_queue.task_done()` after processing each message.

        Notes:
            - This method is intended to run in a daemon thread and should not be
                called directly.
            - Ensures that messages are retried in a fault-tolerant manner.
        
        """
        self.orchestrator.ensure_connected()

        while not self._stop_event.is_set():
            try:
                message = self._queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                self.orchestrator.ensure_connected()
                self._send(message)
                self.logger.info(f"Producer {self.config.type}-{self.config.name}: Message sent: {message.message}")
            except Exception:
                self.logger.warning(f"Producer {self.config.type}-{self.config.name}: Failed to send message")
                self.orchestrator.mark_disconnected()
                
                retries = message.retries

                if retries < self.config.max_retries:
                    message.retries = retries + 1
                    backoff = (2 ** retries) + random.uniform(0, 10)
                    self.logger.info(f"Producer {self.config.type}-{self.config.name}: Retrying message in {backoff} seconds: {message.message}")
                    time.sleep(backoff)
                    self._queue.put(message)
                else:
                    self.logger.error(f"Producer {self.config.type}-{self.config.name}: Max retry reached for message: {message.message}")
                    raise
            finally:
                self._queue.task_done()

    @abstractmethod
    def _send(self, message: Message) -> None:
        """
        Sends a single message to the target system.

        This method must be implemented by subclasses to define the actual
        message delivery logic (e.g., sending to Kafka, HTTP endpoint, database).

        Args:
            message (Message): The message object containing the payload, topic,
                and metadata to be sent.

        Behavior:
            - Called by the background `_worker` thread for each message in the queue.
            - Should raise an exception if the message cannot be sent successfully,
                allowing the worker to handle retries.

        Raises:
            Exception: If the message cannot be sent.
        
        """
        pass
    