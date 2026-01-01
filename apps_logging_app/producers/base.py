from abc import ABC, abstractmethod
import logging
from typing import Dict, Any
from queue import Queue, Empty
from threading import Thread, Event
from .model import BaseProducerConfig
import random 
import time

class BaseProducer(ABC):
    """
    Abstract base class for all producers.

    The `BaseProducer` class defines the interface and common functionality for
    asynchronous message producers that send data to an underlying transport or service.
    It manages connection lifecycle, message queuing, retries, and worker threads.

    Attributes
    ----------
    config : BaseProducerConfig
        The configuration object for the producer, containing type, name, max retries, etc.
    logger : logging.Logger
        Logger used for internal logging of producer events.
    _queue : queue.Queue
        Internal queue that holds messages to be sent.
    _stop_event : threading.Event
        Event used to signal the worker thread to stop.
    _worker_thread : threading.Thread
        Background thread that processes messages from the queue.
    orchestrator : Optional[BaseOrchestrator]
        Optional orchestrator instance responsible for managing the producer's connection.

    Methods
    -------
    start():
        Starts the producer by launching the worker thread.
    enqueue_message(is_error: bool, message: dict):
        Puts a message into the internal queue for asynchronous sending.
    stop(timeout: float | None = None):
        Stops the producer, waits for the worker thread to finish, and closes connections.
    connect():
        Abstract method. Establishes a connection to the underlying transport or service.
    close():
        Abstract method. Closes the connection to the underlying transport or service.
    _worker():
        Internal method run by the worker thread to process messages from the queue.
    _send(is_error: bool, message: Any):
        Abstract method. Sends a message to the underlying transport or service.

    Notes
    -----
    - The producer handles automatic retries with exponential backoff if sending fails.
    - The producer ensures that the orchestrator connection is active before sending messages.
    - Subclasses must implement `connect`, `close`, and `_send` to provide transport-specific behavior.
    """

    def __init__(self, config: BaseProducerConfig):
        """
        Initializes the BaseProducer with the given config.

        :param config: The config for the producer.
        :type config: BaseProducerConfig
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
        Starts the producer.

        This method starts the worker thread which will execute the producer's
        processing loop.

        :return: None
        """
        self._worker_thread.start()

    def enqueue_message(self, is_error: bool, message: Dict[str, Any]) -> None:
        """
        Enqueue a message to be sent by the producer.

        This method puts a message in the internal queue and logs the event.

        Args:
            is_error (bool): Whether the message is an error or not.
            message (Dict[str, Any]): The message to enqueue.

        Raises:
            Exception: If the queue is full or there is an error while putting the message in the queue.
        """
        try:
            self._queue.put({"is_error": is_error, "message": message, "retries": 0})
            self.logger.info(f"Putting message in queue: {message}")
        except Exception as e:
            self.logger.error(f"Producer {self.config.type}-{self.config.name}: Queue full or error putting message: {e}")
            raise

    def stop(self, timeout: float | None = None) -> None:
        """
        Shut down the producer.

        This method sets the stop event, closes the underlying connection, and
        waits for the worker thread to finish (optionally with a timeout).

        Args:
            timeout (float | None, optional): The timeout in seconds to wait
                for the worker thread to finish. Defaults to None.
        """
        self._stop_event.set()
        self._worker_thread.join(timeout=timeout)
        self.close()
        self.logger.info(f"Producer {self.config.type}-{self.config.name}: Producer shut down")

    @abstractmethod
    def connect(self) -> None:
        """
        Establish a connection to the underlying transport or service.

        This method is responsible for allocating any resources required by the producer
        and should be called when the producer is being initialized.

        Subclasses should implement this method to establish a connection to their
        underlying transport or service.

        Raises:
            Exception: If an error occurs while trying to connect to the underlying transport
                or service.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close the underlying connection to the transport or service.

        This method is responsible for cleaning up any resources allocated by the producer
        and should be called when the producer is being shut down.

        :raises: Exception
        """
        pass

    # INTERNALS

    def _worker(self) -> None:

        """
        Internal worker thread that consumes messages from the queue and sends them to the
        underlying transport or service.

        This method is responsible for:

        - Fetching messages from the queue
        - Sending messages via `_send`
        - Retrying messages up to `max_retries`
        - Reconnecting on failures and waiting 60 seconds if maximum retries are exceeded

        This design ensures that the producer operates asynchronously and robustly in the face of
        transient errors.
        """
        self.orchestrator.ensure_connected()

        while not self._stop_event.is_set():
            try:
                payload = self._queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                self.orchestrator.ensure_connected()
                self._send(payload['is_error'], payload['message'])
                self.logger.info(f"Producer {self.config.type}-{self.config.name}: Message sent: {payload['message']}")
            except Exception:
                self.logger.warning(f"Producer {self.config.type}-{self.config.name}: Failed to send message")
                self.orchestrator.mark_disconnected()
                
                retries = payload.get('retries', 0)
                if retries < self.config.max_retries:
                    payload['retries'] = retries + 1
                    backoff = (2 ** retries) + random.uniform(0, 10)
                    self.logger.info(f"Producer {self.config.type}-{self.config.name}: Retrying message in {backoff} seconds: {payload['message']}")
                    time.sleep(backoff)
                    self._queue.put(payload)
                else:
                    self.logger.error(f"Producer {self.config.type}-{self.config.name}: Max retry reached for message: {payload['message']}")
                    raise
            finally:
                self._queue.task_done()

    @abstractmethod
    def _send(self, is_error: bool, message: Any) -> None:
        """
        Sends a message to the underlying transport or service.

        Args:
            is_error (bool): Whether the message is an error or not.
            message (Any): The message to send.

        Raises:
            Exception: If an error occurs while sending the message.
        """
        pass
    
