from abc import ABC, abstractmethod
from pydantic import BaseModel, field_validator
import logging
from typing import Dict, Any
from queue import Queue, Empty
import threading

class BaseProducerConfig(BaseModel):
    type: str
    name: str
    max_retries: int = 5


    @field_validator('type')
    def validate_type(cls, value):
        """
        Validate that the type of the producer is not None.

        Raises:
            ValueError: If the type is None.
        """
        if value is None:
            raise ValueError("Type cannot be None")
        return value
    
    @field_validator('name')
    def validate_name(cls, value):
        """
        Validate that the name of the producer is not None.

        Raises:
            ValueError: If the name is None.
        """
        if value is None:
            raise ValueError("Name cannot be None")
        return value
    
    @field_validator('max_retries')
    def validate_max_retries(cls, value):
        """
        Validate that the max retries of the producer is greater than 0.

        Raises:
            ValueError: If the max retries is less than or equal to 0.
        """
        
        if value <= 0:
            raise ValueError("Max retries must be greater than 0")
        return value

class BaseProducer(ABC):

    def __init__(self, config: BaseProducerConfig):
        """
        Initialize the BaseProducer with the given config.

        Args:
            config (BaseProducerConfig): The config for the producer.

        Initializes the producer with the given config and starts the worker thread.
        """
        self.config = config
        self.logger = logging.getLogger("__main__." +__name__)
        self.logger.info(f"Initialized producer: {self.config.type}-{self.config.name}")
        self._queue = Queue()
        self._stop_event = threading.Event()
        self._worker_thread = threading.Thread(
            target=self._worker,
            name=f"{self.config.type}-{self.config.name}-producer-worker",
            daemon=True
        )

        self._worker_thread.start()

    # PUBLIC API

    def produce(self, is_error: bool, message: Dict[str, Any]) -> None:
        """
        Produce a message to the producer.

        Args:
            is_error (bool): Whether the message is an error or not.
            message (Dict[str, Any]): The message to produce.

        Raises:
            Exception: If the queue is full or there is an error while putting the message in the queue.
        """
        try:
            self._queue.put({"is_error": is_error, "message": message, "retries": 0})
            self.logger.info(f"Producer {self.config.type}-{self.config.name}: Message put in queue")
        except Exception as e:
            self.logger.error(f"Producer {self.config.type}-{self.config.name}: Queue full or error putting message: {e}")
            raise

    def shutdown(self, timeout: float | None = None) -> None:
        """
        Shutdown the producer.

        This method sets the stop event, closes the underlying connection and
        waits for the worker thread to finish. If a timeout is provided, it
        will wait for the specified amount of time before exiting.

        Args:
            timeout (float | None, optional): The timeout in seconds to wait
                for the worker thread to finish. Defaults to None.

        """
        self.logger.info(f"Producer {self.config.type}-{self.config.name}: Shutting down producer")
        self._stop_event.set()
        self._close()
        self._worker_thread.join(timeout=timeout)
        self.logger.info(f"Producer {self.config.type}-{self.config.name}: Producer shut down")

    # INTERNALS

    def _worker(self) -> None:
        """
        The worker thread for the producer.

        This method is responsible for connecting to the underlying connection,
        getting messages from the queue, sending them to the underlying connection and
        handling any errors that occur.

        If the queue is empty, it will wait for 0.5 seconds before checking again.
        If an error occurs while sending a message, it will retry the message up to
        the specified max retries. If the max retries is exceeded, it will close the
        underlying connection, wait for 60 seconds and then reconnect.

        Once the stop event is set, this method will exit and wait for the worker
        thread to finish before returning.

        Raises:
            Exception: If an error occurs while connecting to the underlying connection
                or sending a message.
        """
        try:
            self._safe_connect()
            self.logger.info(f"Started producer: {self.config.type}-{self.config.name}-producer-worker")
        except Exception as e:
            self.logger.error(f"Producer {self.config.type}-{self.config.name}: Failed to connect: {e}")
        
        while not self._stop_event.is_set():
            try:
                payload = self._queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                self._send(payload['is_error'], payload['message'])
            except Exception:
                self.logger.exception(f"Producer {self.config.type}-{self.config.name}: Failed to send message")
                if payload['retries'] < self.config.max_retries:
                    payload['retries'] += 1
                    self.logger.warning(f"Producer {self.config.type}-{self.config.name}: Retrying message")
                else:
                    payload['retries'] = 0
                    self.logger.error(f"Producer {self.config.type}-{self.config.name}: Message retries exceeded")
                    self.logger.error(f"Producer {self.config.type}-{self.config.name}: Trying to reconnect..")
                    self._safe_connect()
                
                self._queue.put(payload)
            finally:
                self._queue.task_done()

    def _safe_connect(self) -> None:
        """
        Tries to connect to the underlying connection. If an error occurs while connecting, it will retry the connection up to the specified max retries. If the max retries is exceeded, it will wait for 60 seconds and then reconnect.

        Raises:
            Exception: If an error occurs while connecting to the underlying connection.
        """
        try:
            self._connect()
        except Exception as e:
            self.logger.error(f"Producer {self.config.type}-{self.config.name}: Failed to connect: {e}")
            for i in range(self.config.max_retries):
                try:
                    self.logger.error(f"Producer {self.config.type}-{self.config.name}: Trying to reconnect..")
                    self._connect()
                    break
                except Exception as e:
                    self.logger.error(f"Producer {self.config.type}-{self.config.name}: Failed to connect: {e}")
                    if i == self.config.max_retries - 1:
                        import time
                        self.logger.error(f"Producer {self.config.type}-{self.config.name}: Waiting 60 seconds..")
                        time.sleep(60)



    @abstractmethod
    def _connect(self) -> None:
        """
        Connects to the underlying connection.

        Raises:
            Exception: If an error occurs while connecting to the underlying connection.
        """
        pass

    @abstractmethod
    def _send(self, is_error: bool, message: Dict[str, Any]) -> None:
        """
        Sends a message to the underlying connection.

        Args:
            is_error (bool): Whether the message is an error or not.
            message (Dict[str, Any]): The message to send.

        Raises:
            Exception: If an error occurs while sending the message to the underlying connection.
        """
        pass
    
    @abstractmethod
    def _close(self) -> None:
        """
        Closes the underlying connection.

        Raises:
            Exception: If an error occurs while closing the underlying connection.
        """
        pass