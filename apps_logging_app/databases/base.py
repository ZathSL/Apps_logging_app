from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
import logging
from queue import Queue, Empty
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor, Future
from .model import BaseDatabaseConfig, QueryTask
import time
import random

class BaseDatabase(ABC):
    """
    Abstract base class for asynchronous database clients.

    The :class:`BaseDatabase` defines a common execution model for database
    interactions based on an internal queue, a dispatcher thread, and a
    thread pool executor. Queries are enqueued and executed asynchronously,
    returning a :class:`concurrent.futures.Future` to the caller.

    This class is responsible for:
    
    - Managing the lifecycle of the database connection
    - Dispatching query tasks from a queue to a worker pool
    - Handling retries with exponential backoff on failures
    - Coordinating connection state via an external orchestrator
    - Providing a uniform interface for different database implementations

    Concrete database implementations must provide:
    
    - Connection management logic
    - Query execution logic

    Subclasses are required to implement the following abstract methods:

    - :meth:`connect`
    - :meth:`close`
    - :meth:`_query`

    Instances of this class are typically created and managed by a
    database factory and used by agents to execute queries asynchronously.

    :param config: Configuration object containing database connection
        parameters and execution settings.
    :type config: BaseDatabaseConfig
    """

    def __init__(self, config: BaseDatabaseConfig) -> None:
        """
        Initializes the BaseDatabase with the given config.

        :param config: The database configuration containing connection details, credentials,
            and operational parameters.
        :type config: BaseDatabaseConfig
        """
        self.config = config
        self.logger = logging.getLogger("__main__." +__name__)
        self._queue = Queue()
        self._stop_event = Event()
        self._executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        self._dispatcher = Thread(target=self._dispatch, daemon=True)
        self.orchestrator = None
        self.logger.info(f"Initialized database: {self.config.type}-{self.config.name}")

    # PUBLIC API

    def start(self) -> None:
        """
        Starts the database dispatcher thread, which picks up queries from the queue
        and submits them to the thread pool executor for execution.
        """        
        self._dispatcher.start()


    def enqueue_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Future[List[Dict[str, Any]]]:
        """
        Enqueues a query to be executed by the database dispatcher thread.

        :param query: The SQL or database query string to be executed.
        :type query: str
        :param params: Optional dictionary of parameters to be passed to the query.
        :type params: Optional[Dict[str, Any]]
        :return: A concurrent.futures.Future object that will hold the result of the query.
        :rtype: Future[List[Dict[str, Any]]]
        :raises: Exception if the queue is full or an error occurs while putting the query.
        """
        try:
            task = QueryTask(query, params)
            self._queue.put(task)
            return task.future
        except Exception as e:
            self.logger.error(f"Client DB {self.config.type}-{self.config.name}: Queue full or error putting query: {e}")
            raise

    def stop(self, timeout: float | None = None) -> None:
        """
        Shuts down the database dispatcher thread, waits for all queued tasks to finish,
        joins the queue, shuts down the executor, and closes the database connection.

        :param timeout: Optional timeout in seconds to wait for the dispatcher thread to finish.
        :type timeout: float | None
        :return: None
        :raises: None
        """
        self._stop_event.set()
        self._dispatcher.join(timeout=timeout)
        self._queue.join()
        self._executor.shutdown(wait=True)
        self.close()
        self.logger.info(f"Database {self.config.type}-{self.config.name} shut down")

    @abstractmethod
    def connect(self) -> None:
        """
        Establish a connection to the database.

        :raises: Exception if connection fails
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Closes the database connection.

        :raises: None
        """
        pass

    # INTERNALS
    
    def _dispatch(self) -> None:
        """
        Internal loop that pulls query tasks from the queue and submits them
        to the executor for execution. Ensures the orchestrator is connected before
        submitting queries. If a query fails, marks the orchestrator as disconnected
        and retries the query up to max_retries times with exponential backoff.
        """
        self.orchestrator.ensure_connected()

        while not self._stop_event.is_set():
            try:
                task = self._queue.get(timeout=0.5)
            except Empty:
                continue

            self.orchestrator.ensure_connected()
            future = self._executor.submit(self._query, task)
            self.logger.info(f"Client DB {self.config.type}-{self.config.name}: Query dispatched: {task.query}")
           
            def _callback(f, task=task):
                try:
                    result = f.result()
                    task.future.set_result(result)
                    self.logger.info(f"Client DB {self.config.type}-{self.config.name}: Query completed: {task.query}")
                except Exception as e:
                    self.logger.warning(f"Client DB {self.config.type}-{self.config.name}: Query failed: {task.query}: {e}")
                    self.orchestrator.mark_disconnected()

                    if not hasattr(task, 'retry_count'):
                        task.retry_count = 0

                    if task.retry_count < self.config.max_retries:
                        task.retry_count += 1
                        backoff = (2 ** task.retry_count) + random.uniform(0, 10)
                        self.logger.info(f"Client DB {self.config.type}-{self.config.name}: Retrying query in {backoff} seconds: {task.query}")
                        time.sleep(backoff)
                        self._queue.put(task)
                    else:
                        self.logger.error(f"Client DB {self.config.type}-{self.config.name}: Max retry reached for query: {task.query}")
                        task.future.set_exception(e)
                finally:
                    self._queue.task_done()
            
            future.add_done_callback(_callback)

    @abstractmethod
    def _query(self, task: QueryTask) -> List[Dict[str, Any]]:
        """
        Internal method that executes a query task on the database.

        :param task: The QueryTask object to be executed.
        :type task: QueryTask
        :return: A list of dictionaries containing the result of the query.
        :rtype: List[Dict[str, Any]]
        :raises: Exception if the query fails
        """
        pass
