from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
import logging
from queue import Queue, Empty
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor, Future
from .model import BaseDatabaseConfig
import time
import random
from .data import Query

class BaseDatabase(ABC):
    """
    Abstract base class for asynchronous, thread-based database clients.

    This class defines the core interface and functionality for handling database connections,
    queuing and dispatching queries, managing retries with exponential backoff, and integrating
    with an orchestrator for connection management. Subclasses must implement the specific
    database operations.

    Attributes:
        config (BaseDatabaseConfig): Configuration object containing database settings.
        logger (logging.Logger): Logger instance for database operations and query handling.
        _queue (Queue): Thread-safe queue holding pending `Query` objects.
        _stop_event (Event): Event used to signal the dispatcher thread to stop.
        _executor (ThreadPoolExecutor): Executor for running queries concurrently.
        _dispatcher (Thread): Thread responsible for pulling queries from the queue and dispatching them.
        orchestrator: Reference to an orchestrator managing database connections (set externally).
    """

    def __init__(self, config: BaseDatabaseConfig) -> None:
        """
        Initializes the BaseDatabase instance with configuration and sets up
        internal components for asynchronous query handling.

        Args:
            config (BaseDatabaseConfig): Configuration object containing database
                connection parameters, maximum workers, and retry settings.

        Initializes the following internal components:
            - `logger`: Logger instance scoped to the module for logging database events.
            - `_queue`: Thread-safe queue for pending `Query` objects.
            - `_stop_event`: Event used to signal the dispatcher thread to stop.
            - `_executor`: ThreadPoolExecutor for concurrent query execution, limited
            by `config.max_workers`.
            - `_dispatcher`: Daemon thread responsible for dispatching queries from
            the queue to the executor.
            - `orchestrator`: Initially set to None; intended to manage database
            connection state externally.

        Logs an informational message indicating that the database has been initialized.
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
        Starts the dispatcher thread to process queued database queries.

        This method launches the internal `_dispatcher` daemon thread, which
        continuously monitors the query queue (`_queue`) and submits queries
        to the thread pool executor (`_executor`) for asynchronous execution.

        Once started, the dispatcher thread will keep running until `stop()`
        is called, allowing the database to handle incoming queries concurrently.
        """
        self._dispatcher.start()


    def enqueue_query(self, query: Query) -> Future[List[Dict[str, Any]]]:
        """
        Adds a database query to the internal queue for asynchronous execution.

        Args:
            query (Query): The query object containing the SQL (or equivalent) statement,
                parameters, and a `Future` to hold the result.

        Returns:
            Future[List[Dict[str, Any]]]: A `Future` object that will eventually
            contain the query result as a list of dictionaries, or an exception
            if the query fails.

        Raises:
            Exception: If the query cannot be added to the queue, e.g., if the
            queue is full or another error occurs during insertion.

        Notes:
            - The query is processed asynchronously by the dispatcher thread and
            executed using the thread pool executor.
            - The returned `Future` allows callers to wait for or retrieve the
            result once the query has been executed.
        """
        try:
            self._queue.put(query)
            return query.future
        except Exception as e:
            self.logger.error(f"Client DB {self.config.type}-{self.config.name}: Queue full or error putting query: {e}")
            raise

    def stop(self, timeout: float | None = None) -> None:
        """
        Gracefully stops the database dispatcher and shuts down resources.

        This method signals the internal dispatcher thread to stop, waits for
        all queued queries to complete, shuts down the thread pool executor,
        closes the database connection, and logs the shutdown event.

        Args:
            timeout (float | None, optional): Maximum time in seconds to wait
                for the dispatcher thread to finish. If `None`, waits indefinitely.

        Notes:
            - After calling this method, the database instance is no longer able
            to accept new queries until restarted.
            - Ensures that all pending queries in `_queue` are completed before
            shutting down the executor and closing the connection.
        """
        self._stop_event.set()
        self._dispatcher.join(timeout=timeout)
        self._queue.join()
        self._executor.shutdown(wait=True)
        self.close()
        self.logger.info(f"Database {self.config.type}-{self.config.name} shut down")

    @abstractmethod
    def is_connected(self) -> bool:
        """
        Checks whether the database connection is currently active.

        This method must be implemented by subclasses to return the connection
        status of the specific database client.

        Returns:
            bool: `True` if the database is connected and ready to execute queries,
                `False` otherwise.

        Notes:
            - This method is intended to be used internally by the dispatcher and
            orchestrator to ensure that queries are only executed when a valid
            connection exists.
            - The default implementation returns `False` but should be overridden
            in concrete subclasses.
        """
        return False

    @abstractmethod
    def connect(self) -> None:
        """
        Establishes a connection to the database.

        This method must be implemented by subclasses to perform the necessary
        operations to connect to the specific database type (e.g., opening a
        network connection, authenticating, initializing a session).

        Raises:
            Exception: If the connection cannot be established.

        Notes:
            - This method is typically called by the orchestrator or the dispatcher
            to ensure the database is connected before executing queries.
            - Subclasses should handle connection-specific errors and may implement
            reconnection logic as needed.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Closes the database connection and releases any associated resources.

        This method must be implemented by subclasses to properly clean up
        connections, sessions, or any other resources allocated during
        `connect()` or query execution.

        Notes:
            - Called automatically by `stop()` to ensure a graceful shutdown.
            - Subclasses should ensure that all pending operations are completed
            or safely terminated before closing the connection.
            - Any exceptions raised during closure should be handled or logged
            appropriately to avoid disrupting the shutdown process.
        """
        pass

    # INTERNALS
    
    def _dispatch(self) -> None:
        """
        Continuously processes queries from the queue and submits them to the executor.

        This internal method runs in the `_dispatcher` daemon thread. It monitors
        the internal `_queue` for incoming `Query` objects and executes them
        asynchronously using the `_executor` thread pool.

        For each query:
            - Ensures the database connection via the `orchestrator`.
            - Submits the query to `_query` for execution.
            - Attaches a callback (`_callback`) to handle the result, exceptions,
            and retry logic.

        Retry Logic:
            - If a query fails, it checks `task.retries` against `config.max_retries`.
            - Retries the query with exponential backoff and random jitter.
            - Marks the orchestrator as disconnected if an error occurs.
            - Sets the exception on the `Query.future` if maximum retries are reached.

        Notes:
            - This method is intended to be run in a separate thread and should
            not be called directly by external code.
            - Query completion, retry handling, and logging are all managed
            internally via the `_callback` function.
            - `_queue.task_done()` is called after each query to signal completion
            to `Queue.join()`.
        """
        self.orchestrator.ensure_connected()

        while not self._stop_event.is_set():
            try:
                task: Query = self._queue.get(timeout=0.5)
            except Empty:
                continue

            self.orchestrator.ensure_connected()
            future = self._executor.submit(self._query, task)
            self.logger.info(f"Client DB {self.config.type}-{self.config.name}: Query dispatched: {task.query}")
           
            def _callback(f, task=task):
                """
                Callback function executed when a query future completes.

                Handles the result, exceptions, and retry logic for a single `Query`.

                Args:
                    f (Future): The `Future` object returned by the thread pool executor
                        for the submitted query.
                    task (Query): The query object associated with this future.

                Behavior:
                    - If the query succeeds, sets the result on `task.future` and logs completion.
                    - If the query fails:
                        - Logs a warning and marks the orchestrator as disconnected.
                        - Checks if the query can be retried based on `task.retries` and
                        `config.max_retries`.
                        - Retries the query with exponential backoff plus random jitter.
                        - If maximum retries are reached, sets the exception on `task.future`.
                    - Calls `_queue.task_done()` in the `finally` block to signal that
                    the query has been processed, regardless of success or failure.

                Notes:
                    - This function is intended to be used as a callback for `Future.add_done_callback`.
                    - Retry delays use the formula `2 ** retries + random.uniform(0, 10)` seconds.
                    - Ensures that the queue and future are always properly updated.
                """
                try:
                    result = f.result()
                    task.future.set_result(result)
                    self.logger.info(f"Client DB {self.config.type}-{self.config.name}: Query completed: {task.query}")
                except Exception as e:
                    self.logger.warning(f"Client DB {self.config.type}-{self.config.name}: Query failed: {task.query}: {e}")
                    self.orchestrator.mark_disconnected()

                    retries = task.retries

                    if retries < self.config.max_retries:
                        task.retries += 1
                        backoff = (2 ** retries) + random.uniform(0, 10)
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
    def _query(self, task: Query) -> List[Dict[str, Any]]:
        """
        Executes a single database query and returns the results.

        This method must be implemented by subclasses to perform the actual
        query execution for the specific database type. It is called internally
        by the dispatcher in a worker thread.

        Args:
            task (Query): The query object containing the SQL (or equivalent)
                statement, parameters, and metadata such as retry count.

        Returns:
            List[Dict[str, Any]]: The result of the query as a list of dictionaries,
            where each dictionary represents a row.

        Raises:
            Exception: If the query execution fails, which triggers retry logic
                in the dispatcher.

        Notes:
            - This method should not handle retries or queue management; those
            responsibilities are handled by `_dispatch` and `_callback`.
            - Must be thread-safe, as it may be executed concurrently in multiple
            threads by the thread pool executor.
        """
        pass
