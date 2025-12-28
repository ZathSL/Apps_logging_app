from abc import ABC, abstractmethod
from pydantic import BaseModel, field_validator
from typing import Optional, Dict, Any
import re
import logging
from queue import Queue, Empty
from threading import Thread, Event
from concurrent.futures import Future, ThreadPoolExecutor



class QueryTask:

    """
    Represents a database query task to be executed asynchronously.

    ``QueryTask`` encapsulates a SQL or database query along with optional
    parameters and manages its execution state through a Future object.

    :param query: The SQL or database query string to be executed.
    :type query: str
    :param params: Optional dictionary of parameters to be passed to the query.
    :type params: Optional[Dict[str, Any]]

    Attributes:
        future (Future): A concurrent.futures.Future object that will hold
                         the result or exception of the query once executed.
        retries (int): Counter for the number of times the query has been
                       retried due to failure. Defaults to 0.

    Example Usage:

    .. code-block:: python

        task = QueryTask(
            query="SELECT * FROM users WHERE id = %(user_id)s",
            params={"user_id": 42}
        )

        future_result = task.future  # Will eventually contain query results
    """

    def __init__(self, query: str, params: Optional[Dict[str, Any]] = None):
        """
        Initialize a QueryTask with a query and optional parameters.

        :param query: Query to be executed.
        :param params: Optional parameters to be passed to the query.
        :ivar future: A Future object that will store the result of the query.
        :ivar retries: The number of times the query has been retried due to a failure.
        """
        self.query = query
        self.params = params
        self.future: Future = Future()
        self.retries = 0

class ConnectionConfig(BaseModel):

    """
    Configuration model for a single database connection.

    ``ConnectionConfig`` defines the connection parameters required to
    connect to a database instance, including host, port, and an optional
    service name or database identifier.

    :param host: Hostname or IP address of the database server.
    :type host: str
    :param port: Port number on which the database server is listening.
                 Must be between 1 and 65535.
    :type port: int
    :param service_name: Optional service name, schema, or database name.
    :type service_name: Optional[str]

    Validators:
        - host: Must be a non-empty valid hostname or IP address.
        - port: Must be an integer between 1 and 65535.

    Example Usage:

    .. code-block:: python

        primary_conn = ConnectionConfig(
            host="localhost",
            port=5432,
            service_name="main_db"
        )

        replica_conn = ConnectionConfig(
            host="replica.local",
            port=5432
        )
    """

    host: str
    port: int
    service_name: Optional[str]

    @field_validator('host')
    def validate_host(cls, v):
        """
        Validate the host field of the ConnectionConfig model.

        :param v: The value of the host field.
        :raises ValueError: If the host is empty or not a valid hostname or IP address.
        :return: The validated host value.
        """
        if not v or not v.strip():
            raise ValueError('Host cannot be empty')
        # Basic validation for hostname or IP
        if not re.match(r'^[a-zA-Z0-9.-]+$', v):
            raise ValueError('Host must be a valid hostname or IP address')
        return v

    @field_validator('port')
    def validate_port(cls, v):
        """
        Validate the port field of the ConnectionConfig model.

        :param v: The value of the port field.
        :raises ValueError: If the port is not an integer between 1 and 65535.
        :return: The validated port value.
        """
        if not isinstance(v, int) or not 1 <= v <= 65535:
            raise ValueError('Port must be an integer between 1 and 65535')
        return v

class BaseDatabaseConfig(BaseModel):

    """
    Configuration model for a database client.

    ``BaseDatabaseConfig`` stores all the necessary connection and operational 
    parameters to initialize a database client. This includes credentials, 
    primary and replica connection details, retry policies, and concurrency settings.

    :param type: Type of the database (e.g., "postgres", "mysql").
    :type type: str
    :param name: Unique name for the database configuration.
    :type name: str
    :param username: Username used to authenticate with the database.
    :type username: str
    :param password: Password used to authenticate with the database.
    :type password: str
    :param primary: Connection configuration for the primary database instance.
    :type primary: ConnectionConfig
    :param replica: Optional connection configuration for a replica database instance.
    :type replica: Optional[ConnectionConfig]
    :param max_retries: Maximum number of times to retry a failed connection.
                        Defaults to 5.
    :type max_retries: int
    :param max_workers: Maximum number of worker threads for executing queries.
                        Defaults to 10.
    :type max_workers: int

    Validators:
        - type: Ensures the type is not None.
        - name: Ensures the name is not None.
        - username: Ensures the username is not None.
        - password: Ensures the password is not None.
        - max_retries: Must be greater than 0.
        - max_workers: Must be greater than 0.

    Example Usage:

    .. code-block:: python

        primary_conn = ConnectionConfig(host="localhost", port=5432, service_name="main_db")
        replica_conn = ConnectionConfig(host="replica.local", port=5432, service_name="replica_db")

        db_config = BaseDatabaseConfig(
            type="postgres",
            name="main",
            username="user",
            password="password",
            primary=primary_conn,
            replica=replica_conn,
            max_retries=5,
            max_workers=10
        )
    """

    type: str
    name: str
    username: str
    password: str
    primary: ConnectionConfig
    replica: Optional[ConnectionConfig] = None
    max_retries: int = 5
    max_workers: int = 10

    @field_validator('type')
    def validate_type(cls, value):
        """
        Validate the type field of the BaseDatabaseConfig model.

        :param value: The value of the type field.
        :raises ValueError: If the type is None.
        :return: The validated type value.
        """
        if value is None:
            raise ValueError("Type cannot be None")
        return value
    
    @field_validator('name')
    def validate_name(cls, value):
        """
        Validate the name field of the BaseDatabaseConfig model.

        :param value: The value of the name field.
        :raises ValueError: If the name is None.
        :return: The validated name value.
        """
        if value is None:
            raise ValueError("Name cannot be None")
        return value

    @field_validator('username')
    def validate_username(cls, value):
        """
        Validate the username field of the BaseDatabaseConfig model.

        :param value: The value of the username field.
        :raises ValueError: If the username is None.
        :return: The validated username value.
        """
        if value is None:
            raise ValueError("Username cannot be None")
        return value
    
    @field_validator('password')
    def validate_password(cls, value):
        """
        Validate the password field of the BaseDatabaseConfig model.

        :param value: The value of the password field.
        :raises ValueError: If the password is None.
        :return: The validated password value.
        """
        if value is None:
            raise ValueError("Password cannot be None")
        return value
    
    @field_validator('max_retries')
    def validate_max_retries(cls, value):
        """
        Validate the max retries field of the BaseDatabaseConfig model.

        :param value: The value of the max retries field.
        :raises ValueError: If the max retries is less than or equal to 0.
        :return: The validated max retries value.
        """
        if value <= 0:
            raise ValueError("Max retries must be greater than 0")
        return value
    
    @field_validator('max_workers')
    def validate_max_workers(cls, value):
        """
        Validate the max workers field of the BaseDatabaseConfig model.

        :param value: The value of the max workers field.
        :raises ValueError: If the max workers is less than or equal to 0.
        :return: The validated max workers value.
        """
        if value <= 0:
            raise ValueError("Max workers must be greater than 0")
        return value

class BaseDatabase(ABC):

    """
    Abstract base class for database clients.

    ``BaseDatabase`` provides a framework for executing queries asynchronously
    against a database. It manages a queue of query tasks, executes them in a
    thread pool, handles retries and reconnections, and logs all relevant events.

    This class is designed to be extended for specific database types by
    implementing the abstract methods ``_connect`` and ``_query``.

    :param config: The database configuration containing connection details, credentials, 
                   and operational parameters.
    :type config: BaseDatabaseConfig

    Attributes:
        config (BaseDatabaseConfig): The database configuration object.
        logger (logging.Logger): Logger for database events.
        _queue (Queue): Internal queue of pending query tasks.
        _stop_event (Event): Event used to signal shutdown of the dispatcher thread.
        _executor (ThreadPoolExecutor): Thread pool for executing queries.
        _dispatcher (Thread): Background thread that dispatches query tasks.
    
    Public Methods:
        execute(query, params=None) -> Future:
            Adds a query to the execution queue and returns a Future for its result.
        
        shutdown() -> None:
            Shuts down the dispatcher thread, waits for all tasks to finish, 
            and shuts down the executor.

    Abstract Methods:
        _connect() -> None:
            Establish a connection to the database. Must be implemented in subclasses.
        
        _query(query, params=None) -> Dict[str, Any]:
            Execute a query on the database and return the result as a dictionary.
            Must be implemented in subclasses.

    Internal Methods:
        _dispatch() -> None:
            Internal loop that pulls query tasks from the queue and submits them
            to the executor for execution.
        
        _safe_query(task: QueryTask) -> Dict[str, Any]:
            Executes a query safely, handling exceptions and logging results.
        
        _safe_connect() -> None:
            Attempts to establish a database connection with retries and error handling.

    Example Usage:

    .. code-block:: python

        class MyDatabase(BaseDatabase):
            def _connect(self):
                # Implementation for connecting to the specific database
                pass

            def _query(self, query, params=None):
                # Implementation for executing a query
                return {"result": "success"}

        config = BaseDatabaseConfig(
            type="my_db",
            name="main",
            username="user",
            password="pass",
            primary=ConnectionConfig(host="localhost", port=5432)
        )

        db = MyDatabase(config)
        future = db.execute("SELECT * FROM table")
        result = future.result()
        db.shutdown()
    """

    def __init__(self, config: BaseDatabaseConfig):
        """
        Initialize the BaseDatabase with the given config.

        :param config: The config for the database.

        Initializes the database with the given config, sets up the logger, and starts the dispatcher thread.
        """
        self.config = config
        self.logger = logging.getLogger("__main__." +__name__)
        self.logger.info(f"Initialized database: {self.config.type}-{self.config.name}")
        self._queue = Queue()
        self._stop_event = Event()
        self._executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        self._dispatcher = Thread(target=self._dispatch, daemon=True)
        self._dispatcher.start()


    # PUBLIC API

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a query with the given parameters and return the result as a Future.

        The query will be executed in a separate thread, allowing the caller to
        continue without blocking.

        :param query: The query to execute.
        :param params: Optional parameters to pass to the query.
        :return: A Future object that will contain the result of the query.
        """
        task = QueryTask(query, params)
        self._queue.put(task)
        return task.future

    def shutdown(self):
        """
        Shut down the database.

        This method stops the dispatcher thread and waits for it to finish,
        then joins the queue and shuts down the executor, waiting for all
        tasks to finish.

        :return: None
        """
        self._stop_event.set()
        self._dispatcher.join(timeout=5)

        self._queue.join()
        self._executor.shutdown(wait=True)

        self.logger.info(f"Database {self.config.type}-{self.config.name} shut down")
    # INTERNALS

    @abstractmethod
    def _connect(self) -> None:
        """
        Abstract method to connect to the database.

        This method is called by the _dispatch method and should establish a
        connection to the database. It should not return a value.

        :return: None
        """
        pass


    @abstractmethod
    def _query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Abstract method to execute a query on the database.

        This method is called by the dispatch method and should execute the given
        query on the database with the given parameters. It should return a
        dictionary containing the result of the query.

        :param query: The query to be executed.
        :param params: Optional parameters to be passed to the query.
        :return: A dictionary containing the result of the query.
        """
        pass
    
    def _dispatch(self):
        """
        Connects to the database, then enters a loop to process tasks
        from the queue. Each task is dispatched to the executor and
        executed in a separate thread. If the task completes successfully,
        the result is set on the task's future. If the task fails, an
        exception is set on the task's future.

        :return: None
        """
        try:
            self._safe_connect()
            self.logger.info(f"Client DB {self.config.type}-{self.config.name}: Connected")
        except Exception as e:
            self.logger.error(f"Client DB {self.config.type}-{self.config.name}: Failed to connect: {e}")


        while not self._stop_event.is_set():
            try:
                task = self._queue.get(timeout=0.5)
            except Empty:
                continue
            future = self._executor.submit(self._safe_query, task)
            self.logger.info(f"Client DB {self.config.type}-{self.config.name}: Query dispatched: {task.query}")
           
            def _callback(f, task=task):
                try:
                    task.future.set_result(f.result())
                    self.logger.info(f"Client DB {self.config.type}-{self.config.name}: Query completed: {task.query}")
                except Exception as e:
                    task.future.set_exception(e)
                    self.logger.warning(f"Client DB {self.config.type}-{self.config.name}: Query failed: {task.query}: {e}")
                finally:
                    self._queue.task_done()
            
            future.add_done_callback(_callback)

    def _safe_query(self, task: QueryTask) -> Dict[str, Any]:
        """
        Execute a query on the database in a safe manner.

        This method is called by the dispatch method and should execute the given
        query on the database with the given parameters. If the query
        completes successfully, the result is returned as a dictionary. If the
        query fails, an exception is logged and the database connection is
        re-established.

        :param task: The task to be executed, containing the query and
            parameters.
        :return: A dictionary containing the result of the query.
        """
        try:
            result_dict = self._query(task.query, task.params)
            self.logger.info(f"Client DB {self.config.type}-{self.config.name}: Query result: {result_dict}")
            return result_dict
        except Exception as e:
            self.logger.error(f"Client DB {self.config.type}-{self.config.name}: Failed to execute query: {e}")
            self._safe_connect()

    def _safe_connect(self) -> None:
        """
        Tries to connect to the database. If an error occurs while connecting, it will
        retry the connection up to the specified max retries. If the max retries is
        exceeded, it will wait for 60 seconds and then reconnect.

        Raises:
            Exception: If an error occurs while connecting to the underlying connection.
        """
        try:
            self._connect()
        except Exception as e:
            self.logger.error(f"Client DB {self.config.type}-{self.config.name}: Failed to connect: {e}")
            for i in range(self.config.max_retries):
                try:
                    self._connect()
                    self.logger.info(f"Client DB {self.config.type}-{self.config.name}: Connected")
                    break
                except Exception as e:
                    self.logger.error(f"Client DB {self.config.type}-{self.config.name}: Failed to connect: {e}")
                    if i == self.config.max_retries - 1:
                        import time
                        self.logger.error(f"Client DB {self.config.type}-{self.config.name}: Waiting 60 seconds..")
                        time.sleep(60)