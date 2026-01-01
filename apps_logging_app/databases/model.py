from pydantic import BaseModel, field_validator
from typing import Optional, Dict, Any
import re
from concurrent.futures import Future


class QueryTask:
    """
    Represents a single database query execution task.

    The :class:`QueryTask` encapsulates a query string, optional parameters,
    and a :class:`concurrent.futures.Future` used to retrieve the asynchronous
    execution result.

    Instances of this class are enqueued by :class:`BaseDatabase` and
    processed by the database dispatcher and worker threads.

    :param query: The SQL or database query string to be executed.
    :type query: str
    :param params: Optional dictionary of parameters to be passed to the query.
    :type params: Optional[Dict[str, Any]]
    """
    def __init__(self, query: str, params: Optional[Dict[str, Any]] = None):
        """
        Initializes a QueryTask object with a query and optional parameters.

        :param query: The SQL or database query string to be executed.
        :type query: str
        :param params: Optional dictionary of parameters to be passed to the query.
        :type params: Optional[Dict[str, Any]]
        :return: A QueryTask object that can be used to enqueue a query and retrieve its result.
        :rtype: QueryTask
        """
        self.query = query
        self.params = params
        self.future: Future = Future()
        self.retries = 0

class ConnectionConfig(BaseModel):
    """
    Configuration model defining a database connection endpoint.

    This model represents connection details for a database instance,
    including host, port, and optional service name. It is used for both
    primary and replica connections.

    :param host: Hostname or IP address of the database server.
    :type host: str
    :param port: Network port of the database server.
    :type port: int
    :param service_name: Optional service or database name.
    :type service_name: Optional[str]
    """
    host: str
    port: int
    service_name: Optional[str]

    @field_validator('host')
    def validate_host(cls, v):
        """
        Validates the host field of the ConnectionConfig model.

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
        Validates the port field of the ConnectionConfig model.

        :param v: The value of the port field.
        :raises ValueError: If the port is not an integer between 1 and 65535.
        :return: The validated port value.
        """
        if not isinstance(v, int) or not 1 <= v <= 65535:
            raise ValueError('Port must be an integer between 1 and 65535')
        return v

class BaseDatabaseConfig(BaseModel):
    """
    Base configuration model for database instances.

    This model defines connection credentials, primary and replica
    endpoints, and execution parameters controlling concurrency and
    retry behavior.

    It is validated when database instances are created by the
    :class:`DatabaseFactory`.

    :param type: Type identifier of the database.
    :type type: str
    :param name: Name of the database instance.
    :type name: str
    :param username: Username used for authentication.
    :type username: str
    :param password: Password used for authentication.
    :type password: str
    :param primary: Primary database connection configuration.
    :type primary: ConnectionConfig
    :param replica: Optional replica database connection configuration.
    :type replica: Optional[ConnectionConfig]
    :param max_retries: Maximum number of retries for failed queries.
    :type max_retries: int
    :param max_workers: Maximum number of concurrent worker threads.
    :type max_workers: int
    """
    type: str
    name: str
    username: str
    password: str
    primary: ConnectionConfig
    replica: Optional[ConnectionConfig] = None
    max_retries: int = 5
    max_workers: int = 10

    
    @field_validator('max_retries')
    def validate_max_retries(cls, value):
        """
        Validates the max retries field of the BaseDatabaseConfig model.

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
        Validates the max workers field of the BaseDatabaseConfig model.

        :param value: The value of the max workers field.
        :raises ValueError: If the max workers is less than or equal to 0.
        :return: The validated max workers value.
        """
        if value <= 0:
            raise ValueError("Max workers must be greater than 0")
        return value
