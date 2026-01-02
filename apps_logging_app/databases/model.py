from pydantic import BaseModel, field_validator
from typing import Optional, Dict, Any
import re
from concurrent.futures import Future


class QueryTask:
    """
    Represents a single database query task that can be executed asynchronously.

    This class encapsulates the query string, optional parameters, 
    and manages the result through a `Future` object. It also tracks 
    the number of retry attempts made for the task.

    Attributes:
        query (str): The SQL query string to be executed.
        params (Optional[Dict[str, Any]]): Optional parameters to be used with the query.
        future (Future): A `concurrent.futures.Future` object that will hold the result of the query execution.
        retries (int): The number of times this query has been retried. Defaults to 0.
    """
    def __init__(self, query: str, params: Optional[Dict[str, Any]] = None):
        """
        Initializes a QueryTask instance.

        Args:
            query (str): The SQL query string to be executed.
            params (Optional[Dict[str, Any]]): Optional dictionary of parameters 
                to be used with the query. Defaults to None.

        Attributes:
            query (str): Stores the SQL query string.
            params (Optional[Dict[str, Any]]): Stores the query parameters.
            future (Future): A `concurrent.futures.Future` object that will 
                eventually hold the result of the query execution.
            retries (int): Tracks the number of retry attempts for this task. 
                Initialized to 0.
        """
        self.query = query
        self.params = params
        self.future: Future = Future()
        self.retries = 0

class ConnectionConfig(BaseModel):
    """
    Represents the configuration for a single database connection.

    This class validates connection parameters such as host, port, and 
    optional service name. It ensures that the host is a valid hostname 
    or IP address and that the port is within the valid range.

    Attributes:
        host (str): The hostname or IP address of the database server.
        port (int): The port number on which the database server is listening.
        service_name (Optional[str]): An optional service name for the database.
    """
    host: str
    port: int
    service_name: Optional[str]

    @field_validator('host')
    def validate_host(cls, v):
        """
        Validates the `host` field of the ConnectionConfig.

        Ensures that the host is not empty and matches a basic pattern for 
        valid hostnames or IP addresses.

        Args:
            cls (Type[ConnectionConfig]): The class being validated.
            v (str): The host value to validate.

        Returns:
            str: The validated host value.

        Raises:
            ValueError: If the host is empty or does not match a valid 
                hostname/IP pattern.
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
        Validates the `port` field of the ConnectionConfig.

        Ensures that the port is an integer and falls within the valid 
        range of 1 to 65535.

        Args:
            cls (Type[ConnectionConfig]): The class being validated.
            v (int): The port value to validate.

        Returns:
            int: The validated port value.

        Raises:
            ValueError: If the port is not an integer or is outside the 
                range 1-65535.
        """
        if not isinstance(v, int) or not 1 <= v <= 65535:
            raise ValueError('Port must be an integer between 1 and 65535')
        return v

class BaseDatabaseConfig(BaseModel):
    """
    Represents the configuration for a database, including authentication,
    primary and optional replica connections, and execution settings.

    This class ensures that all critical database parameters are valid
    and provides default values for retry and worker limits. It supports
    both primary and optional replica connections.

    Attributes:
        type (str): The type of the database (e.g., "PostgreSQL", "MySQL").
        name (str): The name of the database.
        username (str): The username for database authentication.
        password (str): The password for database authentication.
        primary (ConnectionConfig): Configuration for the primary database connection.
        replica (Optional[ConnectionConfig]): Optional configuration for a replica connection. Defaults to None.
        max_retries (int): Maximum number of retry attempts for database operations. Defaults to 5.
        max_workers (int): Maximum number of concurrent workers for executing queries. Defaults to 10.
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
        Validates the `max_retries` field of the BaseDatabaseConfig.

        Ensures that the maximum number of retry attempts is greater than 0.

        Args:
            cls (Type[BaseDatabaseConfig]): The class being validated.
            value (int): The number of maximum retries to validate.

        Returns:
            int: The validated `max_retries` value.

        Raises:
            ValueError: If `max_retries` is less than or equal to 0.
        """
        if value <= 0:
            raise ValueError("Max retries must be greater than 0")
        return value
    
    @field_validator('max_workers')
    def validate_max_workers(cls, value):
        """
        Validates the `max_workers` field of the BaseDatabaseConfig.

        Ensures that the maximum number of concurrent workers is greater than 0.

        Args:
            cls (Type[BaseDatabaseConfig]): The class being validated.
            value (int): The number of maximum workers to validate.

        Returns:
            int: The validated `max_workers` value.

        Raises:
            ValueError: If `max_workers` is less than or equal to 0.
        """
        if value <= 0:
            raise ValueError("Max workers must be greater than 0")
        return value
