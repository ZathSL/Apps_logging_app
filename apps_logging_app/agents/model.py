from pydantic import BaseModel, field_validator, model_validator
from typing import List, Optional, Pattern, Tuple
from pathlib import Path
import re


class PathFileConfig(BaseModel):
    """
    Configuration model for a file path used by an agent.

    This model represents a file that the agent will use, including its
    name, filesystem path, optional cursor, and optional identifier. It
    performs validation to ensure that the name is not None and the path
    exists.

    Attributes:
        name (str): Unique name of the path file. Must not be None.
        path (Path): Filesystem path to the file. Must exist and be a Path object.
        cursor (int, optional): Optional cursor to track progress within the file. Defaults to 0.
        id (int, optional): Optional identifier for the path file.
    """
    name: str
    path: Path
    cursor: int = 0
    id: Optional[int] = None
    
    
    @field_validator('path')
    def validate_path(cls, value) -> Path:
        """
        Validates the `path` field of the PathFileConfig model.

        Ensures that the `path` attribute:
            1. Is not None.
            2. Is a `Path` object.
            3. Points to an existing file or directory in the filesystem.

        This validator is automatically called by Pydantic when creating or
        updating a PathFileConfig instance.

        Args:
            cls: The PathFileConfig class.
            value (Path): The value of the `path` field to validate.

        Returns:
            Path: The validated Path object.

        Raises:
            ValueError: If `path` is None, not a Path object, or does not exist.
        """
        if value is None:
            raise ValueError("Path cannot be None")
        if not isinstance(value, Path):
            raise ValueError("Path must be a Path object")
        if not value.exists():
            raise ValueError("Path does not exist")
        return value

class RegexPatternConfig(BaseModel):
    """
    Configuration model for associating a regex pattern with a specific path file.

    This model represents a regex pattern that will be applied to the contents
    of a file identified by `path_file_name`. The regex pattern is automatically
    compiled if provided as a string.

    Attributes:
        path_file_name (str): Name of the path file that this regex pattern applies to.
        regex_pattern (Pattern[str]): Compiled regular expression pattern used for matching.
    """
    path_file_name: str
    regex_pattern: Pattern[str]
    
    @field_validator('regex_pattern', mode='before')
    def compile_regex(cls, v) -> Pattern[str]:
        """
        Compiles the `regex_pattern` field if it is provided as a string.

        This validator is executed before standard Pydantic validation (`mode='before'`),
        allowing users to provide regex patterns either as strings or pre-compiled
        Pattern objects. If a string is provided, it is compiled into a `Pattern[str]`.

        Args:
            cls: The RegexPatternConfig class.
            v (Union[str, Pattern[str]]): The value of the `regex_pattern` field to validate.

        Returns:
            Pattern[str]: The compiled regex pattern.

        Raises:
            None: This validator does not raise exceptions; invalid regex strings will
                raise an exception when used later in matching operations.
        """
        if isinstance(v, str):
            return re.compile(v)
        return v


class QueryConfig(BaseModel):
    """
    Configuration model for a database query.

    This model represents a query that can be executed by an agent
    or data pipeline. It includes the database type, a name for the query,
    and the query string itself.

    Attributes:
        type (str): Type of the database (e.g., 'postgres', 'mysql').
        name (str): Unique name for the query.
        query (str): The query string to be executed.
    """
    type: str
    name: str
    query: str

class DataConnectionConfig(BaseModel):
    """
    Configuration model for a data connection within a producer.

    This model represents a single data flow from a producer to a destination,
    including error and warning flags, optional source and destination references,
    and an optional expiration time.

    Attributes:
        name (str): Unique name of the data connection.
        is_error (bool): Indicates whether this connection is used for error handling.
        is_warning (bool): Indicates whether this connection is used for warnings.
        source_ref (RegexPatternConfig, optional): Optional reference to a regex
            pattern applied to a path file, used to extract or filter data.
        destination_ref (QueryConfig, optional): Optional reference to a query
            representing the destination database or table.
        expired_time_int (int, optional): Optional expiration time in seconds
            for the data connection.
    """
    name: str
    is_error: bool
    is_warning: bool
    source_ref: RegexPatternConfig = None
    destination_ref: Optional[QueryConfig] = None
    expired_time_int: Optional[int] = None

class ProducerConnectionConfig(BaseModel):
    """
    Configuration model for a producer and its associated data connections.

    This model represents a producer instance that generates or streams data,
    along with all the data connections that it manages. Each data connection
    defines how the produced data is routed, filtered, or sent to a destination.

    Attributes:
        type (str): Type of the producer (e.g., Kafka, RabbitMQ, custom).
        name (str): Unique name of the producer instance.
        topic (str): Messaging topic or stream that the producer writes to.
        data_connections (List[DataConnectionConfig]): List of data connections
            handled by this producer.
    """
    type: str
    name: str
    topic: str
    data_connections: List[DataConnectionConfig]


class BaseAgentConfig(BaseModel):
    """
    Base configuration model for an agent.

    This model defines the core settings required to configure an agent,
    including its type, name, buffering, file paths, producer connections,
    and scheduling intervals for logs and queries. It also includes validation
    for buffer sizes, intervals, and references between regex patterns and path files.

    Attributes:
        type (str): The type of the agent.
        name (str): Unique name of the agent.
        buffer_rows (int, optional): Number of rows to buffer for processing.
            Must be greater than 0. Defaults to 500.
        path_files (List[PathFileConfig], optional): List of path file configurations
            used by the agent.
        producer_connections (List[ProducerConnectionConfig]): List of producer
            configurations that the agent uses to produce or process data.
        fetch_logs_interval (float): Interval in seconds for fetching logs.
            Must be greater than 0. Defaults to 120.
        execute_query_interval (float): Interval in seconds for executing queries.
            Must be greater than 0. Defaults to 600.
    """
    type: str
    name: str
    buffer_rows: Optional[int] = 500
    path_files: Optional[List[PathFileConfig]] = None
    producer_connections: List[ProducerConnectionConfig]
    fetch_logs_interval: float = 120
    execute_query_interval: float = 600
    

    @field_validator('buffer_rows')
    def validate_buffer_rows(cls, value) -> int:
        """
        Validates the `buffer_rows` field of the BaseAgentConfig model.

        Ensures that the number of buffer rows is greater than 0. This value
        is used by the agent to determine how many rows to process or store
        in memory before committing or sending them downstream.

        Args:
            cls: The BaseAgentConfig class.
            value (int): The value of the `buffer_rows` field to validate.

        Returns:
            int: The validated buffer_rows value.

        Raises:
            ValueError: If `buffer_rows` is less than or equal to 0.
        """
        if value <= 0:
            raise ValueError("Buffer rows must be greater than 0")
        return value
    
    @field_validator('fetch_logs_interval')
    def validate_fetch_logs_interval(cls, value) -> float:
        """
        Validates the `fetch_logs_interval` field of the BaseAgentConfig model.

        Ensures that the interval for fetching logs is greater than 0 seconds.
        This interval determines how frequently the agent retrieves logs from
        producers or other sources.

        Args:
            cls: The BaseAgentConfig class.
            value (float): The value of the `fetch_logs_interval` field to validate.

        Returns:
            float: The validated fetch_logs_interval value.

        Raises:
            ValueError: If `fetch_logs_interval` is less than or equal to 0.
        """
        if value <= 0:
            raise ValueError("Pool interval must be greater than 0")
        return value
    
    @field_validator('execute_query_interval')
    def validate_execute_query_interval(cls, value) -> float:
        """
        Validates the `execute_query_interval` field of the BaseAgentConfig model.

        Ensures that the interval for executing queries is greater than 0 seconds.
        This interval determines how frequently the agent executes queries against
        databases or other destinations.

        Note:
            The error message suggests a minimum of 180 seconds, but the current
            validation logic only enforces the value to be greater than 0.

        Args:
            cls: The BaseAgentConfig class.
            value (float): The value of the `execute_query_interval` field to validate.

        Returns:
            float: The validated execute_query_interval value.

        Raises:
            ValueError: If `execute_query_interval` is less than or equal to 0.
        """
        if value <= 0:
            raise ValueError("Pool interval must be greater than 180")
        return value
    

    @model_validator(mode='after')
    def validate_regex_path_references(self) -> "BaseAgentConfig":
        """
        Validates that all source references in producer data connections
        refer to valid path files defined in `path_files`.

        This model-level validator runs after standard Pydantic validation
        (`mode='after'`). It ensures that each `source_ref.path_file_name` 
        in the agent's producers exists in the `path_files` list. This helps 
        prevent misconfigurations where a regex pattern references a non-existent
        path file.

        Returns:
            BaseAgentConfig: The validated BaseAgentConfig instance.

        Raises:
            ValueError: If any `source_ref.path_file_name` does not match
                        a name in `path_files`.
        """
        if not self.path_files:
            return self

        valid_names = {pf.name for pf in self.path_files}

        for producer in self.producer_connections:
            for dc in producer.data_connections:
                if dc.source_ref:
                    if dc.source_ref.path_file_name not in valid_names:
                        raise ValueError(
                            f"Invalid path_file_name '{dc.source_ref.path_file_name}'. "
                            f"Valid values: {valid_names}"
                        )
        return self
