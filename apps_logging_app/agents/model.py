from pydantic import BaseModel, field_validator, model_validator
from typing import List, Optional, Pattern, Tuple
from pathlib import Path
import re


class PathFileConfig(BaseModel):
    """
    Configuration model representing a log file to be monitored.

    This model defines a file path along with runtime metadata used by
    agents to track read position and file rotation.

    :param name: Logical name of the path file, used as a reference
        in regex configurations.
    :type name: str
    :param path: Filesystem path to the log file.
    :type path: pathlib.Path
    :param cursor: Byte position used to track the last read offset.
    :type cursor: int
    :param id: Identifier of the file used to detect file rotation.
    :type id: Optional[int]
    """
    name: str
    path: Path
    cursor: int = 0
    id: Optional[int] = None
    
    @field_validator('name')
    def validate_name(cls, value) -> str:
        if value is None:
            raise ValueError("Name cannot be None")
        return value
    
    @field_validator('path')
    def validate_path(cls, value) -> Path:
        if value is None:
            raise ValueError("Path cannot be None")
        if not isinstance(value, Path):
            raise ValueError("Path must be a Path object")
        if not value.exists():
            raise ValueError("Path does not exist")
        return value

class RegexPatternConfig(BaseModel):
    """
    Configuration model defining a regex pattern applied to a log file.

    This model links a regular expression to a specific
    :class:`PathFileConfig` via its name. The regex is compiled automatically
    if provided as a string.

    :param path_file_name: Name of the path file this regex applies to.
    :type path_file_name: str
    :param regex_pattern: Compiled regular expression used to extract data.
    :type regex_pattern: Pattern[str]
    """
    path_file_name: str
    regex_pattern: Pattern[str]
    
    @field_validator('regex_pattern', mode='before')
    def compile_regex(cls, v) -> Pattern[str]:
        if isinstance(v, str):
            return re.compile(v)
        return v


class QueryConfig(BaseModel):
    """
    Configuration model defining a database query destination.

    This model specifies the database type, instance name, and query
    template to be executed when a data connection is processed.

    :param type: Type of the destination database.
    :type type: str
    :param name: Name of the destination database instance.
    :type name: str
    :param query: Query string to be executed.
    :type query: str
    """
    type: str
    name: str
    query: str

class DataConnectionConfig(BaseModel):
    """
    Configuration model defining a logical data connection.

    A data connection links a source (regex match on a log file) to an
    optional destination (database query) and defines how extracted
    data should be processed and routed.

    :param name: Unique name of the data connection.
    :type name: str
    :param is_error: Whether this data connection represents an error condition.
    :type is_error: bool
    :param source_ref: Regex-based source configuration.
    :type source_ref: Optional[RegexPatternConfig]
    :param destination_ref: Query-based destination configuration.
    :type destination_ref: Optional[QueryConfig]
    :param expired_time_int: Expiration time in minutes for generated data.
    :type expired_time_int: Optional[int]
    """
    name: str
    is_error: bool
    source_ref: RegexPatternConfig = None
    destination_ref: Optional[QueryConfig] = None
    expired_time_int: Optional[int] = None
    
class ProducerConnectionConfig(BaseModel):
    """
    Configuration model defining a producer and its associated data connections.

    This model groups multiple :class:`DataConnectionConfig` instances
    under a single producer definition.

    :param type: Type of the producer.
    :type type: str
    :param name: Name of the producer instance.
    :type name: str
    :param data_connections: List of data connections handled by the producer.
    :type data_connections: List[DataConnectionConfig]
    """
    type: str
    name: str
    data_connections: List[DataConnectionConfig]


class BaseAgentConfig(BaseModel):
    """
    Base configuration model for agent instances.

    This model defines all common configuration options required by agents,
    including file monitoring, buffering behavior, producer connections,
    and execution intervals.

    It also enforces validation rules to ensure consistency between
    path files and regex references.

    :param type: Type identifier of the agent.
    :type type: str
    :param name: Name of the agent instance.
    :type name: str
    :param buffer_rows: Number of log lines to read per batch.
    :type buffer_rows: int
    :param path_files: List of configured log files to monitor.
    :type path_files: Optional[List[PathFileConfig]]
    :param producer_connections: Producer connections handled by the agent.
    :type producer_connections: List[ProducerConnectionConfig]
    :param fetch_logs_interval: Interval in seconds between log fetch cycles.
    :type fetch_logs_interval: float
    :param execute_query_interval: Interval in seconds between query executions.
    :type execute_query_interval: float
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
        Validate the buffer_rows field of the BaseAgentConfig model.

        :param value: The value of the buffer_rows field.
        :raises ValueError: If the buffer_rows is less than or equal to 0.
        :return: The validated buffer_rows value.
        """
        if value <= 0:
            raise ValueError("Buffer rows must be greater than 0")
        return value
    
    @field_validator('fetch_logs_interval')
    def validate_fetch_logs_interval(cls, value) -> float:
        """
        Validate the fetch_logs_interval field of the BaseAgentConfig model.

        :param value: The value of the fetch_logs_interval field.
        :raises ValueError: If the fetch_logs_interval is less than or equal to 0.
        :return: The validated fetch_logs_interval value.
        """
        if value <= 0:
            raise ValueError("Pool interval must be greater than 0")
        return value
    
    @field_validator('execute_query_interval')
    def validate_execute_query_interval(cls, value) -> float:
        """
        Validate the execute_query_interval field of the BaseAgentConfig model.

        :param value: The value of the execute_query_interval field.
        :raises ValueError: If the execute_query_interval is less than or equal to 180.
        :return: The validated execute_query_interval value.
        """
        if value <= 0:
            raise ValueError("Pool interval must be greater than 180")
        return value
    

    @model_validator(mode='after')
    def validate_regex_path_references(self) -> "BaseAgentConfig":
        """
        Validate the regex path references in the BaseAgentConfig model.

        This validator checks if the path_file_name referenced in the data_connections
        of the producer_connections exist in the path_files of the agent.

        :raises ValueError: If the path_file_name referenced in the data_connections
            does not exist in the path_files of the agent.
        :return: The validated BaseAgentConfig model.
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
