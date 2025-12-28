from abc import ABC, abstractmethod
import time
from pydantic import BaseModel, field_validator, model_validator
from typing import List, Optional, Pattern, Dict, Any, Tuple
from pathlib import Path
import logging
import re
from concurrent.futures import Future
from functools import partial
from datetime import datetime, timedelta
from .data import WorkingDataConnection
from threading import Thread, Event
from ..utils import get_file_id

class PathFileConfig(BaseModel):

    """
    Configuration model for a monitored file path.

    ``PathFileConfig`` defines a file that is monitored by an agent. It stores
    both static configuration (file path and logical name) and runtime state
    (read cursor and file identifier) required to read files incrementally
    and to correctly handle file rotations.

    Each file configuration is referenced by name from
    :class:`RegexPatternConfig` to associate regex patterns with specific
    input files.

    **Responsibilities:**

    - Identify a monitored file via a logical name
    - Store the filesystem path of the file
    - Track the current read position (cursor) between polling cycles
    - Detect file rotations using a file identifier

    **Runtime behavior:**

    - The ``cursor`` field represents the byte offset of the last read
      position and is updated after each read operation
    - The ``id`` field stores a file identifier (e.g. inode or platform-
      specific file ID) and is used to detect file rotations

    **Validation rules:**

    - ``name`` must not be ``None``
    - ``path`` must be a valid existing :class:`pathlib.Path`
    - ``id`` must be an integer when provided

    :param name: Logical name of the monitored file, used for cross-referencing
                 by regex configurations
    :type name: str

    :param path: Filesystem path of the file to be monitored
    :type path: pathlib.Path

    :param cursor: Byte offset indicating the current read position in the file
    :type cursor: int, optional

    :param id: File identifier used to detect file rotations
    :type id: int, optional
    """

    name: str
    path: Path
    cursor: int = 0
    id: Tuple | None = None
    
    @field_validator('name')
    def validate_name(cls, value):
        """
        Validate the name field of the PathFileConfig model.

        :param value: The value of the name field.
        :raises ValueError: If the name is None.
        :return: The validated name value.
        """
        if value is None:
            raise ValueError("Name cannot be None")
        return value
    
    @field_validator('path')
    def validate_path(cls, value):
        """
        Validate the path field of the PathFileConfig model.

        :param value: The value of the path field.
        :raises ValueError: If the path is None, not a Path object, or does not exist.
        :return: The validated path value.
        """

        if value is None:
            raise ValueError("Path cannot be None")
        if not isinstance(value, Path):
            raise ValueError("Path must be a Path object")
        if not value.exists():
            raise ValueError("Path does not exist")
        return value
    
    @field_validator('id')
    def validate_inode(cls, value):
        """
        Validate the id field of the PathFileConfig model.

        :param value: The value of the id field.
        :raises ValueError: If the id is not None and not an integer.
        :return: The validated id value.
        """
        if value is not None and type(value) is not int:
            raise ValueError("id must be an integer")
        return value
    

class RegexPatternConfig(BaseModel):

    """
    Configuration model for a regex-based data extraction pattern.

    ``RegexPatternConfig`` defines how data is extracted from a specific input
    file using a regular expression. It is used by
    :class:`DataConnectionConfig` to match log lines and extract named capture
    groups that will later be transformed, enriched, and forwarded to
    producers.

    The regular expression is expected to use **named capture groups**
    (``?P<name>``), which are automatically converted into a dictionary
    and stored as extracted data at runtime.

    The regex pattern may be provided either as a compiled ``re.Pattern``
    or as a string. When provided as a string, it is automatically compiled
    during model validation.

    **Responsibilities:**

    - Bind a regex pattern to a specific configured file
    - Define how log lines are matched and parsed
    - Provide structured data extraction via named capture groups

    **Validation and preprocessing:**

    - If ``regex_pattern`` is provided as a string, it is compiled using
      :func:`re.compile` before model creation

    :param path_file_name: Name of the file configuration this regex applies to
    :type path_file_name: str

    :param regex_pattern: Regular expression used to match and extract data
                          from log lines
    :type regex_pattern: re.Pattern[str]
    """

    path_file_name: str
    regex_pattern: Pattern[str]
    
    @field_validator('regex_pattern', mode='before')
    def compile_regex(cls, v):
        """
        Compile a regex pattern from a string.

        This validator is executed before the model is created.

        If the value is a string, it is compiled into a regex pattern
        using the re.compile function. If the value is not a string, it
        is returned unchanged.

        :param v: The value of the regex_pattern field.
        :return: The compiled regex pattern or the original value if it is not a string.
        """
        if isinstance(v, str):
            return re.compile(v)
        return v


class QueryConfig(BaseModel):

    """
    Configuration model for a database query.

    ``QueryConfig`` defines how an agent executes a query against a specific
    data source in order to enrich data previously extracted from input files.
    It is typically referenced by a :class:`DataConnectionConfig` as a
    destination configuration.

    The query is executed at runtime using the database type and name to
    resolve the appropriate database instance from the database factory.

    **Responsibilities:**

    - Define the database type used to execute the query
    - Identify the target database instance
    - Store the query string to be executed
    - Provide a reusable query definition for data enrichment

    **Validation rules:**

    - ``type`` must not be ``None``
    - ``name`` must not be ``None``
    - ``query`` must not be ``None``

    :param type: Type of the database backend (used to resolve the database
                 implementation at runtime)
    :type type: str

    :param name: Name of the database instance to be used for query execution
    :type name: str

    :param query: Query string to be executed against the database
    :type query: str
    """

    type: str
    name: str
    query: str

    @field_validator('type')
    def validate_type(cls, value):
        """
        Validate the type field of the QueryConfig model.

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
        Validate the name field of the QueryConfig model.

        :param value: The value of the name field.
        :raises ValueError: If the name is None.
        :return: The validated name value.
        """
        if value is None:
            raise ValueError("Name cannot be None")
        return value
    
    @field_validator('query')
    def validate_query(cls, value):
        """
        Validate the query field of the QueryConfig model.

        :param value: The value of the query field.
        :raises ValueError: If the query is None.
        :return: The validated query value.
        """
        if value is None:
            raise ValueError("Query cannot be None")
        return value

class DataConnectionConfig(BaseModel):

    """
    Configuration model for a data connection.

    ``DataConnectionConfig`` defines a single data flow within a producer
    connection. It describes how data is extracted from a source (typically
    via a regular expression), optionally enriched using a database query,
    and finally forwarded to a producer.

    A data connection can operate in two modes:

    - **Extraction only**: data is extracted from log lines using a regex
      defined in :class:`RegexPatternConfig`
    - **Extraction + enrichment**: extracted data is used as input for a
      database query defined in :class:`QueryConfig`

    The resulting data is wrapped into a working data connection at runtime
    and managed until it expires.

    **Responsibilities:**

    - Define the logical name of the data connection
    - Specify whether the produced message represents an error condition
    - Configure the source of extracted data (regex-based)
    - Configure an optional destination query for data enrichment
    - Control the expiration time of the produced data

    **Validation rules:**

    - ``name`` must not be ``None``
    - ``is_error`` must be a boolean
    - ``expired_time`` must be an integer when provided

    :param name: Logical name of the data connection
    :type name: str

    :param is_error: Indicates whether the produced message should be treated
                     as an error by the producer
    :type is_error: bool

    :param source_ref: Configuration defining how data is extracted from input
                       lines using a regular expression
    :type source_ref: RegexPatternConfig, optional

    :param destination_ref: Optional configuration defining a database query
                            used to enrich extracted data
    :type destination_ref: QueryConfig, optional

    :param expired_time_int: Time-to-live of the data connection, expressed in
                         minutes from creation time
    :type expired_time_int: int, optional
    """

    name: str
    is_error: bool
    source_ref: RegexPatternConfig = None
    destination_ref: Optional[QueryConfig] = None
    expired_time_int: Optional[int] = 0

    @field_validator('name')
    def validate_name(cls, value):
        """
        Validate the name field of the DataConnectionConfig model.

        :param value: The value of the name field.
        :raises ValueError: If the name is None.
        :return: The validated name value.
        """
        if value is None:
            raise ValueError("Name cannot be None")
        return value

    @field_validator('is_error')
    def validate_is_error(cls, value):
        """
        Validate the is_error field of the DataConnectionConfig model.

        :param value: The value of the is_error field.
        :raises ValueError: If the is_error is not a boolean.
        :return: The validated is_error value.
        """
        if type(value) is not bool:
            raise ValueError("is_error must be a boolean")
        return value
    
    @field_validator('expired_time_int')
    def validate_expired_time(cls, value):
        """
        Validate the expired_time_int field of the DataConnectionConfig model.

        :param value: The value of the expired_time_int field.
        :raises ValueError: If the expired_time_int is not an integer.
        :return: The validated expired_time_int value.
        """
        if value is not None and type(value) is not int:
            raise ValueError("expired_time_int must be an integer")
        return value
    
class ProducerConnectionConfig(BaseModel):

    """
    Configuration model for a producer connection.

    ``ProducerConnectionConfig`` defines how an agent connects to a specific
    producer and which data connections are associated with it. A producer
    represents the final destination of processed data, such as a message
    broker, alerting system, or external service.

    Each producer connection groups one or more
    :class:`DataConnectionConfig` instances that describe how data is extracted,
    optionally enriched, and forwarded to the producer.

    This model is validated using **Pydantic** to ensure that the producer
    identity is correctly defined before the agent starts.

    **Responsibilities:**

    - Define the producer type and name
    - Group related data connections under a single producer
    - Provide a structured configuration for message dispatching

    **Validation rules:**

    - ``type`` must not be ``None``
    - ``name`` must not be ``None``

    :param type: Logical type of the producer (used to resolve the producer
                 implementation at runtime)
    :type type: str

    :param name: Unique name of the producer instance
    :type name: str

    :param data_connections: List of data connection configurations associated
                             with this producer
    :type data_connections: list[DataConnectionConfig]
    """

    type: str
    name: str
    data_connections: List[DataConnectionConfig]

    @field_validator('type')
    def validate_type(cls, value):
        """
        Validate the type field of the ProducerConnectionConfig model.

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
        Validate the name field of the ProducerConnectionConfig model.

        :param value: The value of the name field.
        :raises ValueError: If the name is None.
        :return: The validated name value.
        """
        if value is None:
            raise ValueError("Name cannot be None")
        return value


class BaseAgentConfig(BaseModel):
    
    """
    Base configuration model for all agents.

    ``BaseAgentConfig`` defines the common configuration parameters required
    to initialize and run an agent. It describes how the agent identifies
    itself, which files it monitors, how often it processes data, and how
    extracted data is routed to producers.

    This model is implemented using **Pydantic** and includes both field-level
    and model-level validation to ensure configuration consistency before the
    agent starts running.

    **Key responsibilities:**

    - Define the agent identity (type and name)
    - Configure file monitoring and buffering behavior
    - Declare producer connections and their data flows
    - Control the polling interval of the agent processing loop
    - Validate cross-references between file configurations and regex sources

    **Validation rules:**

    - ``type`` and ``name`` must not be ``None``
    - ``buffer_rows`` must be greater than zero
    - ``pool_interval`` must be greater than zero
    - Any ``path_file_name`` referenced by a regex source must exist in
      ``path_files``

    :param type: Logical type of the agent (used to select the agent
                 implementation and producers)
    :type type: str

    :param name: Unique name of the agent instance
    :type name: str

    :param buffer_rows: Maximum number of lines read from each file per polling
                        cycle
    :type buffer_rows: int, optional

    :param path_files: List of file path configurations monitored by the agent
    :type path_files: list[PathFileConfig], optional

    :param producer_connections: Definitions of producer connections and their
                                 associated data connections
    :type producer_connections: list[ProducerConnectionConfig]

    :param pool_interval: Time in seconds to wait between consecutive polling
                          cycles
    :type pool_interval: float
    """

    type: str # type of agent
    name: str
    buffer_rows: Optional[int] = 500
    path_files: Optional[List[PathFileConfig]] = None
    producer_connections: List[ProducerConnectionConfig]
    pool_interval: float = 0.1
    

    @field_validator('type')
    def validate_type(cls, value):
        """
        Validate the type field of the BaseAgentConfig model.

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
        Validate the name field of the BaseAgentConfig model.

        :param value: The value of the name field.
        :raises ValueError: If the name is None.
        :return: The validated name value.
        """
        if value is None:
            raise ValueError("Name cannot be None")
        return value

    @field_validator('buffer_rows')
    def validate_buffer_rows(cls, value):
        """
        Validate the buffer_rows field of the BaseAgentConfig model.

        :param value: The value of the buffer_rows field.
        :raises ValueError: If the buffer_rows is less than or equal to 0.
        :return: The validated buffer_rows value.
        """
        if value <= 0:
            raise ValueError("Buffer rows must be greater than 0")
        return value
    
    @field_validator('pool_interval')
    def validate_pool_interval(cls, value):
        """
        Validate the pool_interval field of the BaseAgentConfig model.

        :param value: The value of the pool_interval field.
        :raises ValueError: If the pool_interval is less than or equal to 0.
        :return: The validated pool_interval value.
        """
        if value <= 0:
            raise ValueError("Pool interval must be greater than 0")
        return value
    

    @model_validator(mode='after')
    def validate_regex_path_references(self):
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

class BaseAgent(ABC):

    """
    Abstract base class for all agents.

    The ``BaseAgent`` defines the common runtime behavior and lifecycle for
    agents that monitor one or more files, extract data using regular
    expressions, optionally enrich the extracted data via database queries,
    and forward the resulting messages to producers.

    The agent runs in a dedicated background thread and periodically executes
    its processing loop until it is explicitly stopped.

    **Main responsibilities:**

    - Monitor configured file paths and detect file rotations
    - Read log files incrementally using a cursor-based approach
    - Match lines against configured regular expressions
    - Create and manage :class:`WorkingDataConnection` instances
    - Execute optional database queries associated with matched data
    - Dispatch processed messages to configured producers
    - Clean up expired working data connections

    **Threading model:**

    - Each agent runs a single daemon worker thread
    - The worker thread periodically invokes the internal processing loop
      based on the configured polling interval
    - Thread termination is controlled via a stop event

    **Extensibility:**

    This class is abstract and must be subclassed.
    Subclasses are required to implement the
    :meth:`_data_connections_transformation_and_filtering` method in order to
    apply domain-specific transformations and filtering logic to matched data.

    **Lifecycle:**

    1. The agent is initialized with a :class:`BaseAgentConfig`
    2. A background worker thread is started automatically
    3. The agent periodically processes input files and data connections
    4. The agent can be stopped gracefully by calling :meth:`stop`

    :param config: Configuration object defining agent behavior, file paths,
                   producer connections, polling interval, and buffering rules
    :type config: BaseAgentConfig
    """

    def __init__(self, config: BaseAgentConfig):
        """
        Initialize the BaseAgent with the given config.

        :param config: The config for the agent.

        Initializes the agent with the given config, sets up the logger, and starts the worker thread.
        """
        self.config = config
        self.logger = logging.getLogger("__main__." +__name__)
        self.logger.info(f"Initialized agent: {self.config.type}-{self.config.name}")
        self.working_data_connections: List[WorkingDataConnection] = []

        self._stop_event = Event()
        self._thread = Thread(target=self._worker, daemon=True)
        self._thread.start()

    # PUBLIC API

    def stop(self):
        """
        Stop the agent.

        This method sets the stop event and waits for the worker thread to finish.

        :return: None
        """
        self._stop_event.set()
        self._thread.join()

    # INTERNALS

    def _worker(self):
        """
        The worker thread for the agent.

        This method is responsible for running the agent's code in a loop until the stop event is set.
        It will run the _run_once method and then wait for the specified pool interval before running it again.

        :return: None
        """
        self.logger.info(f"Agent: {self.config.type}-{self.config.name} started")

        while not self._stop_event.is_set():
            self._run_once()
            time.sleep(self.config.pool_interval)

    def _run_once(self) -> None:
        
        """
        Run the agent's code once.

        This method reads the specified number of lines from each path file and then
        calls the _data_connections_flow method to process the lines.

        If a rotation is detected for a path file (i.e. the file ID does not match the current file ID),
        the remaining lines from the old file are read and processed, and the cursor is reset to 0.

        :return: None
        """
        for path_file in self.config.path_files or []:
            
            current_file_id = get_file_id(path_file.path)

            if path_file.id is None:
                path_file.id = current_file_id
                lines = self._read_batch_log(path_file)

            elif path_file.id != current_file_id:
                self.logger.info(f"Agent: {self.config.type}-{self.config.name}: Rotation detected for {path_file.path}")
                lines = self._read_remaining_old_file(path_file)
                path_file.cursor = 0
                path_file.id = current_file_id
            else:
                lines = self._read_batch_log(path_file)
            
            self._data_connections_flow(path_file, lines)

    def _read_remaining_old_file(self, path_file: PathFileConfig):

        """
        Read the remaining lines from an old file after a rotation is detected.

        Given a PathFileConfig, this method searches for an old file with the same file_id
        in the same directory. If an old file is found, it reads the remaining lines from the
        old file and resets the cursor to 0.

        If no old file is found, a warning is logged and an empty list is returned.

        :param path_file: The PathFileConfig for which the remaining lines should be read.
        :return: A list of lines read from the old file, or an empty list if no old file is found.
        """
        file_path: Path = path_file.path
        directory: Path = file_path.parent
        filename: str = file_path.name

        # Pattern to find the old file
        pattern = filename + "*"  # es: app.log*
        candidate_files = sorted(
            directory.glob(pattern),
            key=lambda p: p.stat().st_mtime
        )
        # Search for the old file with the same file_id
        old_file: Path | None = None
        for f in candidate_files:
            if f == file_path:
                continue  # salto il nuovo file
            if self._get_file_id(f) == path_file.file_id:
                old_file = f
                break

        if old_file is None:
            self.logger.warning(f"No old file found for {file_path}")
            return []

        # Read remaining lines
        lines = []
        with old_file.open("r") as f:
            f.seek(path_file.cursor)
            lines = f.readlines()
            self.logger.info(f"Read {len(lines)} remaining lines from old file {old_file}")

        return lines

    def _read_batch_log(self, path_file: PathFileConfig) -> List[str]:
        """
        Read a batch of lines from a log file.

        Given a PathFileConfig, this method reads the specified number of lines from the file and
        updates the cursor of the PathFileConfig to the position after the last read line.

        If the end of the file is reached before the specified number of lines is read, the
        method will return the remaining lines.

        :param path_file: The PathFileConfig for which the lines should be read.
        :return: A list of lines read from the file.
        """
        lines = []
        with open(path_file.path, 'r') as f:
            f.seek(path_file.cursor)
            for _ in range(0, self.config.buffer_rows):
                line = f.readline()
                if not line:
                    break
                lines.append(line)
            path_file.cursor = f.tell()
        return lines

    def _data_connections_flow(self, path_file: PathFileConfig, lines: List[str]):

        """
        Process the data connections matched by the regex in the read lines.

        This method takes a PathFileConfig and a list of lines as input, and processes
        the data connections matched by the regex in the read lines. It applies
        transformations and filtering to the data connections, executes queries using the
        data connections, sends the extracted messages to producers, and then cleans the
        working data connections.

        :param path_file: The PathFileConfig for which the lines should be processed.
        :param lines: The list of lines read from the file.
        :return: None
        """
        self.logger.info(f"Agent: {self.config.type}-{self.config.name}: Processing file: {path_file.path} with cursor at {path_file.cursor}")

        self.logger.info(f"Agent: {self.config.type}-{self.config.name}: Extracted {len(lines)} lines from {path_file.path}")

        working_data_connections = self._data_connections_match_regex(path_file, lines)

        self.logger.info(f"Agent: {self.config.type}-{self.config.name}: Matched {len(self.working_data_connections)} regex in read lines")

        working_data_connections = self._data_connections_transformation_and_filtering(working_data_connections)

        for working_data_connection in working_data_connections:
            if working_data_connection.data_dict_result:
                working_data_connection.is_updated = True
                

        self.working_data_connections.extend(working_data_connections)

        self.logger.info(f"Agent: {self.config.type}-{self.config.name}: Applied transformations and filtering to data connections")

        self._data_connections_execute_queries()

        len_working_query = len([working_data_connection for working_data_connection in self.working_data_connections if (working_data_connection.database_name and working_data_connection.is_updated)])
        
        self.logger.info(f"Agent: {self.config.type}-{self.config.name}: Executed {len_working_query} queries using data connections")

        self._send_messages_to_producers()
        
        self.logger.info(f"Agent: {self.config.type}-{self.config.name}: Sent {len(self.working_data_connections)} messages extracted from regex and queries to producers")

        self._clean_working_data_connections()

        self.logger.info(f"Agent: {self.config.type}-{self.config.name}: Cleaned working data connections")

    def _data_connections_match_regex(self, path_file: PathFileConfig, lines: List[str]) -> List[WorkingDataConnection]:

        """
        Match the data connections with the regex in the read lines.

        Given a PathFileConfig and a list of lines as input, this method processes the data
        connections matched by the regex in the read lines.

        :param path_file: The PathFileConfig for which the lines should be processed.
        :param lines: The list of lines read from the file.
        :return: A list of WorkingDataConnection objects containing the data connections matched by
            the regex in the read lines.
        """
        producer_connections = self.config.producer_connections

        working_data_connections = []

        for line in lines:
            for producer_connection in producer_connections:
                for data_connection in producer_connection.data_connections:
                    if data_connection.destination_ref:                
                        working_data_connection = WorkingDataConnection(
                            name=data_connection.name,
                            producer_type=producer_connection.type,
                            producer_name=producer_connection.name,
                            database_type=data_connection.destination_ref.type if data_connection.destination_ref else None,
                            database_name=data_connection.destination_ref.name if data_connection.destination_ref else None,
                            query=data_connection.destination_ref.query if data_connection.destination_ref else None,
                            is_error=data_connection.is_error,
                            expired_time=datetime.now() + timedelta(minutes=data_connection.expired_time_int)
                        )
                    else:
                        working_data_connection = WorkingDataConnection(
                            name=data_connection.name,
                            producer_type=producer_connection.type,
                            producer_name=producer_connection.name,
                            is_error=data_connection.is_error,
                            expired_time=datetime.now() + timedelta(minutes=data_connection.expired_time_int)
                        )
                    if data_connection.source_ref and \
                        data_connection.source_ref.path_file_name == path_file.name and \
                            not data_connection.destination_ref:
                        regex_pattern = data_connection.source_ref.regex_pattern
                        match = regex_pattern.search(line)
                        if match:
                            working_data_connection.data_dict_match = match.groupdict()
                    elif data_connection.source_ref and \
                        data_connection.source_ref.path_file_name == path_file.name and \
                            data_connection.destination_ref:
                        regex_pattern = data_connection.source_ref.regex_pattern
                        match = regex_pattern.search(line)
                        if match:
                            working_data_connection.data_dict_match = match.groupdict()
                    working_data_connections.append(working_data_connection)
        return working_data_connections

    @abstractmethod
    def _data_connections_transformation_and_filtering(self, working_data_connections: List[WorkingDataConnection]) -> List[WorkingDataConnection]:
        """
        Abstract method to transform and filter the given working data connections.

        :param working_data_connections: The list of working data connections to transform and filter.
        :return: The list of transformed and filtered working data connections.
        """
        
        pass
        
    def _on_done(self, working_data_connection: WorkingDataConnection, fut: Future) -> None:
        """
        Called when the Future for a query has finished executing. Sets the query result on the WorkingDataConnection and updates its status.
        """
        try:
            result: Dict[str, Any] = fut.result()
            if result and working_data_connection.data_dict_result != result:
                working_data_connection.data_dict_result = result
                working_data_connection.is_updated = True
                working_data_connection.query_in_progress = False
            elif result and working_data_connection.data_dict_result == result:
                working_data_connection.is_updated = False
                working_data_connection.query_in_progress = False
        except Exception as e:
            self.logger.error(f"Agent: {self.config.type}-{self.config.name}: Failed to execute query: {e}")
            working_data_connection.is_updated = False
            working_data_connection.query_in_progress = False


    def _data_connections_execute_queries(self) -> None:
        """
        Executes queries for all working data connections that have a database name and
        are not currently executing a query.

        For each working data connection, it gets the database instance from the
        DATABASE_INSTANCES factory and executes the query using the database instance.
        It then adds a done callback to the future to update the working data connection
        status when the query is finished executing.
        """
        from databases.factory import DATABASE_INSTANCES

        for working_data_connection in self.working_data_connections:
            if working_data_connection.database_name and not working_data_connection.query_in_progress:
                database_instance = DATABASE_INSTANCES.get((working_data_connection.database_type, working_data_connection.database_name))
                working_data_connection.query_in_progress = True
                future = database_instance.execute(working_data_connection.query, working_data_connection.data_dict_query_source)
                future.add_done_callback(partial(self._on_done, working_data_connection))

    def _send_messages_to_producers(self) -> None:
        """
        Sends messages extracted from data connections to producers.

        It iterates over all working data connections and for each one, it gets the
        producer instance from the PRODUCER_INSTANCES factory and sends the message
        to the producer if the data connection has a result and is updated. It then
        resets the is_updated flag on the data connection.

        If an error occurs while sending the message to the producer, it logs the error.
        """
        
        from producers.factory import PRODUCER_INSTANCES
        
        for working_data_connection in self.working_data_connections:
            producer_instance = PRODUCER_INSTANCES.get((working_data_connection.producer_type, working_data_connection.producer_name))
            try:
                if working_data_connection.data_dict_result and working_data_connection.is_updated:
                    producer_instance.produce(working_data_connection.is_error, working_data_connection.data_dict_result)
                    working_data_connection.is_updated = False
            except Exception as e:
                self.logger.error(f"Agent: {self.config.type}-{self.config.name}: Failed to send message to producer: {e}")

    def _clean_working_data_connections(self) -> None:
        """
        Cleans expired working data connections from the list of working data connections.

        It iterates over a copy of the working data connections and for each one, it checks
        if the expired time is less than the current datetime. If it is, it removes the
        working data connection from the list of working data connections.

        This method is called every time the agent is about to process a new set of lines from
        a file. It ensures that the list of working data connections does not grow indefinitely
        and that expired data connections are removed from the list.

        :return: None
        """
        datetime_now = datetime.now()

        for working_data_connection in self.working_data_connections[:]:  # copy
            if working_data_connection.expired_time < datetime_now:
                self.working_data_connections.remove(working_data_connection)
