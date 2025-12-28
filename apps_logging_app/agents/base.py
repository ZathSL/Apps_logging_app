from abc import ABC, abstractmethod
import time
from pydantic import BaseModel, field_validator, validator
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
    name: str
    path: Path
    cursor: int = 0
    id: Tuple | None = None
    
    @field_validator('name')
    def validate_name(cls, value):
        if value is None:
            raise ValueError("Name cannot be None")
        return value
    
    @field_validator('path')
    def validate_path(cls, value):
        if value is None:
            raise ValueError("Path cannot be None")
        if not isinstance(value, Path):
            raise ValueError("Path must be a Path object")
        if not value.exists():
            raise ValueError("Path does not exist")
        return value
    
    @field_validator('id')
    def validate_inode(cls, value):
        if value is not None and type(value) is not int:
            raise ValueError("id must be an integer")
        return value
    

class RegexPatternConfig(BaseModel):
    path_file_name: str
    regex_pattern: Pattern[str]

    @validator('path_file_name')
    def validate_path_file_name(cls, value, values):
        agent_config = values.get('agent_config')
        if agent_config and value not in [pf.name for pf in agent_config.path_file]:
            raise ValueError(f"Path file {value} not found in agent config")
        return value
    
    @field_validator('regex_pattern', mode='before')
    def compile_regex(cls, v):
        if isinstance(v, str):
            return re.compile(v)
        return v


class QueryConfig(BaseModel):
    type: str
    name: str
    query: str

    @field_validator('type')
    def validate_type(cls, value):
        if value is None:
            raise ValueError("Type cannot be None")
        return value
    
    @field_validator('name')
    def validate_name(cls, value):
        if value is None:
            raise ValueError("Name cannot be None")
        return value
    
    @field_validator('query')
    def validate_query(cls, value):
        if value is None:
            raise ValueError("Query cannot be None")
        return value

class DataConnectionConfig(BaseModel):
    name: str
    is_error: bool
    source_ref: RegexPatternConfig = None
    destination_ref: Optional[QueryConfig] = None
    expired_time: Optional[int] = 0

    @field_validator('name')
    def validate_name(cls, value):
        if value is None:
            raise ValueError("Name cannot be None")
        return value

    @field_validator('is_error')
    def validate_is_error(cls, value):
        if type(value) is not bool:
            raise ValueError("is_error must be a boolean")
        return value
    
    @field_validator('expired_time')
    def validate_expired_time(cls, value):
        if value is not None and type(value) is not int:
            raise ValueError("expired_time must be an integer")
        return value
    
class ProducerConnectionConfig(BaseModel):
    type: str
    name: str
    data_connections: List[DataConnectionConfig]

    @field_validator('type')
    def validate_type(cls, value):
        if value is None:
            raise ValueError("Type cannot be None")
        return value
    
    @field_validator('name')
    def validate_name(cls, value):
        if value is None:
            raise ValueError("Name cannot be None")
        return value


class BaseAgentConfig(BaseModel):
    # initial common configuration for all agents
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
    

class BaseAgent(ABC):

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
                            expired_time=datetime.now() + timedelta(minutes=data_connection.expired_time)
                        )
                    else:
                        working_data_connection = WorkingDataConnection(
                            name=data_connection.name,
                            producer_type=producer_connection.type,
                            producer_name=producer_connection.name,
                            is_error=data_connection.is_error,
                            expired_time=datetime.now() + timedelta(minutes=data_connection.expired_time)
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
