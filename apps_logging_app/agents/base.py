from abc import ABC, abstractmethod
import time
from typing import List, Dict, Any, Tuple

from pathlib import Path
import logging
from concurrent.futures import Future
from threading import Thread, Event

from .data import WorkingDataConnection, WorkingDataStatus
from ..utils import get_file_id
from .model import BaseAgentConfig, PathFileConfig, ProducerConnectionConfig, DataConnectionConfig
from datetime import datetime, timedelta
from ..producers.data import Message
from ..databases.data import Query

class BaseAgent(ABC):
    """
    Abstract base class for agents that process log files and manage data connections.

    The `BaseAgent` class provides a framework for reading log files, matching 
    lines to data connections using regex patterns, transforming and filtering 
    data, executing database queries, and sending messages to producers. 
    It is designed to run asynchronously using a background thread.

    Subclasses must implement the `_data_connections_transformation_and_filtering` 
    method to define agent-specific logic for processing matched data.

    Attributes:
        config (BaseAgentConfig): The configuration object for the agent.
        logger (logging.Logger): Logger instance for the agent.
        working_data_connections (List[WorkingDataConnection]): Active working 
            data connections being processed.
        path_file_to_data_connections (Dict[str, List[Tuple[ProducerConnectionConfig, DataConnectionConfig]]]):
            Maps log file names to associated data connections.
        _stop_event (Event): Event used to stop the worker thread.
        _thread (Thread): Background worker thread.
        next_execute_query_time (datetime): Next scheduled time to execute database queries.

    Example:
        >>> from .config import BaseAgentConfig
        >>> config = BaseAgentConfig(...)
        >>> class MyAgent(BaseAgent):
        ...     def _data_connections_transformation_and_filtering(self, wdc):
        ...         return wdc.data_dict_match
        >>> agent = MyAgent(config)
        >>> agent.start()
        >>> # ... agent runs asynchronously ...
        >>> agent.stop()

    """
    def __init__(self, config: BaseAgentConfig) -> None:
        """
        Initializes the BaseAgent with the provided configuration.

        Sets up logging, initializes working data connections, maps log files 
        to data connections, and prepares the background worker thread.

        Args:
            config (BaseAgentConfig): Configuration object containing agent 
                parameters, producer connections, path files, and intervals.

        Attributes Initialized:
            config (BaseAgentConfig): The agent configuration.
            logger (logging.Logger): Logger for this agent instance.
            working_data_connections (List[WorkingDataConnection]): List of active 
                working data connections initialized from configuration.
            path_file_to_data_connections (Dict[str, List[Tuple[ProducerConnectionConfig, DataConnectionConfig]]]):
                Mapping from log file names to associated producer and data connections.
            _stop_event (Event): Event used to signal stopping the background thread.
            _thread (Thread): Background worker thread initialized but not started.
            next_execute_query_time (datetime): Timestamp for the next scheduled 
                database query execution.

        Example:
            >>> from .config import BaseAgentConfig
            >>> config = BaseAgentConfig(...)
            >>> agent = BaseAgentSubclass(config)  # must subclass since BaseAgent is abstract
            >>> agent.logger.info("Agent initialized")
        """
        self.config: BaseAgentConfig = config
        self.logger = logging.getLogger("__main__." +__name__)
        self.working_data_connections: List[WorkingDataConnection] = []
        self._initialize_working_data_connections()
        self.path_file_to_data_connections: Dict[str, List[Tuple[ProducerConnectionConfig, DataConnectionConfig]]] = {}
        self._initialize_path_file_to_data_connections_map()
        self._stop_event = Event()
        self._thread = Thread(target=self._worker, daemon=True)
        self.next_execute_query_time = datetime.now()
        self.logger.info(f"Initialized agent: {self.config.type}-{self.config.name}")


    # PUBLIC API

    def start(self) -> None:
        """
        Starts the agent's background worker thread.

        This method begins asynchronous processing of log files, data connections,
        and message production by running the `_worker` method in a separate daemon thread.

        Once started, the agent will continuously read log files, match lines to 
        data connections, process transformations, execute queries, and send messages 
        until the `stop()` method is called.

        Example:
            >>> agent = BaseAgentSubclass(config)  # subclass must implement abstract methods
            >>> agent.start()
        """
        self._thread.start()

    def stop(self) -> None:
        """
        Stops the agent's background worker thread safely.

        This method signals the `_worker` thread to stop by setting the `_stop_event`,
        then waits for the thread to finish its current processing loop and exit.

        After calling `stop()`, the agent will no longer read log files, process data 
        connections, or send messages to producers.

        Example:
            >>> agent = BaseAgentSubclass(config)  # subclass must implement abstract methods
            >>> agent.start()
            >>> # ... agent is running ...
            >>> agent.stop()
        """
        self._stop_event.set()
        self._thread.join()

    # INTERNALS

    def _initialize_working_data_connections(self) -> None:
        """
        Initializes working data connections from the agent's configuration.

        This method iterates over all producer connections and their associated 
        data connections defined in `self.config`. For each data connection, 
        it creates a `WorkingDataConnection` instance using `from_config()`, 
        sets its status to ready, and appends it to the `working_data_connections` list.

        This prepares the agent to start processing log lines and executing 
        transformations or queries for each connection.

        Example:
            >>> agent = BaseAgentSubclass(config)  # subclass must implement abstract methods
            >>> agent._initialize_working_data_connections()
            >>> print(agent.working_data_connections)
            [<WorkingDataConnection ...>, ...]
        """
        for producer_connection in self.config.producer_connections:
            for data_connection in producer_connection.data_connections:
                working_data_connection = WorkingDataConnection.from_config(producer_connection.type, producer_connection.name, producer_connection.topic, data_connection)
                working_data_connection.set_ready_status()
                self.working_data_connections.append(working_data_connection)

    def _initialize_path_file_to_data_connections_map(self):
        """
        Builds a mapping from log file names to their associated data connections.

        This method iterates over all producer connections and their data connections 
        defined in the agent's configuration. If a data connection has a `source_ref` 
        (indicating it is tied to a log file), it adds a tuple of `(producer, data_connection)` 
        to the `path_file_to_data_connections` dictionary under the key corresponding 
        to the file name.

        This mapping is used later to quickly identify which data connections should 
        process lines from each log file.

        Example:
            >>> agent = BaseAgentSubclass(config)
            >>> agent._initialize_path_file_to_data_connections_map()
            >>> print(agent.path_file_to_data_connections)
            {'app.log': [(<ProducerConnectionConfig ...>, <DataConnectionConfig ...>), ...]}
        """
        for producer in self.config.producer_connections:
            for dc in producer.data_connections:
                if dc.source_ref:
                    self.path_file_to_data_connections.setdefault(dc.source_ref.path_file_name, []).append((producer, dc))


    def _worker(self) -> None:
        """
        Background worker method that continuously processes log files.

        This method runs in a separate daemon thread and repeatedly calls `_run_once()` 
        to read log files, match data connections, transform and filter data, execute queries, 
        and send messages to producers. It pauses for `fetch_logs_interval` seconds 
        between iterations.

        The loop continues until `_stop_event` is set by the `stop()` method.

        Example:
            >>> agent = BaseAgentSubclass(config)
            >>> agent.start()  # _worker runs asynchronously in background
            >>> # ... agent is processing logs ...
            >>> agent.stop()   # stops the worker thread
        """
        self.logger.info(f"Agent: {self.config.type}-{self.config.name} started")

        while not self._stop_event.is_set():
            self._run_once()
            time.sleep(self.config.fetch_logs_interval)

    def _run_once(self) -> None:
        """
        Performs a single iteration of log file processing.

        This method iterates over all configured path files and handles:
        - Reading new lines from the file using `_read_batch_log`.
        - Detecting file rotation by comparing file IDs and reading remaining 
        lines from the old file using `_read_remaining_old_file`.
        - Resetting the cursor and updating the file ID when rotation occurs.
        - Sending the read lines to `_data_connections_flow` for further processing.

        This method is called repeatedly by the `_worker` method in the background thread.

        Example:
            >>> agent = BaseAgentSubclass(config)
            >>> agent._run_once()  # processes all path files once
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

    def _read_remaining_old_file(self, path_file: PathFileConfig) -> List[str]:
        """
        Reads the remaining lines from a rotated log file.

        When a log file is rotated, this method searches for the old version of the file
        that matches the original `file_id`. It reads all remaining lines starting from 
        the last cursor position and returns them for further processing.

        Args:
            path_file (PathFileConfig): The configuration of the path file, 
                including its current cursor and file ID.

        Returns:
            List[str]: A list of lines remaining in the old file. Returns an empty 
            list if no matching old file is found.

        Notes:
            - Uses a glob pattern based on the file name to locate candidate rotated files.
            - Logs a warning if no old file is found.
            - Updates are read starting from `path_file.cursor`.

        Example:
            >>> lines = agent._read_remaining_old_file(path_file)
            >>> for line in lines:
            ...     process(line)
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
        Reads a batch of lines from the given log file starting at the current cursor.

        This method opens the file specified in `path_file.path`, seeks to the last 
        read position (`path_file.cursor`), and reads up to `buffer_rows` lines 
        as defined in the agent configuration. After reading, it updates the cursor 
        to the new file position.

        Args:
            path_file (PathFileConfig): The configuration of the log file, 
                including the file path and current cursor position.

        Returns:
            List[str]: A list of lines read from the log file. May be empty if 
            the end of file is reached.

        Example:
            >>> lines = agent._read_batch_log(path_file)
            >>> for line in lines:
            ...     print(line.strip())
        """
        lines: List[str] = []
        with open(path_file.path, 'r') as f:
            f.seek(path_file.cursor)
            for _ in range(0, self.config.buffer_rows):
                line = f.readline()
                if not line:
                    break
                lines.append(line)
            path_file.cursor = f.tell()
        return lines

    def _data_connections_flow(self, path_file: PathFileConfig, lines: List[str]) -> None:
        """
        Processes log lines through data connections, applies transformations,
        executes queries, sends messages, and cleans up expired connections.

        This method performs the full workflow for a batch of log lines:
        1. Matches lines to data connections using `_data_connections_match_regex`.
        2. Transforms and filters matched data via `_data_connections_transformation_and_filtering`.
        3. Updates the working data connection status and stores results.
        4. Executes database queries if the scheduled time is reached.
        5. Sends messages to producers for updated data connections.
        6. Cleans expired working data connections from the internal list.

        Args:
            path_file (PathFileConfig): Configuration of the log file being processed.
            lines (List[str]): List of log lines read from the file.

        Example:
            >>> agent._data_connections_flow(path_file, lines)
            # Processes lines, updates connections, executes queries, and sends messages.
        """
        working_data_connections = self._data_connections_match_regex(path_file, lines)

        for wdc in working_data_connections:
            data_dict_tmp = self._data_connections_transformation_and_filtering(wdc)
            if wdc.query:
                wdc.data_dict_query_source = data_dict_tmp
                wdc.set_ready_status()
            else:
                if data_dict_tmp != wdc.data_dict_result:
                    wdc.data_dict_result = data_dict_tmp
                    wdc.status = WorkingDataStatus.UPDATED
                else:
                    wdc.set_ready_status()
 
        self.working_data_connections.extend(working_data_connections)
        if self.next_execute_query_time <= datetime.now():
            self._data_connections_execute_queries()
            self.next_execute_query_time = datetime.now() + timedelta(seconds=self.config.execute_query_interval)

        self._send_messages_to_producers()
        self._clean_working_data_connections()


    def _data_connections_match_regex(self, path_file: PathFileConfig, lines: List[str]) -> List[WorkingDataConnection]:
        """
        Matches log lines against configured data connection regex patterns.

        For each line in the provided log batch, this method checks whether it matches 
        the regex pattern defined in each relevant data connection for the given path file. 
        When a match is found, a new `WorkingDataConnection` is created with the matched 
        data populated in `data_dict_match`.

        Args:
            path_file (PathFileConfig): The log file configuration containing the file name.
            lines (List[str]): The batch of log lines to process.

        Returns:
            List[WorkingDataConnection]: A list of working data connections corresponding 
            to lines that matched the regex patterns. Empty if no matches are found.

        Example:
            >>> matches = agent._data_connections_match_regex(path_file, lines)
            >>> for wdc in matches:
            ...     print(wdc.data_dict_match)
        """
        working_data_connections = []

        relevant_connections = self.path_file_to_data_connections.get(path_file.name, [])

        for line in lines:
            for producer_connection, data_connection in relevant_connections:
                match = data_connection.source_ref.regex_pattern.search(line)
                if match:
                    wdc = WorkingDataConnection.from_config(
                        producer_connection.type, 
                        producer_connection.name,
                        producer_connection.topic, 
                        data_connection
                    )
                    wdc.data_dict_match = match.groupdict()
                    working_data_connections.append(wdc)
        
        self.logger.info(f"Found {len(working_data_connections)} working data connections through regex in {path_file.name}")
        return working_data_connections

    @abstractmethod
    def _data_connections_transformation_and_filtering(self, working_data_connection: WorkingDataConnection) -> Dict[str, Any]:
        """
        Transforms and filters the data from a working data connection.

        This abstract method must be implemented by subclasses to define
        agent-specific logic for processing matched data. It typically involves
        extracting, transforming, or filtering the `data_dict_match` from the 
        provided `WorkingDataConnection` and returning the processed data.

        Args:
            working_data_connection (WorkingDataConnection): The working data 
                connection containing matched log line data to be transformed.

        Returns:
            Dict[str, Any]: A dictionary with the transformed and filtered data 
            that will be used for queries, messages, or further processing.

        Example:
            >>> class MyAgent(BaseAgent):
            ...     def _data_connections_transformation_and_filtering(self, wdc):
            ...         # Example transformation
            ...         return {"status": wdc.data_dict_match.get("status")}
        """
        pass

    def _data_connections_execute_queries(self) -> None:
        """
        Executes database queries for ready working data connections.

        Iterates over all `working_data_connections` and, for each connection that 
        has a `database_name` and a status of `READY`, it:
        1. Retrieves a database instance using `DatabaseFactory`.
        2. Creates a `Query` object using the connection's query and source data.
        3. Enqueues the query asynchronously and sets a callback `_on_done` 
            to handle the result.
        4. Updates the connection status to indicate that the query is running.

        If an exception occurs during query execution, the connection is reset 
        to `READY` and an error is logged.

        Notes:
            - Queries are executed asynchronously using futures.
            - `_on_done` updates the working data connection with query results.

        Example:
            >>> agent._data_connections_execute_queries()
            # Executes queries for all ready connections asynchronously.
        """
        from ..databases.factory import DatabaseFactory

        for working_data_connection in self.working_data_connections:
            if working_data_connection.database_name and working_data_connection.status == WorkingDataStatus.READY:                
                database_instance = DatabaseFactory.get_instance(
                    working_data_connection.database_type,
                    working_data_connection.database_name
                )

                def _on_done(future: Future[List[Dict[str, Any]]], wdc: WorkingDataConnection = working_data_connection) -> None:
                    wdc.on_query_done(future)

                try:
                    query: Query = Query(working_data_connection.query, working_data_connection.data_dict_query_source)
                    future = database_instance.enqueue_query(query)
                    future.add_done_callback(_on_done)
                    working_data_connection.set_query_running_status()
                except Exception:
                    working_data_connection.set_ready_status()
                    self.logger.error(f"Agent: {self.config.type}-{self.config.name}: Error executing query: {working_data_connection.query}")


    def _send_messages_to_producers(self) -> None:
        """
        Sends messages to producers based on updated working data connections.

        This method iterates over all `working_data_connections` and, for each 
        connection with status `UPDATED`, it creates a `Message` object from either 
        `data_dict_result` or `list_data_dict_query_result` and enqueues it to the 
        corresponding producer using `ProducerFactory`.

        After sending the message, the working data connection:
            - Checks if it has expired using `check_expired_time()`.
            - Resets its status to `READY`.

        Exceptions during message sending are caught, the connection's expired time 
        is reset to 0, and the error is logged.

        Notes:
            - Messages are only sent if the connection status is `UPDATED`.
            - Both single result dictionaries and lists of query results are supported.

        Example:
            >>> agent._send_messages_to_producers()
            # Sends messages for all updated connections to their configured producers.
        """
        from ..producers.factory import ProducerFactory

        for working_data_connection in self.working_data_connections:
            producer_instance = ProducerFactory.get_instance(
                working_data_connection.producer_type, 
                working_data_connection.producer_name
            )
            try:
                if working_data_connection.data_dict_result and working_data_connection.status == WorkingDataStatus.UPDATED:
                    self.logger.info(f"Agent: {self.config.type}-{self.config.name}: Sending message to producer: data with name: {working_data_connection.name} with status {working_data_connection.status}")
                    message = Message(working_data_connection.topic, working_data_connection.is_error, working_data_connection.is_warning, working_data_connection.data_dict_result)
                    producer_instance.enqueue_message(message)
                elif working_data_connection.list_data_dict_query_result and working_data_connection.status == WorkingDataStatus.UPDATED:
                    message = Message(working_data_connection.topic, working_data_connection.is_error, working_data_connection.is_warning, working_data_connection.list_data_dict_query_result)
                    producer_instance.enqueue_message(message)
                working_data_connection.check_expired_time()
                working_data_connection.set_ready_status()
            except Exception as e:
                working_data_connection.update_expired_time(0)
                self.logger.error(f"Agent: {self.config.type}-{self.config.name}: Error sending message to producer: {working_data_connection.producer_type}-{working_data_connection.producer_name}: {e}")

    def _clean_working_data_connections(self) -> None:
        """
        Removes expired working data connections from the internal list.

        Iterates over all `working_data_connections` and removes any connection 
        whose status is `EXPIRED`. Logs warnings for both the status of each 
        connection and when a connection is removed.

        This cleanup ensures that the agent's list of working data connections 
        remains current and does not retain stale or irrelevant entries.

        Notes:
            - Uses a copy of the list to safely remove items during iteration.
            - Logging provides traceability for expired connections.

        Example:
            >>> agent._clean_working_data_connections()
            # Removes all expired working data connections from the agent's internal list.
        """
        for working_data_connection in self.working_data_connections[:]:
            self.logger.warning(f"{working_data_connection.status} + {working_data_connection.name}")
            if working_data_connection.status == WorkingDataStatus.EXPIRED:
                self.logger.warning(f"Agent: {self.config.type}-{self.config.name}: Removing expired working data connection: {working_data_connection.name}")
                self.working_data_connections.remove(working_data_connection)
