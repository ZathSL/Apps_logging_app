from abc import ABC, abstractmethod
import time
from typing import List, Dict, Any
from pathlib import Path
import logging
from concurrent.futures import Future
from threading import Thread, Event

from .data import WorkingDataConnection, WorkingDataStatus
from ..utils import get_file_id
from .model import BaseAgentConfig, PathFileConfig
from datetime import datetime, timedelta

class BaseAgent(ABC):
    """
    Abstract base class for all agent implementations.

    The :class:`BaseAgent` defines the core lifecycle, threading model, and
    data-processing pipeline for agents. Concrete agents must extend this
    class and implement the abstract transformation and filtering logic.

    Responsibilities of this class include:

    - Initializing and managing :class:`WorkingDataConnection` instances
    - Monitoring and reading log files with rotation handling
    - Matching data via regular expressions
    - Executing database queries asynchronously
    - Sending processed data to producers
    - Managing agent lifecycle (start/stop) in a background thread

    Each agent runs in its own worker thread and periodically processes
    configured log files according to the provided configuration.

    Subclasses must implement the
    :meth:`_data_connections_transformation_and_filtering` method.

    :param config: Configuration object used to initialize the agent.
    :type config: BaseAgentConfig
    """
    def __init__(self, config: BaseAgentConfig) -> None:
        """
        Initializes the BaseAgent with the given config.

        :param config: The config for the agent.
        :type config: BaseAgentConfig
        """
        self.config: BaseAgentConfig = config
        self.logger = logging.getLogger("__main__." +__name__)
        self.working_data_connections: List[WorkingDataConnection] = []
        self._initialize_working_data_connections()
        self._stop_event = Event()
        self._thread = Thread(target=self._worker, daemon=True)
        self.next_execute_query_time = datetime.now()
        self.logger.info(f"Initialized agent: {self.config.type}-{self.config.name}")


    # PUBLIC API

    def start(self) -> None:
        """
        Starts the agent.

        This method starts the worker thread which will execute the agent's
        processing loop.

        :return: None
        """
        self._thread.start()

    def stop(self) -> None:
        """
        Stops the agent.

        This method sets the stop event and waits for the worker thread to finish.

        :return: None
        """
        self._stop_event.set()
        self._thread.join()

    # INTERNALS

    def _initialize_working_data_connections(self) -> None:
        """
        Initializes the working data connections for the agent.

        This method iterates over the producer connections and data connections in the
        agent's config and creates a WorkingDataConnection for each data connection.
        The WorkingDataConnection is set to the READY status and added to the
        agent's list of working data connections.

        :return: None
        """
        for producer_connection in self.config.producer_connections:
            for data_connection in producer_connection.data_connections:
                working_data_connection = WorkingDataConnection.from_config(producer_connection.type, producer_connection.name, data_connection)
                working_data_connection.set_ready_status()
                self.working_data_connections.append(working_data_connection)

    def _worker(self) -> None:
        """
        Runs the agent's processing loop in a separate thread.

        This method starts a logging message indicating that the agent has
        started and then enters a loop where it calls `_run_once` to process
        data and then waits for the specified fetch logs interval before
        repeating the loop.

        The loop exits when the stop event is set.

        :return: None
        """
        self.logger.info(f"Agent: {self.config.type}-{self.config.name} started")

        while not self._stop_event.is_set():
            self._run_once()
            time.sleep(self.config.fetch_logs_interval)

    def _run_once(self) -> None:
        """
        Runs the agent's processing loop once.

        This method iterates over the configured path files, detects file rotations, reads
        log files in batches, and processes the extracted data using configured data
        connections.

        For each path file, it checks if the file ID has changed. If the file ID is
        None, it sets the file ID to the current file ID and reads a batch of lines
        from the file using `_read_batch_log`. If the file ID has changed, it reads any
        remaining lines from the previous file using `_read_remaining_old_file`, resets
        the cursor to 0, and sets the file ID to the current file ID. If the file ID
        has not changed, it reads a batch of lines from the file using `_read_batch_log`.

        After reading the lines, it calls `_data_connections_flow` to process the lines
        using the configured data connections.

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

    def _read_remaining_old_file(self, path_file: PathFileConfig) -> List[str]:
        """
        Reads the remaining lines from a previous file with the same file_id as the given path_file.

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
        Reads a batch of lines from a log file.

        Given a PathFileConfig, this method reads the specified number of lines from the file and
        updates the cursor of the PathFileConfig to the position after the last read line.

        If the end of the file is reached before the specified number of lines is read, the
        method will return the remaining lines.

        :param path_file: The PathFileConfig for which the lines should be read.
        :return: A list of lines read from the file.
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
        Matches data using regex patterns defined in DataConnectionConfig.

        This method takes a PathFileConfig and a list of lines as input, and processes
        the data connections matched by the regex in the read lines. It iterates over the
        given lines and the data connections in the agent's config, and for each data
        connection with a matching regex pattern, it creates a WorkingDataConnection and
        adds it to the list of working data connections.

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
                    if data_connection.source_ref and \
                        data_connection.source_ref.path_file_name == path_file.name:
                        regex_pattern = data_connection.source_ref.regex_pattern
                        match = regex_pattern.search(line)
                        if match:
                            working_data_connection = WorkingDataConnection.from_config(producer_connection.type, 
                                                                                        producer_connection.name, 
                                                                                        data_connection)
                            working_data_connection.data_dict_match = match.groupdict()
                            working_data_connections.append(working_data_connection)
        self.logger.info(f"Found {len(working_data_connections)} working data connections through regex in {path_file.name}")
        return working_data_connections

    @abstractmethod
    def _data_connections_transformation_and_filtering(self, working_data_connection: WorkingDataConnection) -> Dict[str, Any]:
        """
        Abstract method to transform and filter the given working data connections.

        :param working_data_connection: The working data connection to transform and filter.
        :return: A dictionary containing the transformed and filtered data.
        """
        pass

    def _data_connections_execute_queries(self) -> None:
        """
        Executes queries using the data connections.

        This method iterates over the working data connections that have a database name
        and are in the READY status. It executes the query for each data connection using
        the corresponding database instance, and sets the query running status for each
        data connection. If an exception occurs during the query execution, it sets
        the ready status for the data connection and logs the error.

        :return: None
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
                    future = database_instance.enqueue_query(working_data_connection.query, working_data_connection.data_dict_query_source)
                    future.add_done_callback(_on_done)
                    working_data_connection.status = WorkingDataStatus.QUERY_RUNNING
                except Exception:
                    working_data_connection.set_ready_status()
                    self.logger.error(f"Agent: {self.config.type}-{self.config.name}: Error executing query: {working_data_connection.query}")


    def _send_messages_to_producers(self) -> None:
        """
        Sends the extracted messages to the configured producers.

        This method iterates over the working data connections, and for each data connection with a
        producer type and name, it sends the extracted messages to the corresponding producer. If an
        exception occurs during the message sending, it sets the expired status for the data
        connection and logs the error.

        :return: None
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
                    producer_instance.enqueue_message(working_data_connection.is_error, working_data_connection.data_dict_result)
                elif working_data_connection.list_data_dict_query_result and working_data_connection.status == WorkingDataStatus.UPDATED:
                    producer_instance.enqueue_message(working_data_connection.is_error, working_data_connection.list_data_dict_query_result)
                working_data_connection.set_ready_status()
                working_data_connection.check_expired_time()
            except Exception:
                working_data_connection.status = WorkingDataStatus.EXPIRED
                self.logger.error(f"Agent: {self.config.type}-{self.config.name}: Error sending message to producer: {working_data_connection.producer_type}-{working_data_connection.producer_name}")

    def _clean_working_data_connections(self) -> None:
        """
        Cleans expired working data connections from the list of working data connections.

        This method iterates over the list of working data connections and for each one, it checks
        if the status of the working data connection is expired. If it is, it removes the
        working data connection from the list of working data connections.

        :return: None
        """
        for working_data_connection in self.working_data_connections[:]:
            self.logger.warning(f"{working_data_connection.status} + {working_data_connection.name}")
            if working_data_connection.status == WorkingDataStatus.EXPIRED:
                self.logger.warning(f"Agent: {self.config.type}-{self.config.name}: Removing expired working data connection: {working_data_connection.name}")
                self.working_data_connections.remove(working_data_connection)
