from abc import ABC, abstractmethod
import time
from typing import List, Dict, Any
from pathlib import Path
import logging
from concurrent.futures import Future
from functools import partial
from threading import Thread, Event

from .data import WorkingDataConnection, WorkingDataStatus
from ..utils import get_file_id
from .model import BaseAgentConfig, PathFileConfig


class BaseAgent(ABC):
    def __init__(self, config: BaseAgentConfig) -> None:
        self.config: BaseAgentConfig = config
        self.logger = logging.getLogger("__main__." +__name__)
        self.working_data_connections: List[WorkingDataConnection] = []
        self._initialize_working_data_connections()
        self._stop_event = Event()
        self._thread = Thread(target=self._worker, daemon=True)
        self._thread.start()
        self.logger.info(f"Initialized agent: {self.config.type}-{self.config.name}")


    # PUBLIC API

    def stop(self) -> None:
        self._stop_event.set()
        self._thread.join()

    # INTERNALS

    def _initialize_working_data_connections(self) -> None:
        for producer_connection in self.config.producer_connections:
            for data_connection in producer_connection.data_connections:
                working_data_connection = WorkingDataConnection.from_config(producer_connection.type, producer_connection.name, data_connection)
                working_data_connection.status = WorkingDataStatus.READY
                self.working_data_connections.append(working_data_connection)

    def _worker(self) -> None:
        self.logger.info(f"Agent: {self.config.type}-{self.config.name} started")

        while not self._stop_event.is_set():
            self._run_once()
            time.sleep(self.config.pool_interval)

    def _run_once(self) -> None:
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
        working_data_connections = self._data_connections_match_regex(path_file, lines)

        for wdc in working_data_connections:
            data_dict_tmp = self._data_connections_transformation_and_filtering(wdc)
            if wdc.query:
                wdc.data_dict_query_source = data_dict_tmp
                wdc.status = WorkingDataStatus.READY
            else:
                if data_dict_tmp != wdc.data_dict_result:
                    wdc.data_dict_result = data_dict_tmp
                    wdc.status = WorkingDataStatus.UPDATED
                else:
                    wdc.status = WorkingDataStatus.READY
 
        self.working_data_connections.extend(working_data_connections)

        self._data_connections_execute_queries()
        self._send_messages_to_producers()
        self._clean_working_data_connections()


    def _data_connections_match_regex(self, path_file: PathFileConfig, lines: List[str]) -> List[WorkingDataConnection]:

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
                            working_data_connection = WorkingDataConnection.from_config(producer_connection.type, producer_connection.name, data_connection)
                            working_data_connection.data_dict_match = match.groupdict()
                            working_data_connections.append(working_data_connection)
        self.logger.info(f"Found {len(working_data_connections)} working data connections through regex in {path_file.name}")
        return working_data_connections

    @abstractmethod
    def _data_connections_transformation_and_filtering(self, working_data_connection: WorkingDataConnection) -> Dict[str, Any]:
        pass
        
    def _on_done(self, working_data_connection: WorkingDataConnection, fut: Future[List[Dict[str, Any]]]) -> None:
        working_data_connection.on_query_done(fut)


    def _data_connections_execute_queries(self) -> None:
        from ..databases.factory import DatabaseFactory

        for working_data_connection in self.working_data_connections:
            if working_data_connection.database_name and working_data_connection.status == WorkingDataStatus.READY:                
                database_instance = DatabaseFactory.get_instance(
                    working_data_connection.database_type,
                    working_data_connection.database_name
                )
                try:
                    future = database_instance.enqueue_query(working_data_connection.query, working_data_connection.data_dict_query_source)
                    future.add_done_callback(partial(self._on_done, working_data_connection))
                    working_data_connection.status = WorkingDataStatus.QUERY_RUNNING
                except Exception:
                    working_data_connection.status = WorkingDataStatus.READY


    def _send_messages_to_producers(self) -> None:
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
                working_data_connection.status = WorkingDataStatus.READY
                working_data_connection.check_expired_time()
            except Exception:
                working_data_connection.status = WorkingDataStatus.READY

    def _clean_working_data_connections(self) -> None:
        for working_data_connection in self.working_data_connections[:]:
            if working_data_connection.status == WorkingDataStatus.EXPIRED:
                self.working_data_connections.remove(working_data_connection)
