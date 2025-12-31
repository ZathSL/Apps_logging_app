from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
import logging
from queue import Queue, Empty
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor, Future
from .model import BaseDatabaseConfig, QueryTask

class BaseDatabase(ABC):

    def __init__(self, config: BaseDatabaseConfig) -> None:

        self.config = config
        self.logger = logging.getLogger("__main__." +__name__)
        self._queue = Queue()
        self._stop_event = Event()
        self._executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        self._dispatcher = Thread(target=self._dispatch, daemon=True)
        self.orchestrator = None
        self.logger.info(f"Initialized database: {self.config.type}-{self.config.name}")

    # PUBLIC API

    def start(self) -> None:
        self._dispatcher.start()


    def enqueue_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Future[List[Dict[str, Any]]]:
        try:
            task = QueryTask(query, params)
            self._queue.put(task)
            return task.future
        except Exception as e:
            self.logger.error(f"Client DB {self.config.type}-{self.config.name}: Queue full or error putting query: {e}")
            raise

    def stop(self, timeout: float | None = None) -> None:
        self._stop_event.set()
        self._dispatcher.join(timeout=timeout)
        self._queue.join()
        self._executor.shutdown(wait=True)
        self.close()
        self.logger.info(f"Database {self.config.type}-{self.config.name} shut down")

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    # INTERNALS
    
    def _dispatch(self) -> None:

        self.orchestrator.ensure_connected()

        while not self._stop_event.is_set():
            try:
                task = self._queue.get(timeout=0.5)
            except Empty:
                continue

            self.orchestrator.ensure_connected()
            future = self._executor.submit(self._query, task)
            self.logger.info(f"Client DB {self.config.type}-{self.config.name}: Query dispatched: {task.query}")
           
            def _callback(f, task=task):
                try:
                    task.future.set_result(f.result())
                    self.logger.debug(f"Client DB {self.config.type}-{self.config.name}: Query result: {task.query}: {f.result()}")
                    self.logger.info(f"Client DB {self.config.type}-{self.config.name}: Query completed: {task.query}")
                except Exception as e:
                    self.logger.warning(f"Client DB {self.config.type}-{self.config.name}: Query failed: {task.query}: {e}")
                    self.orchestrator.mark_disconnected()
                    self._queue.put(task)
                finally:
                    self._queue.task_done()
            
            future.add_done_callback(_callback)

    @abstractmethod
    def _query(self, task: QueryTask) -> List[Dict[str, Any]]:
        pass
