from abc import ABC, abstractmethod
import logging
from typing import Dict, Any
from queue import Queue, Empty
from threading import Thread, Event
from .model import BaseProducerConfig

class BaseProducer(ABC):

    def __init__(self, config: BaseProducerConfig):

        self.config = config
        self.logger = logging.getLogger("__main__." +__name__)
        self._queue = Queue()
        self._stop_event = Event()
        self._worker_thread = Thread(
            target=self._worker,
            name=f"{self.config.type}-{self.config.name}-producer-worker",
            daemon=True
        )
        self.orchestrator = None
        self.logger.info(f"Initialized producer: {self.config.type}-{self.config.name}")


    # PUBLIC API

    def start(self) -> None:
        self._worker_thread.start()

    def enqueue_message(self, is_error: bool, message: Dict[str, Any]) -> None:
        try:
            self._queue.put({"is_error": is_error, "message": message, "retries": 0})
            self.logger.info(f"Putting message in queue: {message}")
        except Exception as e:
            self.logger.error(f"Producer {self.config.type}-{self.config.name}: Queue full or error putting message: {e}")
            raise

    def stop(self, timeout: float | None = None) -> None:
        self._stop_event.set()
        self._worker_thread.join(timeout=timeout)
        self.close()
        self.logger.info(f"Producer {self.config.type}-{self.config.name}: Producer shut down")

    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    # INTERNALS

    def _worker(self) -> None:

        self.orchestrator.ensure_connected()

        while not self._stop_event.is_set():
            try:
                payload = self._queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                self.orchestrator.ensure_connected()
                self._send(payload['is_error'], payload['message'])
            except Exception:
                self.logger.warning(f"Producer {self.config.type}-{self.config.name}: Failed to send message")
                self.orchestrator.mark_disconnected()
                self._queue.put(payload)
            finally:
                self._queue.task_done()

    @abstractmethod
    def _send(self, is_error: bool, message: Any) -> None:
        pass
    
