from abc import ABC, abstractmethod
import logging
import time
import threading

class BaseOrchestrator(ABC):

    def __init__(self, name: str, max_retries: int, retry_delay: int = 5):
        self.name = name
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logging.getLogger("__main__." + __name__)

        self._connected = False
        self._lock = threading.Lock()

    def ensure_connected(self) -> None:
        if self._connected:
            return

        with self._lock:
            if self._connected:
                return

            self.logger.warning(f"{self.name}: Connection not active, trying to connect")
            self._connect_with_retry()

    def _connect_with_retry(self):
        for attempt in range(1, self.max_retries + 1):
            try:
                self._connect()
                self._connected = True
                self.logger.info(f"{self.name}: Connected")
                return
            except Exception as e:
                self.logger.error(
                    f"{self.name}: Connection failed (attempt {attempt}/{self.max_retries}): {e}"
                )
                time.sleep(self.retry_delay)

        self.logger.error(f"{self.name}: Connection failed after {self.max_retries} attempts")
        time.sleep(120)
        self._connect_with_retry()


    def mark_disconnected(self):
        self.logger.warning(f"{self.name}: Marked as disconnected")
        self._connected = False

    @abstractmethod
    def _connect(self) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
