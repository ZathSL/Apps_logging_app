from ..orchestration.orchestrator import BaseOrchestrator
from .base import BaseDatabase

class DatabaseOrchestrator(BaseOrchestrator):

    def __init__(self, database: BaseDatabase):
        super().__init__(
            name=f"DB-{database.config.type}-{database.config.name}",
            max_retries=database.config.max_retries
        )
        self.database = database

    def _connect(self) -> None:
        self.database.connect()

    def close(self) -> None:
        self.database.close()
