from ..orchestration.orchestrator import BaseOrchestrator
from .base import BaseProducer

class ProducerOrchestrator(BaseOrchestrator):

    def __init__(self, producer: BaseProducer):
        super().__init__(
            name=f"Producer-{producer.config.type}-{producer.config.name}",
            max_retries=producer.config.max_retries
        )
        self.producer = producer

    def _connect(self) -> None:
        self.producer.connect()

    def close(self) -> None:
        self.producer.close()
