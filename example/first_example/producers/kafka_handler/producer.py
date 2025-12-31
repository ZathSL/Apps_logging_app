from ..base import BaseProducer
from ..registry import register_producer
from .config import KafkaHandlerConfig
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
from kafka.errors import NodeNotReadyError, TopicAlreadyExistsError

@register_producer(
    producer_type="kafka_handler",
    config_model=KafkaHandlerConfig,
)
class KafkaHandlerProducer(BaseProducer):
    def __init__(self, config: KafkaHandlerConfig):
        super().__init__(config)

    def connect(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.brokers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except Exception as e:
            self.logger.critical(f"Impossibile creare KafkaProducer: {e}")
            raise

    def close(self) -> None:
        self.producer.flush()
        self.producer.close()

    def _send(self, is_error: bool, message: Any) -> None:
        try:
            self.producer.send(self.config.topic, value={"is_error": is_error, "message": message})
            self.logger.info(f"Producer {self.config.type}-{self.config.name}: Message {message} sent to Kafka")
            self.producer.flush()
        except Exception:
            raise
        