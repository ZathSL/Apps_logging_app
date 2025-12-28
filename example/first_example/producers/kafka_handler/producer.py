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
        
        
        """
        Initialize a KafkaHandlerProducer.

        :param config: The config to use for connecting to Kafka
        :type config: KafkaHandlerConfig
        """
        super().__init__(config)

    def _connect(self):
        
        """
        Connect to Kafka using the config provided.

        :raises:
            Exception: If impossible to connect to Kafka
        """
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.brokers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
        except Exception as e:
            self.logger.critical(f"Impossibile creare KafkaProducer: {e}")
            raise

    def _send(self, is_error: bool, message: Dict[str, Any]) -> None:
        # Implement sending logic to Kafka
        """
        Send a message to Kafka.

        :param is_error: If the message is an error
        :type is_error: bool
        :param message: The message to send
        :type message: Dict[str, Any]
        :raises:
            Exception: If impossible to send message
        """
        try:
            self.producer.send(self.config.topic, value={"is_error": is_error, "message": message})
            self.producer.flush()
        except Exception as e:
            self.logger.error(f"Impossible to send message: {e}")
            raise
    def _close(self) -> None:
        # Implement closing logic to Kafka
        """
        Close the Kafka producer.

        This method will flush any pending messages to be sent and then close the producer.
        """
        self.producer.flush()
        self.producer.close()