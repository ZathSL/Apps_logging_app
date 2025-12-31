from typing import Dict, Type, Generic, TypeVar

from .base import BaseProducer, BaseProducerConfig

C = TypeVar('C', bound=BaseProducerConfig)

class ProducerEntry(Generic[C]):
    config_model: Type[C]
    producer_class: Type[BaseProducer]

PRODUCER_REGISTRY: Dict[str, ProducerEntry] = {}


def register_producer(
    *,
    producer_type: str,
    config_model: Type[BaseProducerConfig],
):

    def decorator(producer_class: Type[BaseProducer]) -> Type[BaseProducer]:
        entry = ProducerEntry()
        entry.config_model = config_model
        entry.producer_class = producer_class

        PRODUCER_REGISTRY[producer_type] = entry
        producer_class.type = producer_type  # type: ignore
        return producer_class
    
    return decorator
