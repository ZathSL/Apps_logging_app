from typing import Dict, Type, Generic, TypeVar

from .base import BaseProducer, BaseProducerConfig

C = TypeVar('C', bound=BaseProducerConfig)

class ProducerEntry(Generic[C]):
    """
    Represents a registry entry for a producer class.

    `ProducerEntry` associates a producer class with its corresponding configuration model.
    It is used internally by the `PRODUCER_REGISTRY` to map producer types to their classes 
    and configuration schemas.

    Attributes
    ----------
    config_model : Type[C]
        The Pydantic configuration model class associated with the producer. 
        This model defines the required configuration fields for instantiating the producer.
    producer_class : Type[BaseProducer]
        The actual producer class that implements the `BaseProducer` interface.

    Usage
    -----
    Producer classes are registered in the global `PRODUCER_REGISTRY` using the
    `@register_producer` decorator, which creates a `ProducerEntry` and stores it
    under a unique producer type key.

    Example
    -------
    >>> @register_producer(producer_type="kafka", config_model=KafkaProducerConfig)
    >>> class KafkaProducer(BaseProducer):
    >>>     ...
    >>> entry = PRODUCER_REGISTRY["kafka"]
    >>> entry.producer_class  # <class 'KafkaProducer'>
    >>> entry.config_model    # <class 'KafkaProducerConfig'>
    """
    config_model: Type[C]
    producer_class: Type[BaseProducer]

PRODUCER_REGISTRY: Dict[str, ProducerEntry] = {}


def register_producer(
    *,
    producer_type: str,
    config_model: Type[BaseProducerConfig],
):

    """
    Registers a producer class with the given config model and type.

    Args:
        producer_type (str): The type of the producer to be registered.
        config_model (Type[BaseProducerConfig]): The config model for the producer.

    Returns:
        Callable[[Type[BaseProducer]], Type[BaseProducer]]: A decorator that registers the producer class with the given config model and type.
    """

    def decorator(producer_class: Type[BaseProducer]) -> Type[BaseProducer]:
        """
        A decorator that registers the producer class with the given config model and type.

        Args:
            producer_class (Type[BaseProducer]): The producer class to register.

        Returns:
            Type[BaseProducer]: The registered producer class.
        """
        assert issubclass(producer_class, BaseProducer)
        entry = ProducerEntry()
        entry.config_model = config_model
        entry.producer_class = producer_class
        PRODUCER_REGISTRY[producer_type] = entry
        producer_class.type = producer_type
        return producer_class
    
    return decorator
