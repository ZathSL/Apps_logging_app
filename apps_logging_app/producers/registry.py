from typing import Dict, Type, Generic, TypeVar

from .base import BaseProducer, BaseProducerConfig

C = TypeVar('C', bound=BaseProducerConfig)

class ProducerEntry(Generic[C]):

    """
    A registry entry for a producer class.

    This class binds a producer type to its configuration model and
    producer class, allowing dynamic creation of producer instances
    via the ProducerFactory.

    Attributes
    ----------
    config_model : Type[C]
        The Pydantic model class used to validate and parse the producer configuration.
    producer_class : Type[BaseProducer]
        The concrete producer class that will be instantiated.
    """

    config_model: Type[C]
    producer_class: Type[BaseProducer]

PRODUCER_REGISTRY: Dict[str, ProducerEntry] = {}

"""
A global registry that maps producer types (str) to their corresponding ``ProducerEntry`` objects.

This registry is used by factories like ``ProducerFactory`` to look up and create producer
instances based on their type.
"""

def register_producer(
    *,
    producer_type: str,
    config_model: Type[BaseProducerConfig],
):
    """
    Registers a producer class with the given config model and type.

    Args:
        producer_type (str): The type of the producer.
        config_model (Type[BaseProducerConfig]): The config model for the producer.

    Returns:
        Callable[[Type[BaseProducer]], Type[BaseProducer]]: A decorator that registers the producer class with the given config model and type.
    """
    def decorator(producer_class: Type[BaseProducer]) -> Type[BaseProducer]:
        """
        A decorator that registers the producer class with the given config model and type.

        This decorator takes in a producer class and registers it in the PRODUCER_REGISTRY
        with the given config model and type. It also sets the type attribute of the producer
        class to the given type.

        Args:
            producer_class (Type[BaseProducer]): The producer class to register.

        Returns:
            Type[BaseProducer]: The registered producer class.
        """
        entry = ProducerEntry()
        entry.config_model = config_model
        entry.producer_class = producer_class

        PRODUCER_REGISTRY[producer_type] = entry
        producer_class.type = producer_type  # type: ignore
        return producer_class
    
    return decorator
