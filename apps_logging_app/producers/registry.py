from typing import Dict, Type, Generic, TypeVar

from .base import BaseProducer, BaseProducerConfig

C = TypeVar('C', bound=BaseProducerConfig)

class ProducerEntry(Generic[C]):
    """
    Registry entry describing a producer and its configuration model.

    This class acts as a lightweight container used by the producer registry
    to associate a producer implementation with its corresponding
    configuration model. It enables dynamic lookup and instantiation of
    producers based on a declared producer type.

    The class is generic over the configuration model type to preserve
    type safety when accessing the producer's configuration.

    Attributes:
        config_model (Type[C]): The configuration model class associated
            with the producer. Must be a subclass of
            :class:`BaseProducerConfig`.
        producer_class (Type[BaseProducer]): The concrete producer class
            implementing the producer logic.
    """
    config_model: Type[C]
    producer_class: Type[BaseProducer]

PRODUCER_REGISTRY: Dict[str, ProducerEntry] = {}
"""
Global registry mapping producer types to producer entries.

This dictionary acts as a central registry for all available producer
implementations. Each entry maps a unique producer type identifier to
a :class:`ProducerEntry` containing the producer class and its associated
configuration model.

The registry is populated via the :func:`register_producer` decorator
at import time.

Keys:
    str: Unique producer type identifier.

Values:
    ProducerEntry: Metadata describing the registered producer.
"""

def register_producer(
    *,
    producer_type: str,
    config_model: Type[BaseProducerConfig],
):
    """
    Register a producer class and its configuration model.

    This function returns a class decorator that registers a concrete
    :class:`BaseProducer` implementation under a given producer type.
    The registration associates the producer class with its configuration
    model and stores the mapping in the global :data:`PRODUCER_REGISTRY`.

    The decorator also assigns the producer type to the decorated class
    as a class-level attribute.

    Args:
        producer_type (str): Unique identifier used to register and
            reference the producer implementation.
        config_model (Type[BaseProducerConfig]): Configuration model class
            associated with the producer. Must be a subclass of
            :class:`BaseProducerConfig`.

    Returns:
        Callable[[Type[BaseProducer]], Type[BaseProducer]]: A class
        decorator that registers the producer.

    Raises:
        AssertionError: If the decorated class is not a subclass of
            :class:`BaseProducer`.
    """

    def decorator(producer_class: Type[BaseProducer]) -> Type[BaseProducer]:
        """
        Decorate and register a producer implementation.

        This decorator validates the producer class, creates a
        :class:`ProducerEntry` linking the producer to its configuration
        model, and registers it in the global producer registry under the
        specified producer type.

        The producer type is also assigned to the producer class as a
        class-level attribute for later introspection.

        Args:
            producer_class (Type[BaseProducer]): Concrete producer class
                to be registered.

        Returns:
            Type[BaseProducer]: The original producer class, unmodified
            aside from the assigned ``type`` attribute.

        Raises:
            AssertionError: If the provided class does not inherit from
                :class:`BaseProducer`.
        """
        assert issubclass(producer_class, BaseProducer)
        entry = ProducerEntry()
        entry.config_model = config_model
        entry.producer_class = producer_class
        PRODUCER_REGISTRY[producer_type] = entry
        producer_class.type = producer_type
        return producer_class
    
    return decorator
