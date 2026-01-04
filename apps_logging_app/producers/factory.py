import yaml
from typing import Dict, Tuple, TYPE_CHECKING
from pathlib import Path
import threading

from .registry import PRODUCER_REGISTRY
from .orchestrator import ProducerOrchestrator

if TYPE_CHECKING:
    from .base import BaseProducer


class ProducerFactory:
    """
    Factory class to create and manage producer instances in a thread-safe manner.

    This class ensures that each combination of `producer_type` and `producer_name`
    has a single shared instance (singleton per key). It reads producer configurations
    from a YAML file and integrates them with registered producer classes and orchestrators.

    Attributes:
        _instances (Dict[Tuple[str, str], "BaseProducer"]): Cached producer instances, keyed by (producer_type, producer_name).
        _lock (threading.Lock): Lock to ensure thread-safe creation of producer instances.
        _config (Optional[dict]): Cached configuration loaded from the producers YAML file.
    """

    _instances: Dict[Tuple[str, str], "BaseProducer"] = {}
    _lock = threading.Lock()
    _config = None

    @classmethod
    def get_instance(cls, producer_type: str, producer_name: str, topic: str = None) -> "BaseProducer":
        """
        Returns a singleton producer instance for the given type and name, creating it if necessary.

        This method ensures that only one instance exists per `(producer_type, producer_name)` key.
        It is thread-safe, using a lock to synchronize creation when multiple threads request the
        same producer simultaneously.

        Args:
            producer_type (str): The type of the producer to retrieve.
            producer_name (str): The name of the producer to retrieve.
            topic (str, optional): Optional topic that the producer should be associated with.
                Defaults to None.

        Returns:
            BaseProducer: The producer instance corresponding to the given type and name.

        Raises:
            ValueError: If the producer configuration or topic is invalid, as raised by `_create`.
        """
        key = (producer_type, producer_name)

        if key in cls._instances:
            return cls._instances[key]
        
        with cls._lock:
            if key not in cls._instances:
                cls._instances[key] = cls._create(producer_type, producer_name, topic)
        return cls._instances[key]
    
    @classmethod
    def _create(cls, producer_type: str, producer_name: str, topic: str = None):
        """
        Creates a new producer instance based on the provided type, name, and optional topic.

        This method loads the producer configuration from a YAML file, validates the
        producer type and topic, instantiates the producer class, attaches a
        `ProducerOrchestrator`, starts the producer, and returns it. It is intended
        to be used internally by `get_instance` and ensures proper initialization
        of producer instances.

        Args:
            producer_type (str): The type of the producer to create. Must exist in `PRODUCER_REGISTRY`.
            producer_name (str): The name of the producer to create.
            topic (str, optional): Optional topic to validate against the producer's allowed topics.
                Defaults to None.

        Returns:
            BaseProducer: The newly created and started producer instance.

        Raises:
            ValueError: If the producer configuration is not found in the YAML file.
            ValueError: If the producer type is not registered in `PRODUCER_REGISTRY`.
            ValueError: If the provided topic is not included in the producer's configured topics.
        """
        config_path = Path(__file__).parent.parent / 'configs' / 'producers.yaml'
        if cls._config is None:
            try:
                with open(config_path) as f:
                    cls._config = yaml.safe_load(f)["producers"]
            except FileNotFoundError:
                cls._config = []

        raw_config = next(
            (p for p in cls._config
             if p["type"] == producer_type and p["name"] == producer_name),
            None
        )

        if not raw_config:
            raise ValueError(
                f"Producer config not found for type={producer_type}, name={producer_name}"
            )
        

        entry = PRODUCER_REGISTRY.get(producer_type)
        if not entry:
            raise ValueError(f"Unknown producer type: {producer_type}")
        

        producer_config = entry.config_model.model_validate(raw_config)
        
        if producer_config.topics and topic not in producer_config.topics:
            raise ValueError(f"Topic {topic} not found in producer {producer_type}")

        producer_ref = entry.producer_class(producer_config)

        orchestrator = ProducerOrchestrator(producer_ref)

        producer_ref.orchestrator = orchestrator

        producer_ref.start()

        return producer_ref
    