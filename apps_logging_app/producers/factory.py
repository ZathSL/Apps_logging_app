import yaml
from typing import Dict, Tuple, TYPE_CHECKING
from pathlib import Path
from .registry import PRODUCER_REGISTRY
import threading
from .orchestrator import ProducerOrchestrator

if TYPE_CHECKING:
    from .base import BaseProducer


class ProducerFactory:
    """
    Factory class for creating and managing shared producer instances.

    The `ProducerFactory` provides a centralized mechanism to create,
    cache, and retrieve instances of producers defined in the system. It
    ensures that there is only one shared instance per producer type and name.

    Attributes
    ----------
    _instances : Dict[Tuple[str, str], BaseProducer]
        Internal cache mapping `(producer_type, producer_name)` tuples to
        producer instances.
    _lock : threading.Lock
        Thread lock to ensure thread-safe access when creating new producer instances.
    _config : Optional[list]
        Cached configuration loaded from the YAML file for all producers.

    Methods
    -------
    get_instance(producer_type: str, producer_name: str) -> BaseProducer
        Returns a shared producer instance for the given type and name.
        If the instance does not exist, it will be created and started.
    _create(producer_type: str, producer_name: str) -> BaseProducer
        Internal method to create a new producer instance based on the
        configuration from the YAML file, register an orchestrator, and
        start the producer.

    Notes
    -----
    - The factory reads configuration from `configs/producers.yaml`.
    - Producer instances are automatically wrapped with a `ProducerOrchestrator`
      to manage connection lifecycle and retries.
    - The class is thread-safe: multiple threads can request producer instances
      without creating duplicates.
    - Raises `ValueError` if the producer type is unknown or the configuration
      is missing from the YAML file.
    """
    _instances: Dict[Tuple[str, str], "BaseProducer"] = {}
    _lock = threading.Lock()
    _config = None

    @classmethod
    def get_instance(cls, producer_type: str, producer_name: str):
        """
        Returns a shared producer instance for the given type and name.

        The instance is cached globally, and subsequent calls with the same
        key will return the same instance.

        Args:
            producer_type (str): The type of the producer to be created.
            producer_name (str): The name of the producer to be created.

        Returns:
            BaseProducer: The shared producer instance.
        """
        key = (producer_type, producer_name)

        if key in cls._instances:
            return cls._instances[key]
        
        with cls._lock:
            if key not in cls._instances:
                cls._instances[key] = cls._create(producer_type, producer_name)
        return cls._instances[key]
    
    @classmethod
    def _create(cls, producer_type: str, producer_name: str):
        """
        Creates a new producer instance based on the configuration from the YAML file.

        Args:
            producer_type (str): The type of the producer to be created.
            producer_name (str): The name of the producer to be created.

        Raises:
            ValueError: If the producer configuration is not found in the YAML file.
            ValueError: If the producer type is not registered in PRODUCER_REGISTRY.

        Returns:
            BaseProducer: The created producer instance.
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
        
        producer_ref = entry.producer_class(producer_config)

        orchestrator = ProducerOrchestrator(producer_ref)

        producer_ref.orchestrator = orchestrator

        producer_ref.start()

        return producer_ref
    