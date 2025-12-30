import yaml
from pathlib import Path
from .registry import PRODUCER_REGISTRY
import threading

class ProducerFactory:

    """
    Factory class responsible for creating and managing shared producer instances.

    ``ProducerFactory`` implements a *multiton* pattern (singleton per key),
    ensuring that only one producer instance exists for each
    ``(producer_type, producer_name)`` combination.

    Producer instances represent shared, long-lived resources (e.g. message
    brokers, event producers, streaming clients) and are therefore cached
    globally within the factory. Subsequent requests for the same producer
    return the cached instance.

    The factory loads producer configurations from a YAML file and validates
    them using the registered configuration model before instantiating the
    corresponding producer class.

    This factory relies on:

    - ``PRODUCER_REGISTRY``: a registry mapping producer types to their
      configuration models and producer implementation classes.
    - ``producers.yaml``: a configuration file containing producer definitions.

    **Responsibilities:**

    - Manage the lifecycle of shared producer instances
    - Ensure thread-safe creation of producer instances
    - Validate producer configurations
    - Instantiate the correct producer implementation

    .. note::

        The factory is thread-safe and lazily initializes both the producer
        configuration and producer instances.

    """


    _instances = {}
    _lock = threading.Lock()
    _config = None

    @classmethod
    def get_instance(cls, producer_type: str, producer_name: str):
        """
        Returns a shared producer instance for the given type and name.

        If the instance already exists in the cache, it is returned directly;
        otherwise, the instance is created using the configuration from the producers.yaml
        file and stored in the cache.

        Parameters
        ----------
        producer_type : str
            The type of the producer to be created
        producer_name : str
            The name of the producer to be created

        Returns
        -------
        The shared producer instance
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
        Creates a new producer instance using the configuration from the YAML file.

        Args:
            producer_type (str): The type of the producer to create.
            producer_name (str): The name of the producer to create.

        Raises:
            ValueError: If the producer configuration is not found in the YAML file.
            ValueError: If the producer type is not registered in PRODUCER_REGISTRY.

        Returns:
            BaseProducer: The instantiated producer class.
        """
        config_path = Path(__file__).parent.parent / 'configs' / 'producers.yaml'
        if cls._config is None:
            with open(config_path) as f:
                cls._config = yaml.safe_load(f)["producers"]

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
        return entry.producer_class(producer_config)
    