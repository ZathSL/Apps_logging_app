import yaml
from pathlib import Path
from .registry import PRODUCER_REGISTRY

# Cache globale per istanze producer condivise
PRODUCER_INSTANCES = {}

class ProducerFactory:

    """
    Factory class to create and manage producer instances.

    ProducerFactory is responsible for creating producer instances based on
    a specified type and name. It ensures that each producer is instantiated
    only once by maintaining a global cache (`PRODUCER_INSTANCES`). If an
    instance already exists in the cache, it is returned directly; otherwise,
    the instance is created using the configuration loaded from the 
    `producers.yaml` file and registered producer classes.

    The factory uses the `PRODUCER_REGISTRY` to map producer types to their
    corresponding configuration model and class.

    Methods
    -------
    create(producer_type: str, producer_name: str)
        Creates a producer instance for the given type and name, using cached
        instances if available, or configuration from `producers.yaml` otherwise.

    Raises
    ------
    ValueError
        If the configuration for the specified producer type and name is not
        found in `producers.yaml`.
    ValueError
        If the producer type is not registered in `PRODUCER_REGISTRY`.

    Usage
    -----
    producer = ProducerFactory.create("kafka", "main_topic")
    """

    @staticmethod
    def create(producer_type: str, producer_name: str):

        """
        Create a producer instance based on the given type and name.

        If the instance already exists in the cache, it is returned directly.
        Otherwise, the instance is created using the configuration from the producers.yaml
        file and stored in the cache.

        Parameters
        ----------
        producer_type : str
            The type of the producer to be created
        producer_name : str
            The name of the producer to be created

        Returns
        -------
        Producer
            The created producer instance

        Raises
        ------
        ValueError
            If the producer config is not found for the given type and name
        ValueError
            If the producer type is unknown
        """
        if (producer_type, producer_name) in PRODUCER_INSTANCES:
            return

        with open(Path(__file__).parent.parent / 'configs' / 'producers.yaml') as f:
            producers_config = yaml.safe_load(f)["producers"]  

        raw_config = next((p for p in producers_config if (p["type"] == producer_type and p["name"] == producer_name)), None)

        if not raw_config:
            raise ValueError(f"Producer config not found for type: {producer_type}")

        # Create instance using the registry
        entry = PRODUCER_REGISTRY.get(producer_type)

        if not entry:
            raise ValueError(f"Unknown producer type: {producer_type}")

        producer_config = entry.config_model.model_validate(raw_config)


        instance = entry.producer_class(producer_config)


        PRODUCER_INSTANCES[(producer_type, producer_name)] = instance
        
