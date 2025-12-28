import yaml
from pathlib import Path
from .registry import PRODUCER_REGISTRY

# Cache globale per istanze producer condivise
PRODUCER_INSTANCES = {}

class ProducerFactory:
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
        
