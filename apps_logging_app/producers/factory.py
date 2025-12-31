import yaml
from pathlib import Path
from .registry import PRODUCER_REGISTRY
import threading
from .orchestrator import ProducerOrchestrator

class ProducerFactory:
    _instances = {}
    _lock = threading.Lock()
    _config = None

    @classmethod
    def get_instance(cls, producer_type: str, producer_name: str):
        key = (producer_type, producer_name)

        if key in cls._instances:
            return cls._instances[key]
        
        with cls._lock:
            if key not in cls._instances:
                cls._instances[key] = cls._create(producer_type, producer_name)
        return cls._instances[key]
    
    @classmethod
    def _create(cls, producer_type: str, producer_name: str):
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
        
        producer_ref = entry.producer_class(producer_config)

        orchestrator = ProducerOrchestrator(producer_ref)

        producer_ref.orchestrator = orchestrator

        producer_ref.start()

        return producer_ref
    