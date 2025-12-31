import yaml
from pathlib import Path
from .registry import DATABASE_REGISTRY
import threading
from .orchestrator import DatabaseOrchestrator
from typing import TYPE_CHECKING, Dict, Tuple

if TYPE_CHECKING:
    from .base import BaseDatabase

class DatabaseFactory:
    _instances: Dict[Tuple[str, str], "BaseDatabase"] = {}
    _lock = threading.Lock()
    _config = None

    @classmethod
    def get_instance(cls, database_type: str, database_name: str) -> "BaseDatabase":
        key = (database_type, database_name)

        if key in cls._instances:
            return cls._instances[key]

        with cls._lock:
            if key not in cls._instances:
                cls._instances[key] = cls._create(database_type, database_name)

        return cls._instances[key]

    @classmethod
    def _create(cls, database_type: str, database_name: str) -> "BaseDatabase":
        config_path = Path(__file__).parent.parent / 'configs' / 'databases.yaml'
        if cls._config is None:
            with open(config_path) as f:
                cls._config = yaml.safe_load(f)["databases"]

        raw_config = next(
            (d for d in cls._config
             if d["type"] == database_type and d["name"] == database_name),
            None
        )

        if not raw_config:
            raise ValueError(
                f"Database config not found for type={database_type}, name={database_name}"
            )

        entry = DATABASE_REGISTRY.get(database_type)
        if not entry:
            raise ValueError(f"Unknown database type: {database_type}")

        database_config = entry.config_model.model_validate(raw_config)
        
        database_ref = entry.database_class(database_config)

        orchestrator = DatabaseOrchestrator(database_ref)

        database_ref.orchestrator = orchestrator

        database_ref.start()

        return database_ref
