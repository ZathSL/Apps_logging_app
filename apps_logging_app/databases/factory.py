import yaml
from pathlib import Path
from .registry import DATABASE_REGISTRY
import threading
from .orchestrator import DatabaseOrchestrator
from typing import TYPE_CHECKING, Dict, Tuple

if TYPE_CHECKING:
    from .base import BaseDatabase

class DatabaseFactory:
    """
    Factory and singleton manager for database instances.

    The :class:`DatabaseFactory` is responsible for creating and managing
    shared instances of databases based on their type and name. Each
    database instance is created once and cached globally to ensure that
    the same connection and execution resources are reused across the
    application.

    Database configurations are loaded lazily from a YAML configuration
    file and validated using the configuration model registered in
    :data:`DATABASE_REGISTRY`.

    Thread safety is guaranteed during instance creation via a class-level
    lock.

    Responsibilities include:

    - Loading database configurations from YAML
    - Validating configurations using registered config models
    - Instantiating concrete database implementations
    - Wiring databases with a :class:`DatabaseOrchestrator`
    - Starting database dispatcher threads
    - Caching and returning shared database instances

    Database types must be registered in :data:`DATABASE_REGISTRY`
    prior to usage.

    This class should not be instantiated.
    All interactions are performed via class methods.
    """
    _instances: Dict[Tuple[str, str], "BaseDatabase"] = {}
    _lock = threading.Lock()
    _config = None

    @classmethod
    def get_instance(cls, database_type: str, database_name: str) -> "BaseDatabase":
        """
        Returns a shared database instance for the given type and name.

        The instance is cached globally, and subsequent calls with the same
        database type and name will return the same instance.

        Args:
            database_type (str): The type of the database (must be registered in DATABASE_REGISTRY).
            database_name (str): The name of the database (must exist in the YAML config).

        Returns:
            BaseDatabase: The shared database instance.
        """
        key = (database_type, database_name)

        if key in cls._instances:
            return cls._instances[key]

        with cls._lock:
            if key not in cls._instances:
                cls._instances[key] = cls._create(database_type, database_name)

        return cls._instances[key]

    @classmethod
    def _create(cls, database_type: str, database_name: str) -> "BaseDatabase":
        """
        Creates a new database instance using the configuration from the YAML file.

        Args:
            database_type (str): The type of the database (must be registered in DATABASE_REGISTRY).
            database_name (str): The name of the database (must exist in the YAML config).

        Raises:
            ValueError: If the database configuration is not found in the YAML file.
            ValueError: If the database type is not registered in DATABASE_REGISTRY.

        Returns:
            BaseDatabase: The instantiated database class.
        """
        config_path = Path(__file__).parent.parent / 'configs' / 'databases.yaml'
        if cls._config is None:
            try:
                with open(config_path) as f:
                    cls._config = yaml.safe_load(f)["databases"]
            except FileNotFoundError:
                cls._config = []
                
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
