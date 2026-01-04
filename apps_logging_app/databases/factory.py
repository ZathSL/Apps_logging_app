import yaml
from pathlib import Path
import threading
from typing import TYPE_CHECKING, Dict, Tuple

from .orchestrator import DatabaseOrchestrator
from .registry import DATABASE_REGISTRY

if TYPE_CHECKING:
    from .base import BaseDatabase

class DatabaseFactory:
    """
    Factory and singleton manager for database instances.

    The DatabaseFactory is responsible for creating and managing shared 
    instances of databases based on their type and name. Each database 
    instance is created only once and cached to ensure that the same 
    connection and execution resources are reused across the application.

    Database configurations are loaded lazily from a YAML file and validated 
    using the configuration model registered in DATABASE_REGISTRY. Thread 
    safety is ensured during instance creation via a class-level lock.

    This class should not be instantiated; all interactions are performed 
    via class methods.

    Attributes:
        _instances (Dict[Tuple[str, str], BaseDatabase]): A cache of created 
            database instances, keyed by (database_type, database_name).
        _lock (threading.Lock): A class-level lock to guarantee thread-safe 
            instance creation.
        _config (Optional[dict]): Loaded database configurations from the 
            YAML file. Initialized on first access.
    """
    _instances: Dict[Tuple[str, str], "BaseDatabase"] = {}
    _lock = threading.Lock()
    _config = None

    @classmethod
    def get_instance(cls, database_type: str, database_name: str) -> "BaseDatabase":
        """
        Returns a shared database instance for the specified type and name.

        If an instance with the given type and name already exists in the cache,
        it is returned. Otherwise, a new instance is created using the internal
        `_create` method, cached, and then returned.

        Thread safety is ensured via a class-level lock to prevent multiple
        threads from creating the same database instance simultaneously.

        Args:
            database_type (str): The type of the database (must be registered in DATABASE_REGISTRY).
            database_name (str): The name of the database configuration to use.

        Returns:
            BaseDatabase: A singleton instance of the requested database.

        Raises:
            ValueError: If the database configuration is missing or the database type is unknown.
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
        Creates a new database instance for the specified type and name.

        This is an internal method used by `get_instance` to instantiate
        databases that are not yet cached. It performs the following steps:

        1. Loads database configurations from the YAML file (`configs/databases.yaml`) 
        if they are not already loaded.
        2. Searches for a configuration matching the given `database_type` and 
        `database_name`.
        3. Validates the configuration using the config model registered in 
        `DATABASE_REGISTRY`.
        4. Instantiates the concrete database class.
        5. Creates a `DatabaseOrchestrator` for the database and assigns it.
        6. Starts the database (e.g., dispatcher threads or connection handling).
        7. Returns the newly created database instance.

        Args:
            database_type (str): The type of the database (must be registered in DATABASE_REGISTRY).
            database_name (str): The name of the database configuration to use.

        Returns:
            BaseDatabase: A newly created instance of the requested database.

        Raises:
            ValueError: If no configuration is found for the given type and name,
                        or if the database type is not registered in DATABASE_REGISTRY.
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
