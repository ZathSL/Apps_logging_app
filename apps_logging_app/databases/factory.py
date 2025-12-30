import yaml
from pathlib import Path
from .registry import DATABASE_REGISTRY
import threading

class DatabaseFactory:

    """
    Factory class responsible for creating and managing shared database instances.

    ``DatabaseFactory`` implements a *multiton* pattern (singleton per key),
    ensuring that only one database instance exists for each
    ``(database_type, database_name)`` combination.

    Database instances are considered shared resources and are cached
    globally within the factory. Subsequent calls requesting the same
    database type and name will return the same instance.

    The factory loads database configurations from a YAML file and validates
    them using the registered configuration model before instantiating the
    corresponding database class.

    This factory relies on:

    - ``DATABASE_REGISTRY``: a registry mapping database types to their
      configuration models and database implementation classes.
    - ``databases.yaml``: a configuration file containing database definitions.

    **Responsibilities:**

    - Manage the lifecycle of shared database instances
    - Ensure thread-safe creation of database instances
    - Validate database configurations
    - Instantiate the correct database implementation

    .. note::

        The factory is thread-safe and lazily initializes both the database
        configuration and database instances.

    """

    _instances = {}
    _lock = threading.Lock()
    _config = None

    @classmethod
    def get_instance(cls, database_type: str, database_name: str):
        """
        Returns a shared database instance for the given type and name.

        The instance is cached globally, and subsequent calls with the same type and name
        will return the same instance.

        :param database_type: The type of the database (must be registered in DATABASE_REGISTRY).
        :type database_type: str
        :param database_name: The name of the database (must exist in the YAML config).
        :type database_name: str
        :return: The shared database instance.
        :rtype: BaseDatabase
        """
        key = (database_type, database_name)

        if key in cls._instances:
            return cls._instances[key]

        with cls._lock:
            if key not in cls._instances:
                cls._instances[key] = cls._create(database_type, database_name)

        return cls._instances[key]

    @classmethod
    def _create(cls, database_type: str, database_name: str):

        """Creates a new database instance using the configuration from the YAML file.

        Args:
            database_type (str): The type of the database to create.
            database_name (str): The name of the database to create.

        Raises:
            ValueError: If the database configuration is not found in the YAML file.
            ValueError: If the database type is not registered in DATABASE_REGISTRY.

        Returns:
            BaseDatabase: The instantiated database class.
        """
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
        return entry.database_class(database_config)
