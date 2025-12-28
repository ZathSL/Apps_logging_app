import yaml
from pathlib import Path
from .registry import DATABASE_REGISTRY

# Global cache for shared database instances
DATABASE_INSTANCES = {}

class DatabaseFactory:

    """
    Factory class responsible for creating and managing database instances.

    ``DatabaseFactory`` reads database configurations from a YAML file,
    validates them using the registered config model, and instantiates
    the corresponding database class. It ensures that only a single
    instance exists for each (database_type, database_name) combination,
    acting as a global cache for database connections.

    This factory relies on:
        - `DATABASE_REGISTRY`: A mapping of database types to their
          corresponding configuration model and database class.
        - `databases.yaml`: A configuration file containing database
          definitions.

    Methods:
        create(database_type: str, database_name: str):
            Creates a new database instance or returns the cached instance
            if it already exists.

    :param database_type: The type of the database to create (must be registered).
    :type database_type: str
    :param database_name: The name of the database to create (must exist in the YAML config).
    :type database_name: str

    Raises:
        ValueError: If the database configuration is not found in the YAML file.
        ValueError: If the database type is not registered in DATABASE_REGISTRY.

    Example Usage:

    .. code-block:: python

        # Create a PostgreSQL database instance
        db_instance = DatabaseFactory.create("postgres", "users_db")

        # Subsequent calls return the same instance (cached)
        same_instance = DatabaseFactory.create("postgres", "users_db")
        assert db_instance is same_instance
    """
       
    @staticmethod
    def create(database_type: str, database_name: str):
        
        if (database_type, database_name) in DATABASE_INSTANCES:
            return 

        with open(Path(__file__).parent.parent / 'configs' / 'databases.yaml') as f:
            databases = yaml.safe_load(f)["databases"]

        raw_config = next((d for d in databases if (d["type"] == database_type and d['name'] == database_name)), None)

        if not raw_config:
            raise ValueError(f"Database config not found for type: {database_type}")

        # Create instance using the registry
        entry = DATABASE_REGISTRY.get(database_type)

        if not entry:
            raise ValueError(f"Unknown database type: {database_type}")

        database_config = entry.config_model.model_validate(raw_config)

        instance = entry.database_class(database_config)

        DATABASE_INSTANCES[(database_type, database_name)] = instance
