from typing import Dict, Type, Generic, TypeVar

from .base import BaseDatabase, BaseDatabaseConfig

C = TypeVar('C', bound=BaseDatabaseConfig)

class DatabaseEntry(Generic[C]):
    """
    Represents a registry entry linking a database class with its configuration model.

    This generic class pairs a `BaseDatabase` subclass with a corresponding
    `BaseDatabaseConfig` subclass, enabling type-safe registration and lookup
    of database implementations.

    Attributes:
        config_model (Type[C]): The configuration model class associated with the database.
            Must be a subclass of `BaseDatabaseConfig`.
        database_class (Type[BaseDatabase]): The database class associated with this entry.
            Must be a subclass of `BaseDatabase`.
    """
    config_model: Type[C]
    database_class: Type[BaseDatabase]

DATABASE_REGISTRY: Dict[str, DatabaseEntry[BaseDatabaseConfig]] = {}
"""
Global registry mapping database types to their corresponding DatabaseEntry.

This dictionary serves as the central registry for all database implementations.
Each key is a string identifier (`database_type`) and each value is a `DatabaseEntry`
containing the database class and its associated configuration model.

Example:
    >>> DATABASE_REGISTRY["postgres"]
    DatabaseEntry(config_model=PostgresConfig, database_class=PostgresDatabase)
"""

def register_database(
    *,
    database_type: str,
    config_model: Type[BaseDatabaseConfig],
):
    """
    Decorator to register a database class in the global DATABASE_REGISTRY.

    This function associates a unique database type string with a database class
    and its configuration model. It ensures the decorated class is a subclass
    of `BaseDatabase` and creates a `DatabaseEntry` for registry lookup.

    Args:
        database_type (str): Unique string identifier for the database type.
        config_model (Type[BaseDatabaseConfig]): Configuration model class corresponding
            to the database class. Must be a subclass of `BaseDatabaseConfig`.

    Returns:
        Callable[[Type[BaseDatabase]], Type[BaseDatabase]]: A decorator that registers
        the database class in `DATABASE_REGISTRY` and sets its `.type` attribute.

    Example:
        >>> @register_database(database_type="postgres", config_model=PostgresConfig)
        ... class PostgresDatabase(BaseDatabase):
        ...     ...
        >>> DATABASE_REGISTRY["postgres"].database_class
        <class 'PostgresDatabase'>
    """
    def decorator(database_class: Type[BaseDatabase]) -> Type[BaseDatabase]:
        """
        Registers the decorated database class in the global DATABASE_REGISTRY.

        This function is returned by `register_database` and is used as a decorator.
        It performs the following steps:
            1. Ensures the decorated class is a subclass of `BaseDatabase`.
            2. Creates a `DatabaseEntry` linking the database class with its config model.
            3. Stores the entry in `DATABASE_REGISTRY` under the specified database type.
            4. Adds a `.type` attribute to the database class for easy identification.

        Args:
            database_class (Type[BaseDatabase]): The database class to register. Must be
                a subclass of `BaseDatabase`.

        Returns:
            Type[BaseDatabase]: The original database class, now registered in the registry
            and annotated with a `.type` attribute.

        Raises:
            AssertionError: If `database_class` is not a subclass of `BaseDatabase`.

        Example:
            >>> @register_database(database_type="mysql", config_model=MySQLConfig)
            ... class MySQLDatabase(BaseDatabase):
            ...     ...
            >>> MySQLDatabase.type
            "mysql"
        """
        assert issubclass(database_class, BaseDatabase)
        entry = DatabaseEntry()
        entry.config_model = config_model
        entry.database_class = database_class

        DATABASE_REGISTRY[database_type] = entry
        database_class.type = database_type
        return database_class
    
    return decorator
