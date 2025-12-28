from typing import Dict, Type, Generic, TypeVar

from .base import BaseDatabase, BaseDatabaseConfig

C = TypeVar('C', bound=BaseDatabaseConfig)

class DatabaseEntry(Generic[C]):
    config_model: Type[C]
    database_class: Type[BaseDatabase]

DATABASE_REGISTRY: Dict[str, DatabaseEntry] = {}

def register_database(
    *,
    database_type: str,
    config_model: Type[BaseDatabaseConfig],
):
    """
    Registers a database class with the given config model and type.

    Args:
        database_class (Type[BaseDatabase]): The database class to register.

    Returns:
        Type[BaseDatabase]: The registered database class.

    The decorator sets the type attribute of the database class to the given type.
    It also creates a DatabaseEntry and adds it to the DATABASE_REGISTRY with the given type as the key.
    """
    def decorator(database_class: Type[BaseDatabase]) -> Type[BaseDatabase]:
        """
        A decorator that registers the database class with the given config model and type.

        Args:
            database_class (Type[BaseDatabase]): The database class to register.

        Returns:
            Type[BaseDatabase]: The registered database class.

        The decorator sets the type attribute of the database class to the given type.
        It also creates a DatabaseEntry and adds it to the DATABASE_REGISTRY with the given type as the key.
        """
        entry = DatabaseEntry()
        entry.config_model = config_model
        entry.database_class = database_class

        DATABASE_REGISTRY[database_type] = entry
        database_class.type = database_type  # type: ignore
        return database_class
    
    return decorator
