from typing import Dict, Type, Generic, TypeVar

from .base import BaseDatabase, BaseDatabaseConfig

C = TypeVar('C', bound=BaseDatabaseConfig)

class DatabaseEntry(Generic[C]):
    """
    Represents a registry entry for a database type.

    Each DatabaseEntry binds a database class to its corresponding configuration model.
    It is used internally by the :data:`DATABASE_REGISTRY` to map database types to
    their implementation and configuration schema.

    Attributes
    ----------
    config_model : Type[C]
        The Pydantic configuration model class associated with this database.
        This model defines the structure and validation rules for the database configuration.
    database_class : Type[BaseDatabase]
        The actual database class associated with this entry.
        This class must inherit from :class:`BaseDatabase`.

    Notes
    -----
    Instances of this class are typically created automatically by the
    :func:`register_database` decorator and stored in :data:`DATABASE_REGISTRY`.
    You generally do not instantiate this class manually.
    """
    config_model: Type[C]
    database_class: Type[BaseDatabase]

DATABASE_REGISTRY: Dict[str, DatabaseEntry[BaseDatabaseConfig]] = {}


def register_database(
    *,
    database_type: str,
    config_model: Type[BaseDatabaseConfig],
):
    """
    A decorator that registers a database class with the given config model and type.

    The decorator sets the type attribute of the database class to the given type.
    It also creates a DatabaseEntry and adds it to the DATABASE_REGISTRY with the given type as the key.

    Args:
        database_class (Type[BaseDatabase]): The database class to register.

    Returns:
        Type[BaseDatabase]: The registered database class.
    """
    def decorator(database_class: Type[BaseDatabase]) -> Type[BaseDatabase]:
        """
        A decorator that registers a database class with the given config model and type.

        Args:
            database_class (Type[BaseDatabase]): The database class to register.

        Returns:
            Type[BaseDatabase]: The registered database class.

        The decorator sets the type attribute of the database class to the given type.
        It also creates a DatabaseEntry and adds it to the DATABASE_REGISTRY with the given type as the key.
        """
        assert issubclass(database_class, BaseDatabase)
        entry = DatabaseEntry()
        entry.config_model = config_model
        entry.database_class = database_class

        DATABASE_REGISTRY[database_type] = entry
        database_class.type = database_type  # type: ignore
        return database_class
    
    return decorator
