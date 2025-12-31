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
    def decorator(database_class: Type[BaseDatabase]) -> Type[BaseDatabase]:
        entry = DatabaseEntry()
        entry.config_model = config_model
        entry.database_class = database_class

        DATABASE_REGISTRY[database_type] = entry
        database_class.type = database_type  # type: ignore
        return database_class
    
    return decorator
