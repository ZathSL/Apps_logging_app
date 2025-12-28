import yaml
from pathlib import Path
from .registry import DATABASE_REGISTRY

# Global cache for shared database instances
DATABASE_INSTANCES = {}

class DatabaseFactory:
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
