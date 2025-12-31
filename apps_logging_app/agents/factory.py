from .registry import AGENT_REGISTRY

class AgentFactory:
    @staticmethod
    def create(raw_config: dict):
        agent_type = raw_config.get("type")
        entry = AGENT_REGISTRY.get(agent_type)
        if not entry:
            raise ValueError(f"Unknown agent type: {agent_type}")

        config = entry.config_model.model_validate(raw_config)

        from ..producers.factory import ProducerFactory
        from ..databases.factory import DatabaseFactory

        for producer_connection in config.producer_connections:
            ProducerFactory.get_instance(
                producer_connection.type, 
                producer_connection.name
            )

            for data_connection in producer_connection.data_connections:
                if data_connection.destination_ref:
                    DatabaseFactory.get_instance(
                        data_connection.destination_ref.type, 
                        data_connection.destination_ref.name
                    )
        return entry.agent_class(config=config)
