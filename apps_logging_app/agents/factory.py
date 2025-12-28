from .registry import AGENT_REGISTRY


class AgentFactory:
    @staticmethod
    def create(raw_config: dict):
        """
        Create an agent from a given configuration.

        Args:
            raw_config: A dictionary containing the configuration for the agent.

        Returns:
            An instance of the agent class with the given configuration.

        Raises:
            ValueError: If the agent type is unknown.
        """
        agent_type = raw_config.get("type")
        entry = AGENT_REGISTRY.get(agent_type)
        if not entry:
            raise ValueError(f"Unknown agent type: {agent_type}")

        raw_config = entry.config_model.model_validate(raw_config)

        producer_connections = raw_config.producer_connections

        from producers.factory import ProducerFactory
        from databases.factory import DatabaseFactory
        for producer_connection in producer_connections:
            ProducerFactory.create(producer_connection.type, producer_connection.name)
            import time
            for data_connection in producer_connection.data_connections:
                if data_connection.destination_ref: 
                    DatabaseFactory.create(data_connection.destination_ref.type, data_connection.destination_ref.name)
        return entry.agent_class(config=raw_config)