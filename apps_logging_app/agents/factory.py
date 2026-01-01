from .registry import AGENT_REGISTRY

class AgentFactory:
    """
    Factory class responsible for creating agent instances.

    The :class:`AgentFactory` instantiates agents based on a raw configuration
    dictionary. It resolves the agent type using the global
    :data:`AGENT_REGISTRY`, validates the configuration against the
    corresponding config model, and ensures that all required producers
    and databases are initialized before returning the agent instance.

    This class centralizes agent creation logic and guarantees that
    dependencies are available at startup time.
    """
    @staticmethod
    def create(raw_config: dict):
        """
        Create an agent from a given configuration.

        Args:
            raw_config (dict): A dictionary containing the configuration for the agent.

        Returns:
            An instance of the agent class with the given configuration.

        Raises:
            ValueError: If the agent type is unknown.
        """
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
