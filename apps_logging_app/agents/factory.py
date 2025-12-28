from .registry import AGENT_REGISTRY


class AgentFactory:

    """
    Factory class responsible for creating agent instances from configuration dictionaries.

    ``AgentFactory`` provides a static method ``create`` that instantiates an agent
    based on a given raw configuration dictionary. It validates the configuration
    against the agent's expected config model, initializes required producers and
    databases, and returns a fully configured agent instance.

    **Responsibilities:**

    - Validate the agent type from the configuration
    - Instantiate the correct agent class
    - Initialize associated producer connections
    - Initialize associated database connections for data connections

    **Methods:**

    .. method:: create(raw_config: dict) -> BaseAgent

        Create an agent from a given configuration dictionary.

        :param raw_config: A dictionary containing the configuration for the agent.
        :type raw_config: dict
        :return: An instance of the agent class configured according to ``raw_config``.
        :rtype: BaseAgent
        :raises ValueError: If the agent type is unknown or not registered in ``AGENT_REGISTRY``.
        
        **Behavior:**
        
        1. Retrieves the agent type from ``raw_config``.
        2. Looks up the agent type in ``AGENT_REGISTRY``.
        3. Validates the configuration using the agent's config model.
        4. Creates all producers associated with the agent's ``producer_connections``.
        5. Creates all databases referenced by data connections that have a ``destination_ref``.
        6. Instantiates and returns the configured agent.
    """

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