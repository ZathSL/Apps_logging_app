from .registry import AGENT_REGISTRY

class AgentFactory:
    """
    Factory class responsible for creating agent instances from configuration dictionaries.

    This class provides a static method to instantiate agents based on a given
    configuration. It ensures that the agent type is registered, validates the
    configuration using the agent's config model, and verifies that all required
    producer and database connections can be created before returning the agent instance.

    The factory performs the following steps:
        1. Retrieves the agent type from the AGENT_REGISTRY.
        2. Validates the raw configuration using the agent's config model.
        3. Iterates through producer connections and ensures each producer instance
           can be created.
        4. For each producer, iterates through its data connections and ensures
           referenced database instances can be created.
        5. Returns an initialized agent instance with the validated configuration.

    Raises:
        ValueError: If the agent type is unknown, or if a producer or database
                    connection cannot be created.
    """
    @staticmethod
    def create(raw_config: dict):
        """
        Creates and returns an agent instance based on the provided configuration.

        This method performs the following steps:
            1. Retrieves the agent type from the configuration dictionary.
            2. Looks up the agent class and configuration model in the AGENT_REGISTRY.
            3. Validates the raw configuration using the agent's config model.
            4. Iterates through all producer connections defined in the configuration:
                - Ensures each producer instance can be created using ProducerFactory.
                - Iterates through each producer's data connections:
                    - If a data connection has a destination reference, ensures the
                    corresponding database instance can be created using DatabaseFactory.
            5. Returns an instance of the agent initialized with the validated configuration.

        Args:
            raw_config (dict): Dictionary containing the agent configuration. Must
                include a "type" key corresponding to a registered agent.

        Returns:
            object: An instance of the agent class specified in the configuration.

        Raises:
            ValueError: If the agent type is unknown.
            ValueError: If a producer instance cannot be created.
            ValueError: If a database instance cannot be created for a destination reference.
        """
        agent_type = raw_config.get("type")
        entry = AGENT_REGISTRY.get(agent_type)
        if not entry:
            raise ValueError(f"Unknown agent type: {agent_type}")

        config = entry.config_model.model_validate(raw_config)

        from ..producers.factory import ProducerFactory
        from ..databases.factory import DatabaseFactory

        for producer_connection in config.producer_connections:
            try:
                ProducerFactory.get_instance(
                    producer_connection.type, 
                    producer_connection.name,
                    producer_connection.topic
                )
            except Exception as e:
                raise ValueError(f"Error creating producer {producer_connection.type}-{producer_connection.name}: {e}")

            for data_connection in producer_connection.data_connections:
                if data_connection.destination_ref:
                    try:
                        DatabaseFactory.get_instance(
                            data_connection.destination_ref.type, 
                            data_connection.destination_ref.name
                        )
                    except Exception as e:
                        raise ValueError(f"Error creating database {data_connection.destination_ref.type}-{data_connection.destination_ref.name}: {e}")

        return entry.agent_class(config=config)
