from .registry import AGENT_REGISTRY

class AgentFactory:

    """
    Factory class responsible for creating agent instances from configuration
    dictionaries.

    ``AgentFactory`` is a stateless factory that instantiates agent objects
    based on a raw configuration dictionary. It validates the configuration
    against the agent's registered configuration model, initializes all
    required shared resources (producers and databases), and returns a fully
    configured agent instance.

    This factory does **not** cache agent instances: each call to
    :meth:`create` returns a new agent object. Shared resources such as
    producers and databases are retrieved through their respective factories
    and are internally cached.

    The factory relies on:

    - ``AGENT_REGISTRY``: a registry mapping agent types to their corresponding
      configuration model and agent class.
    - ``ProducerFactory``: used to initialize producer connections required
      by the agent.
    - ``DatabaseFactory``: used to initialize database connections referenced
      by producer data connections.

    **Responsibilities:**

    - Validate the agent type from the configuration
    - Validate the configuration using the registered config model
    - Initialize all required producer instances
    - Initialize all required database instances
    - Instantiate and return a new agent instance

    .. note::

        ``AgentFactory`` is intentionally stateless and thread-safe, as it does
        not maintain any internal cache. Resource lifecycle management is
        delegated to the corresponding resource factories.

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
