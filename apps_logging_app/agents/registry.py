from typing import Dict, Type, Generic, TypeVar

from .base import BaseAgent, BaseAgentConfig

C = TypeVar('C', bound=BaseAgentConfig)

class AgentEntry(Generic[C]):
    """
    Registry entry describing a registered agent type.

    The :class:`AgentEntry` class represents a single entry in the
    global :data:`AGENT_REGISTRY`. It associates an agent type with:

    - the agent implementation class
    - the corresponding configuration model

    This indirection allows agents to be dynamically instantiated
    from raw configuration data, while preserving strong typing
    between agent classes and their configuration models.

    Instances of this class are created internally by the
    :func:`register_agent` decorator and should not be instantiated
    directly by users.

    :param config_model: Pydantic model used to validate the agent configuration.
    :type config_model: Type[BaseAgentConfig]
    :param agent_class: Concrete agent class associated with the entry.
    :type agent_class: Type[BaseAgent]
    """
    config_model: Type[C]
    agent_class: Type[BaseAgent]

AGENT_REGISTRY: Dict[str, AgentEntry[BaseAgentConfig]] = {}


def register_agent(
    *,
    agent_type: str,
    config_model: Type[BaseAgentConfig],
):

    """
    Registers an agent class with the given config model and type.

    Args:
        agent_type (str): The type of the agent.
        config_model (Type[BaseAgentConfig]): The config model for the agent.

    Returns:
        Type[BaseAgent]: The registered agent class.
    """
    def decorator(agent_class: Type[BaseAgent]) -> Type[BaseAgent]:
        """
        A decorator that registers the agent class with the given config model and type.

        Args:
            agent_class (Type[BaseAgent]): The agent class to register.

        Returns:
            Type[BaseAgent]: The registered agent class.
        """
        assert issubclass(agent_class, BaseAgent)
        entry = AgentEntry()
        entry.config_model = config_model
        entry.agent_class = agent_class
        AGENT_REGISTRY[agent_type] = entry
        agent_class.type = agent_type
        return agent_class

    return decorator
