from typing import Dict, Type, Generic, TypeVar

from .base import BaseAgent, BaseAgentConfig

C = TypeVar('C', bound=BaseAgentConfig)

class AgentEntry(Generic[C]):

    """
    Represents a registry entry for an agent.

    ``AgentEntry`` stores the mapping between an agent type, its configuration model,
    and the corresponding agent class. This class is used internally by the agent
    registry to manage agent types and their associated metadata.

    Attributes:
        config_model (Type[C]): The Pydantic configuration model class associated with the agent.
        agent_class (Type[BaseAgent]): The agent class corresponding to the agent type.
    """

    config_model: Type[C]
    agent_class: Type[BaseAgent]

AGENT_REGISTRY: Dict[str, AgentEntry] = {}

"""
A global registry that maps agent types (str) to their corresponding ``AgentEntry`` objects.

This registry is used by factories like ``AgentFactory`` to look up and create agent
instances based on their type.
"""

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
        Callable[[Type[BaseAgent]], Type[BaseAgent]]: A decorator that registers the agent class with the given config model and type.
    """
    def decorator(agent_class: Type[BaseAgent]) -> Type[BaseAgent]:
        """
        A decorator that registers the agent class with the given config model and type.

        Args:
            agent_class (Type[BaseAgent]): The agent class to register.

        Returns:
            Type[BaseAgent]: The registered agent class.
        """
        entry = AgentEntry()
        entry.config_model = config_model
        entry.agent_class = agent_class

        AGENT_REGISTRY[agent_type] = entry
        agent_class.type = agent_type  # type: ignore
        return agent_class

    return decorator
