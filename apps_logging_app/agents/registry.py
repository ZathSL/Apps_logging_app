from typing import Dict, Type, Generic, TypeVar

from .base import BaseAgent, BaseAgentConfig

C = TypeVar('C', bound=BaseAgentConfig)

class AgentEntry(Generic[C]):
    config_model: Type[C]
    agent_class: Type[BaseAgent]

AGENT_REGISTRY: Dict[str, AgentEntry] = {}

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
