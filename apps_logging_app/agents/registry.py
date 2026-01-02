from typing import Dict, Type, Generic, TypeVar

from .base import BaseAgent, BaseAgentConfig

C = TypeVar('C', bound=BaseAgentConfig)

class AgentEntry(Generic[C]):
    """
    Generic container that pairs an agent class with its configuration model.

    This class is used internally by the agent registry to store the
    relationship between a specific agent type and its corresponding
    configuration model.

    Type Parameters:
        C (BaseAgentConfig): The type of the configuration model associated with the agent.

    Attributes:
        config_model (Type[C]): The Pydantic configuration model class for the agent.
        agent_class (Type[BaseAgent]): The agent class itself.
    """
    config_model: Type[C]
    agent_class: Type[BaseAgent]

AGENT_REGISTRY: Dict[str, AgentEntry[BaseAgentConfig]] = {}
"""
Central registry mapping agent type strings to their corresponding AgentEntry objects.

This dictionary is used to look up agent classes and their configuration models
by agent type. It is populated using the `register_agent` decorator and is
typically used by factories (e.g., `AgentFactory`) to dynamically create
agent instances.

Keys:
    str: The unique agent type string.

Values:
    AgentEntry[BaseAgentConfig]: An entry containing the agent class and its
        associated configuration model.
"""

def register_agent(
    *,
    agent_type: str,
    config_model: Type[BaseAgentConfig],
):
    """
    Decorator factory to register an agent class with its configuration model.

    This function returns a decorator that, when applied to a subclass of `BaseAgent`,
    performs the following:
        1. Validates that the decorated class is a subclass of `BaseAgent`.
        2. Creates an `AgentEntry` linking the agent class with the provided config model.
        3. Adds the entry to the `AGENT_REGISTRY` under the given `agent_type`.
        4. Sets the `type` attribute on the agent class to the specified `agent_type`.

    Args:
        agent_type (str): Unique type string to identify the agent in the registry.
        config_model (Type[BaseAgentConfig]): The Pydantic configuration model
            class associated with this agent.

    Returns:
        Callable[[Type[BaseAgent]], Type[BaseAgent]]: A decorator that registers the agent class.

    Example:
        >>> @register_agent(agent_type="my_agent", config_model=MyAgentConfig)
        >>> class MyAgent(BaseAgent):
        >>>     ...
        >>> assert "my_agent" in AGENT_REGISTRY
    """
    def decorator(agent_class: Type[BaseAgent]) -> Type[BaseAgent]:
        """
        Decorator that registers an agent class in the AGENT_REGISTRY.

        This function is returned by `register_agent` and is applied to a subclass
        of `BaseAgent`. It performs the following steps:
            1. Verifies that `agent_class` is a subclass of BaseAgent.
            2. Creates an `AgentEntry` linking the class with its configuration model.
            3. Adds the entry to the global `AGENT_REGISTRY` under the specified `agent_type`.
            4. Sets the `type` attribute on the agent class to the specified `agent_type`.
            5. Returns the original agent class.

        Args:
            agent_class (Type[BaseAgent]): The agent class to be registered.

        Returns:
            Type[BaseAgent]: The same agent class that was passed in, after registration.

        Raises:
            AssertionError: If `agent_class` is not a subclass of BaseAgent.

        Example:
            >>> @register_agent(agent_type="my_agent", config_model=MyAgentConfig)
            >>> class MyAgent(BaseAgent):
            >>>     ...
            >>> # MyAgent is now registered and can be instantiated via AGENT_REGISTRY
        """
        assert issubclass(agent_class, BaseAgent)
        entry = AgentEntry()
        entry.config_model = config_model
        entry.agent_class = agent_class
        AGENT_REGISTRY[agent_type] = entry
        agent_class.type = agent_type
        return agent_class

    return decorator
