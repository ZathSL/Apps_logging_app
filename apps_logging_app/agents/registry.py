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

    def decorator(agent_class: Type[BaseAgent]) -> Type[BaseAgent]:
        entry = AgentEntry()
        entry.config_model = config_model
        entry.agent_class = agent_class

        AGENT_REGISTRY[agent_type] = entry
        agent_class.type = agent_type  # type: ignore
        return agent_class

    return decorator
