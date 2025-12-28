from typing import List
from ..base import BaseAgent
from ..registry import register_agent
from .config import SpringAgentConfig
from ..data import WorkingDataConnection

@register_agent(
    agent_type="spring",
    config_model=SpringAgentConfig,
)
class SpringAgent(BaseAgent):
    def __init__(self, config: SpringAgentConfig):
        super().__init__(config)

    def _data_connections_transformation_and_filtering(self, working_data_connections: List[WorkingDataConnection]) -> List[WorkingDataConnection]:
        super()._data_connections_transformation_and_filtering(working_data_connections=working_data_connections)
        return working_data_connections
        