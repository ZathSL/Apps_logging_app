from typing import Dict, Any
from ..base import BaseAgent
from ..registry import register_agent
from .config import SasdmAgentConfig
from ..data import WorkingDataConnection
import json

@register_agent(
    agent_type="sasdm",
    config_model=SasdmAgentConfig,
)
class SasdmAgent(BaseAgent):
 
    def __init__(self, config: SasdmAgentConfig):
        super().__init__(config)

    def _data_connections_transformation_and_filtering(self, working_data_connection: WorkingDataConnection) -> Dict[str, Any]:
        new_dict = {}
        if working_data_connection.name == 'info_pattern':
            working_data_connection.data_dict_match = json.loads(working_data_connection.data_dict_match['response_json'])
            keys_to_extract = ["externalCode", "status", "httpStatus"]
            new_dict = {k: working_data_connection.data_dict_match[k] for k in keys_to_extract}
        elif working_data_connection.name == 'info_pattern_engagement_all_messages':
            working_data_connection.data_dict_match = json.loads(working_data_connection.data_dict_match['response_json'])
            keys_to_extract = ["externalCode", "status", "httpStatus"]
            new_dict = {k: working_data_connection.data_dict_match[k] for k in keys_to_extract}
        return new_dict
        