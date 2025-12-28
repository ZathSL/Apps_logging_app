from typing import List
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
        """
        Initializes the SasdmAgent with the given config.

        :param config: The config for the agent.
        :type config: SasdmAgentConfig
        """
        super().__init__(config)

    def _data_connections_transformation_and_filtering(self, working_data_connections: List[WorkingDataConnection]) -> List[WorkingDataConnection]:
        """
        Transforms and filters the given working data connections.

        It iterates over the given working data connections and for each one, it checks if the
        name of the working data connection matches 'info_pattern' or
        'info_pattern_engagement_all_messages'. If it does, it extracts the given keys from the
        JSON response and sets the data_dict_result attribute of the working data connection
        to the extracted values.

        :param working_data_connections: The list of working data connections to transform and filter.
        :return: The list of transformed and filtered working data connections.
        """
        for working_data_connection in working_data_connections:
            if working_data_connection.name == 'info_pattern':
                working_data_connection.data_dict_match = json.loads(working_data_connection.data_dict_match['response_json'])
                keys_to_extract = ["externalCode", "status", "httpStatus"]
                new_dict = {k: working_data_connection.data_dict_match[k] for k in keys_to_extract}
                working_data_connection.data_dict_result = new_dict
            elif working_data_connection.name == 'info_pattern_engagement_all_messages':
                working_data_connection.data_dict_match = json.loads(working_data_connection.data_dict_match['response_json'])
                keys_to_extract = ["externalCode", "status", "httpStatus"]
                new_dict = {k: working_data_connection.data_dict_match[k] for k in keys_to_extract}
                #working_data_connection.data_dict_query_source = new_dict
        return working_data_connections
        