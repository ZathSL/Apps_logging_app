from typing import Dict, Any
from ..base import BaseAgent
from ..registry import register_agent
from .config import SpringAgentConfig
from ..data import WorkingDataConnection

@register_agent(
    agent_type="spring",
    config_model=SpringAgentConfig,
)
class SpringAgent(BaseAgent):
    """
    Agent implementation for the *spring* agent type.

    The :class:`SpringAgent` extends :class:`BaseAgent` and provides
    specific logic for handling Spring-based workflows. It is registered
    in the agent registry with the ``spring`` agent type and uses
    :class:`SpringAgentConfig` as its configuration model.

    This agent is responsible for applying transformations and filtering
    logic to instances of :class:`WorkingDataConnection` before they are
    used in downstream processing.

    :param config: Configuration object used to initialize the agent.
    :type config: SpringAgentConfig
    """
    def __init__(self, config: SpringAgentConfig) -> None:
        """
        Initializes the SpringAgent with the given config.

        :param config: The config for the agent.
        :type config: SpringAgentConfig
        """
        super().__init__(config)

    def _data_connections_transformation_and_filtering(self, working_data_connection: WorkingDataConnection) -> Dict[str, Any]:
        """
        Applies transformations and filtering to the given working data connection.

        :param working_data_connection: The working data connection to apply transformations and filtering to.
        :type working_data_connection: WorkingDataConnection

        :return: The transformed and filtered working data connection.
        :rtype: Dict[str, Any]
        """
        super()._data_connections_transformation_and_filtering(working_data_connections=working_data_connection)
        return working_data_connection
        