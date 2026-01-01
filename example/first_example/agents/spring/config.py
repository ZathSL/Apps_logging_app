from ..base import BaseAgentConfig

class SpringAgentConfig(BaseAgentConfig):
    """
    Configuration model for the :class:`SpringAgent`.

    This class extends :class:`BaseAgentConfig` without introducing
    additional configuration fields. It exists to provide a dedicated
    configuration type for the Spring agent, allowing future extensions
    and clearer type separation between different agent implementations.

    At present, all configuration options are inherited directly from
    :class:`BaseAgentConfig`.
    """
    pass
