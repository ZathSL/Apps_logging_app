import yaml
from pathlib import Path
import logging
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler
from .agents.factory import AgentFactory
from .agents.sasdm.agent import SasdmAgent                         # Import required to register agent, database and producer classes
from .agents.spring.agent import SpringAgent                       # Import required to register agent, database and producer classes
from .databases.oracle.database import OracleDatabase              # Import required to register agent, database and producer classes
from .producers.kafka_handler.producer import KafkaHandlerProducer # Import required to register agent, database and producer classes

def main():
    """
    Entry point of the application.

    This function performs the following tasks:

    1. **Loads the base configuration**  
       Reads `configs/base.yaml` to retrieve general application settings such as
       application name, version, log directory, and log level.

    2. **Sets up logging**  
       Configures a `TimedRotatingFileHandler` to store logs in the specified directory,
       rotating logs daily at midnight and keeping the last 7 backups.  
       Log messages include the application name, timestamp, logger name, log level, and message.

    3. **Loads agents configuration**  
       Reads `configs/agents.yaml` to get the list of agents to run. Each agent configuration
       specifies the agent type and name, along with type-specific configuration.

    4. **Instantiates and starts agents**  
       Iterates over all agent configurations, creating agent instances via `AgentFactory.create()`,
       stores them in a `RUNNING_AGENTS` dictionary keyed by `(agent_type, agent_name)`, and calls
       `start()` on each agent. Logs success or failure for each agent creation.

    5. **Keeps the application running**  
       Enters a blocking loop that sleeps in 60-second intervals. This keeps the application alive
       while agents perform their asynchronous work (e.g., processing messages or database queries).

    6. **Handles graceful shutdown**  
       Listens for `KeyboardInterrupt` (Ctrl+C) to stop all running agents by calling their `stop()`
       methods. Logs any errors encountered during agent shutdown.

    Notes
    -----
    - This function ensures that all necessary classes (agents, databases, producers) are imported
      so they are automatically registered in their respective registries.
    - The function is designed to be the main executable entry point for the application.

    Example
    -------
    To run the application:

    >>> python -m your_package_name.main
    """
    base_config_path = Path(__file__).parent / 'configs' / 'base.yaml'
    with open(base_config_path, 'r') as f:
        base_config = yaml.safe_load(f)
    
    log_dir_path = Path(__file__).parent / base_config['app']['log_dir']
    log_dir_path.mkdir(parents=True, exist_ok=True)
    log_file_path = log_dir_path / 'application.log'

    log_level = getattr(logging, base_config['app']['log_level'].upper(), logging.INFO)

    main_logger = logging.getLogger(__name__)
    main_logger.setLevel(log_level)
    
    handler = TimedRotatingFileHandler(str(log_file_path), when='midnight', interval=1, backupCount=7)
    app_name = base_config['app']['name']
    formatter = Formatter(fmt=f'<{app_name} - %(asctime)s - %(name)s - %(levelname)s - %(message)s>', datefmt='<%Y-%m-%d %H:%M:%S>')
    handler.setFormatter(formatter)
    
    main_logger.addHandler(handler)
    main_logger.info(f"Starting application: {base_config['app']['name']} v{base_config['app']['version']}")

    agents_config_path = Path(__file__).parent / 'configs' / 'agents.yaml'
    with open(agents_config_path, 'r') as f:
        agents_config = yaml.safe_load(f)

    RUNNING_AGENTS = {}
    
    for agent_raw_config in agents_config['agents']:
        try:
            agent_type = agent_raw_config['type']
            agent_name = agent_raw_config['name']
            main_logger.info(f"Creating agent: {agent_name} (type: {agent_type})")
            agent = AgentFactory.create(agent_raw_config)
            RUNNING_AGENTS[(agent_type, agent_name)] = agent
            agent.start()
            main_logger.info(f"Agent {agent_name} created successfully")
        except Exception as e:
            main_logger.error(f"Error creating agent {agent_name}: {e}")

    import time
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        main_logger.warning(f"Shutdown requested")
        for agent in RUNNING_AGENTS.values():
            try:
                agent.stop()
            except Exception as e:
                main_logger.error(f"Error stopping agent {agent.name}: {e}")

if __name__ == "__main__":
    main()