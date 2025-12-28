import yaml
from pathlib import Path
import logging
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler
from .agents.factory import AgentFactory
from .agents.sasdm.agent import SasdmAgent
from .agents.spring.agent import SpringAgent
from .databases.oracle.database import OracleDatabase
from .producers.kafka_handler.producer import KafkaHandlerProducer

def main():
    """
    Main entry point for the application.

    This function loads the configuration from base.yaml and agents.yaml,
    sets up the logging system, creates instances of the agents using
    the factory, and then enters an infinite loop to maintain the
    application in execution.

    The application can be stopped by sending a SIGINT signal (e.g.
    using Ctrl+C in the terminal).
    """
    base_config_path = Path(__file__).parent / 'configs' / 'base.yaml'
    with open(base_config_path, 'r') as f:
        base_config = yaml.safe_load(f)
    
    log_dir_path = Path(base_config['app']['log_dir'])
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

    AGENT_INSTANCES = {}
    
    for agent_raw_config in agents_config['agents']:
        try:
            agent_type = agent_raw_config['type']
            agent_name = agent_raw_config['name']
            main_logger.info(f"Creating agent: {agent_name} (type: {agent_type})")
            agent = AgentFactory.create(agent_raw_config)
            AGENT_INSTANCES[(agent_type, agent_name)] = agent
            main_logger.info(f"Agent {agent_name} created successfully")
        except Exception as e:
            main_logger.error(f"Error creating agent {agent_name}: {e}")


    import time
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        main_logger.warning(f"Shutdown requested")
        for agent in AGENT_INSTANCES.values():
            agent.stop()


if __name__ == "__main__":
    main()