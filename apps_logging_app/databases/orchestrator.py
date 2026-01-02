from ..orchestration.orchestrator import BaseOrchestrator
from .base import BaseDatabase

class DatabaseOrchestrator(BaseOrchestrator):
    """
    Orchestrator class for managing a database connection using the orchestration framework.

    This class integrates a `BaseDatabase` instance into the `BaseOrchestrator` workflow,
    handling connection management and retry logic based on the database configuration.

    Attributes:
        database (BaseDatabase): The database instance being orchestrated. Provides methods
            to connect, check connection status, and close the database.
    """
    def __init__(self, database: BaseDatabase):
        """
        Initializes the DatabaseOrchestrator with a specific database instance.

        Sets up the orchestrator's name and retry configuration based on the provided
        database's configuration, and stores the database instance for later operations.

        Args:
            database (BaseDatabase): The database instance to be orchestrated. Its configuration
                is used to set the orchestrator's name and maximum retry attempts.
        """
        super().__init__(
            name=f"DB-{database.config.type}-{database.config.name}",
            max_retries=database.config.max_retries
        )
        self.database = database

    def _is_connected(self) -> bool:
        """
        Checks whether the orchestrated database is currently connected.

        This internal method wraps the `is_connected` method of the `BaseDatabase` instance
        to integrate with the orchestrator's connection management logic.

        Returns:
            bool: `True` if the database is connected, `False` otherwise.
        """
        return self.database.is_connected()

    def _connect(self) -> None:
        """
        Establishes a connection to the orchestrated database.

        This internal method wraps the `connect` method of the `BaseDatabase` instance,
        allowing the orchestrator to manage database connections consistently
        with its retry and lifecycle logic.

        Raises:
            Exception: Propagates any exception raised by the underlying database's
                `connect` method if the connection attempt fails.
        """
        self.database.connect()

    def close(self) -> None:
        """
        Closes the connection to the orchestrated database.

        This method wraps the `close` method of the `BaseDatabase` instance,
        ensuring that the database connection is properly terminated and resources
        are released when the orchestrator is done using the database.

        Raises:
            Exception: Propagates any exception raised by the underlying database's
                `close` method if the closure fails.
        """
        self.database.close()
