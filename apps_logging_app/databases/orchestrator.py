from ..orchestration.orchestrator import BaseOrchestrator
from .base import BaseDatabase

class DatabaseOrchestrator(BaseOrchestrator):
    """
    Orchestrator for managing the lifecycle and connectivity of a database instance.

    The :class:`DatabaseOrchestrator` wraps a :class:`BaseDatabase` instance
    and provides thread-safe connection management, retry handling, and
    orchestration of database operations. It ensures that queries are only
    executed when the database is connected and handles reconnect attempts
    transparently.

    Attributes
    ----------
    database : BaseDatabase
        The database instance managed by this orchestrator.

    Parameters
    ----------
    database : BaseDatabase
        The database instance to manage.

    Notes
    -----
    This orchestrator is intended to be used internally by the
    :class:`DatabaseFactory` and the :class:`BaseDatabase` dispatcher.
    """
    def __init__(self, database: BaseDatabase):
        """
        Initializes a DatabaseOrchestrator instance.

        Parameters
        ----------
        database : BaseDatabase
            The database instance to manage.

        """
        super().__init__(
            name=f"DB-{database.config.type}-{database.config.name}",
            max_retries=database.config.max_retries
        )
        self.database = database

    def _connect(self) -> None:
        """
        Establishes a connection to the database. This method is thread-safe and idempotent.

        It will only attempt to connect to the database if the orchestrator is not already
        connected. If the connection attempt fails, it will retry according to the
        configured maximum number of retries.

        :return: None
        """
        self.database.connect()

    def close(self) -> None:
        """
        Closes the database connection and marks the orchestrator as disconnected.

        This method is thread-safe and idempotent.
        """
        self.database.close()
