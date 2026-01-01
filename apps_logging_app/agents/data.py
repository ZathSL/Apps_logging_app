from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from concurrent.futures import Future
from enum import Enum
from .model import DataConnectionConfig

class WorkingDataStatus(Enum):
    """
    Enumeration representing the lifecycle status of a
    :class:`WorkingDataConnection`.

    This enum is used to track the current state of a working data
    connection as it moves through the agent processing pipeline.

    Possible states are:

    - ``READY``: The data connection is ready to be processed.
    - ``QUERY_RUNNING``: A database query is currently being executed.
    - ``UPDATED``: New data is available and ready to be sent to producers.
    - ``EXPIRED``: The data connection has expired and will be removed.
    """
    READY = "ready"
    QUERY_RUNNING = "query_running"
    UPDATED = "updated"
    EXPIRED = "expired"

class WorkingDataConnection:
    """
    Runtime representation of a data connection during agent execution.

    The :class:`WorkingDataConnection` is a mutable, working-state object
    created from a :class:`DataConnectionConfig`. It holds intermediate and
    final data extracted from log files, query execution results, and
    delivery status toward producers.

    Instances of this class are managed by :class:`BaseAgent` and move
    through different states defined by :class:`WorkingDataStatus`.

    A working data connection may:
    - Be populated by regex matches from log lines
    - Generate and execute database queries
    - Store query results
    - Be sent to one or more producers
    - Expire after a configured time window

    :param name: Unique identifier for the data connection.
    :type name: str
    :param producer_type: Type of the associated producer.
    :type producer_type: str
    :param producer_name: Name of the associated producer.
    :type producer_name: str
    :param database_type: Type of the destination database, if any.
    :type database_type: Optional[str]
    :param database_name: Name of the destination database, if any.
    :type database_name: Optional[str]
    :param query: Query associated with this data connection, if any.
    :type query: Optional[str]
    :param is_error: Whether this data connection represents an error condition.
    :type is_error: bool
    :param expired_time: Expiration timestamp for this data connection.
    :type expired_time: Optional[datetime]
    """
    def __init__(self, name: str,
                        producer_type: str, 
                        producer_name: str,
                        database_type: Optional[str] = None,
                        database_name: Optional[str] = None,
                        query: Optional[str] = None,
                        is_error: bool = False,
                        expired_time: datetime = None) -> None:
        """
        Initializes a WorkingDataConnection object.

        Args:
            name (str): Unique identifier for this data connection.
            producer_type (str): Type of the producer this data connection belongs to.
            producer_name (str): Name of the producer this data connection belongs to.
            database_type (Optional[str], optional): Type of the database this data connection belongs to. Defaults to None.
            database_name (Optional[str], optional): Name of the database this data connection belongs to. Defaults to None.
            query (Optional[str], optional): Query associated with this data connection. Defaults to None.
            is_error (bool, optional): Whether this data connection is associated with an error. Defaults to False.
            expired_time (datetime, optional): Expiration timestamp of the data connection. Defaults to None.

        Attributes:
            name (str): Unique identifier for this data connection.
            producer_type (str): Type of the producer this data connection belongs to.
            producer_name (str): Name of the producer this data connection belongs to.
            database_type (str, optional): Type of the database this data connection belongs to. Defaults to None.
            database_name (str, optional): Name of the database this data connection belongs to. Defaults to None.
            query (str, optional): Query associated with this data connection. Defaults to None.
            is_error (bool, optional): Whether this data connection is associated with an error. Defaults to False.
            expired_time (datetime, optional): Expiration timestamp of the data connection. Defaults to None.
            data_dict_match (Optional[Dict[str, Any]], optional): Data extracted by regex matches. Defaults to None.
            data_dict_query_source (Optional[Dict[str, Any]], optional): Data used to generate queries. Defaults to None.
            data_dict_result (Optional[List[Dict[str, Any]]], optional): Resulting data from executed queries. Defaults to None.
            list_data_dict_query_result (Optional[List[Dict[str, Any]]], optional): List of resulting data from executed queries. Defaults to None.
        """
        # static
        self.name: str = name
        self.producer_type: str = producer_type
        self.producer_name: str = producer_name
        self.database_type: str  = database_type
        self.database_name: str  = database_name
        self.query: str = query
        self.is_error: bool = is_error

        # working data
        self.status: WorkingDataStatus = WorkingDataStatus.READY
        self.expired_time: Optional[datetime] = expired_time
        self.data_dict_match: Optional[Dict[str, Any]] = None
        self.data_dict_query_source: Optional[Dict[str, Any]] = None
        self.data_dict_result: Optional[List[Dict[str, Any]]] = None
        self.list_data_dict_query_result: Optional[List[Dict[str, Any]]] = None
    
    @classmethod
    def from_config(cls, producer_type: str, producer_name: str, cfg: DataConnectionConfig) -> "WorkingDataConnection":
        """
        Creates a WorkingDataConnection object from a DataConnectionConfig object.

        Args:
            producer_type (str): Type of the producer this data connection belongs to.
            producer_name (str): Name of the producer this data connection belongs to.
            cfg (DataConnectionConfig): DataConnectionConfig object to create the WorkingDataConnection from.

        Returns:
            WorkingDataConnection: A WorkingDataConnection object created from the given DataConnectionConfig object.
        """
        expired_time = (
            datetime.now() + timedelta(minutes=cfg.expired_time_int)
            if cfg.expired_time_int is not None
            else None
        )

        return cls(
            name=cfg.name,
            producer_type=producer_type,
            producer_name=producer_name,
            database_type=cfg.destination_ref.type if cfg.destination_ref else None,
            database_name=cfg.destination_ref.name if cfg.destination_ref else None,
            query=cfg.destination_ref.query if cfg.destination_ref else None,
            is_error=cfg.is_error,
            expired_time=expired_time,
        )

    def update_expired_time(self, minutes: int) -> None:
        """
        Updates the expired time for this data connection by adding the given number of minutes to the current time.

        Args:
            minutes (int): The number of minutes to add to the current time.
        """
        self.expired_time = datetime.now() + timedelta(minutes=minutes)

    def set_ready_status(self) -> None:
        """
        Sets the status of this WorkingDataConnection to READY if it is not expired.

        It is used to mark the data connection as ready for further processing after a query execution has finished.

        :return: None
        """
        if self.status != WorkingDataStatus.EXPIRED:
            self.status = WorkingDataStatus.READY

    def on_query_done(self, fut: Future[List[Dict[str, Any]]]) -> None:
        """
        Called when a query has finished executing.

        If the query result is not equal to the current list of results, it updates the list of results and sets the status to UPDATED.
        If the query result is equal to the current list of results, it sets the status to READY.
        If an exception occurs during the query execution, it sets the status to EXPIRED.
        """
        try:
            result = fut.result()
            if result and self.list_data_dict_query_result != result:
                self.list_data_dict_query_result = result
                self.status = WorkingDataStatus.UPDATED
            elif result and self.list_data_dict_query_result == result:
                self.set_ready_status()
        except Exception:
            self.update_expired_time(0)


    def check_expired_time(self) -> None:
        """
        Checks if the current time is greater than the expired time of this data connection.

        If it is, it sets the status of this data connection to EXPIRED. If not, it does nothing.

        :return: None
        """
        if self.expired_time is None:
            return
        if datetime.now() > self.expired_time:
            self.status = WorkingDataStatus.EXPIRED

