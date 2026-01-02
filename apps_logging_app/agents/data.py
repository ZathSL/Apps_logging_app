from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from concurrent.futures import Future
from enum import Enum
from .model import DataConnectionConfig

class WorkingDataStatus(Enum):
    """Enumeration of possible statuses for a WorkingDataConnection.

    This enum is used to track the current state of a working data connection,
    which can change based on query execution, data updates, and expiration.

    Attributes:
        READY: The connection is ready for use.
        QUERY_RUNNING: A query is currently running on the connection.
        UPDATED: The data has been updated successfully after a query.
        EXPIRED: The connection has expired and may need to be refreshed.
    """
    READY = "ready"
    QUERY_RUNNING = "query_running"
    UPDATED = "updated"
    EXPIRED = "expired"

class WorkingDataConnection:
    """Represents a working data connection with state, query handling, and expiration management.

    This class encapsulates both static metadata and dynamic working data for a data connection.
    It tracks the connection's status, handles query results asynchronously, and manages expiration times.

    Attributes:
        name (str): Unique name of the connection.
        producer_type (str): Type of the data producer (e.g., service or system).
        producer_name (str): Name of the data producer.
        topic (Optional[str]): Topic associated with the connection.
        database_type (Optional[str]): Type of the database (if applicable).
        database_name (Optional[str]): Name of the database (if applicable).
        query (Optional[str]): Query string used to fetch data (if applicable).
        is_error (bool): Indicates whether the connection has an error.
        is_warning (bool): Indicates whether the connection has a warning.
        status (WorkingDataStatus): Current status of the connection.
        expired_time (Optional[datetime]): Timestamp when the connection expires.
        data_dict_match (Optional[Dict[str, Any]]): Optional cached data match.
        data_dict_query_source (Optional[Dict[str, Any]]): Optional source data from the query.
        data_dict_result (Optional[List[Dict[str, Any]]]): Optional result of a query.
        list_data_dict_query_result (Optional[List[Dict[str, Any]]]): List of query results, updated on query completion.

    Methods:
        from_config(producer_type, producer_name, topic, cfg):
            Creates a WorkingDataConnection from a DataConnectionConfig.
        update_expired_time(minutes):
            Updates the expiration time by adding the specified number of minutes.
        set_query_running_status():
            Sets the connection status to QUERY_RUNNING if not expired or updated.
        set_ready_status():
            Sets the connection status to READY if not expired.
        on_query_done(fut):
            Handles completion of a query future and updates status and results.
        check_expired_time():
            Checks if the connection has expired and updates status accordingly.
    """

    def __init__(self, name: str,
                        producer_type: str, 
                        producer_name: str,
                        topic: str = None,
                        database_type: Optional[str] = None,
                        database_name: Optional[str] = None,
                        query: Optional[str] = None,
                        is_error: bool = False,
                        is_warning: bool = False,
                        expired_time: datetime = None) -> None:
        """
        Initializes a WorkingDataConnection instance with metadata and optional working data.

        This constructor sets both static attributes (metadata about the connection) and
        working attributes (status, expiration, and data placeholders). By default,
        the status is set to READY and working data fields are initialized as None.

        Args:
            name (str): Unique name of the connection.
            producer_type (str): Type of the data producer (e.g., service or system).
            producer_name (str): Name of the data producer.
            topic (str, optional): Topic associated with the connection. Defaults to None.
            database_type (Optional[str], optional): Type of the database (if applicable). Defaults to None.
            database_name (Optional[str], optional): Name of the database (if applicable). Defaults to None.
            query (Optional[str], optional): Query string used to fetch data (if applicable). Defaults to None.
            is_error (bool, optional): Flag indicating whether the connection has an error. Defaults to False.
            is_warning (bool, optional): Flag indicating whether the connection has a warning. Defaults to False.
            expired_time (datetime, optional): Expiration timestamp for the connection. Defaults to None.

        Attributes:
            name (str): Unique name of the connection.
            producer_type (str): Type of the data producer.
            producer_name (str): Name of the data producer.
            topic (str): Topic associated with the connection.
            database_type (str): Type of the database.
            database_name (str): Name of the database.
            query (str): Query used to fetch data.
            is_error (bool): Indicates if the connection has an error.
            is_warning (bool): Indicates if the connection has a warning.
            status (WorkingDataStatus): Current status of the connection, initialized as READY.
            expired_time (Optional[datetime]): Expiration timestamp.
            data_dict_match (Optional[Dict[str, Any]]): Optional cached data match.
            data_dict_query_source (Optional[Dict[str, Any]]): Optional source data from a query.
            data_dict_result (Optional[List[Dict[str, Any]]]): Optional result of a query.
            list_data_dict_query_result (Optional[List[Dict[str, Any]]]): List of query results.
        """
        # static
        self.name: str = name
        self.producer_type: str = producer_type
        self.producer_name: str = producer_name
        self.topic: str = topic
        self.database_type: str  = database_type
        self.database_name: str  = database_name
        self.query: str = query
        self.is_error: bool = is_error
        self.is_warning: bool = is_warning

        # working data
        self.status: WorkingDataStatus = WorkingDataStatus.READY
        self.expired_time: Optional[datetime] = expired_time
        self.data_dict_match: Optional[Dict[str, Any]] = None
        self.data_dict_query_source: Optional[Dict[str, Any]] = None
        self.data_dict_result: Optional[List[Dict[str, Any]]] = None
        self.list_data_dict_query_result: Optional[List[Dict[str, Any]]] = None
    
    @classmethod
    def from_config(cls, producer_type: str, producer_name: str, topic: str, cfg: DataConnectionConfig) -> "WorkingDataConnection":
        """
        Creates a WorkingDataConnection instance from a DataConnectionConfig object.

        This class method constructs a new WorkingDataConnection by combining runtime
        metadata (producer_type, producer_name, topic) with a configuration object.
        It also calculates the expiration time based on the configuration.

        Args:
            producer_type (str): Type of the data producer (e.g., service or system).
            producer_name (str): Name of the data producer.
            topic (str): Topic associated with the connection.
            cfg (DataConnectionConfig): Configuration object containing connection details,
                including name, database reference, query, expiration, and error/warning flags.

        Returns:
            WorkingDataConnection: A new instance initialized with values from the configuration.
        
        Notes:
            - If `cfg.destination_ref` is None, database_type, database_name, and query
            will be set to None.
            - If `cfg.expired_time_int` is provided, the expiration time is set to the
            current time plus the configured number of minutes.
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
            topic=topic,
            database_type=cfg.destination_ref.type if cfg.destination_ref else None,
            database_name=cfg.destination_ref.name if cfg.destination_ref else None,
            query=cfg.destination_ref.query if cfg.destination_ref else None,
            is_error=cfg.is_error,
            is_warning=cfg.is_warning,
            expired_time=expired_time,
        )

    def update_expired_time(self, minutes: int) -> None:
        """
        Updates the expiration time of the connection.

        Sets the `expired_time` attribute to the current time plus the specified
        number of minutes, effectively extending or shortening the validity period
        of the connection.

        Args:
            minutes (int): Number of minutes to add to the current time for expiration.

        Returns:
            None
        """
        self.expired_time = datetime.now() + timedelta(minutes=minutes)

    def set_query_running_status(self) -> None:
        """
        Sets the connection status to QUERY_RUNNING if appropriate.

        Updates the `status` attribute to `WorkingDataStatus.QUERY_RUNNING` only if
        the current status is neither EXPIRED nor UPDATED. This indicates that a query
        is currently in progress for the connection.

        Returns:
            None
        """
        if self.status != WorkingDataStatus.EXPIRED and self.status != WorkingDataStatus.UPDATED:
            self.status = WorkingDataStatus.QUERY_RUNNING

    def set_ready_status(self) -> None:
        """
        Sets the connection status to READY if it has not expired.

        Updates the `status` attribute to `WorkingDataStatus.READY` unless the
        connection is already marked as EXPIRED. This indicates that the connection
        is ready for use.

        Returns:
            None
        """
        if self.status != WorkingDataStatus.EXPIRED:
            self.status = WorkingDataStatus.READY

    def on_query_done(self, fut: Future[List[Dict[str, Any]]]) -> None:
        """
        Handles completion of an asynchronous query and updates the connection state.

        This method is intended to be used as a callback for a `Future` representing
        an asynchronous query. It retrieves the query result and updates the
        `list_data_dict_query_result` and `status` accordingly:

            - If the result is new (different from the existing data), the result is
            stored and the status is set to UPDATED.
            - If the result is the same as the existing data, the status is set to READY.
            - If an exception occurs while retrieving the result, the expiration time
            is immediately updated to the current time (effectively expiring the connection).

        Args:
            fut (Future[List[Dict[str, Any]]]): A Future representing the asynchronous
                query that returns a list of dictionaries.

        Returns:
            None
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
        Checks whether the connection has expired and updates its status.

        Compares the current time with the `expired_time` attribute. If the current
        time is later than `expired_time`, the `status` is set to `WorkingDataStatus.EXPIRED`.
        If `expired_time` is None, no action is taken.

        Returns:
            None
        """
        if self.expired_time is None:
            return
        if datetime.now() > self.expired_time:
            self.status = WorkingDataStatus.EXPIRED

