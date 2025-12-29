from datetime import datetime, timedelta
from typing import Dict, Any, Optional

class WorkingDataConnection:

    """
    Represents a working instance of a data connection during agent execution.

    ``WorkingDataConnection`` is used internally by agents to track the state
    of a data connection as it flows through the pipeline, including
    extraction from log lines, query execution, and message production.

    This class stores both static configuration and dynamic runtime state
    such as whether a query is in progress, whether the data has been updated,
    and expiration times for cached results.

    **Responsibilities:**

    - Store identification of the data connection (name, producer, database)
    - Track matched data from regex extraction
    - Store query results and mark if updates occurred
    - Track expiration of the data
    - Mark whether a query is currently in progress

    **Attributes:**

    - ``name`` (str): Unique identifier for this data connection
    - ``producer_type`` (str): Type of the producer this data connection belongs to
    - ``producer_name`` (str): Name of the producer this data connection belongs to
    - ``database_type`` (str, optional): Type of the database this data connection belongs to
    - ``database_name`` (str, optional): Name of the database this data connection belongs to
    - ``query`` (str, optional): Query associated with this data connection
    - ``is_error`` (bool): Indicates if this data connection is associated with an error
    - ``expired_time`` (datetime): Expiration timestamp of the data connection
    - ``query_in_progress`` (bool): True if a query execution is in progress
    - ``is_updated`` (bool): True if the data has been updated since last processing
    - ``data_dict_match`` (Dict[str, Any]): Data extracted by regex matches
    - ``data_dict_query_source`` (Dict[str, Any]): Data used to generate queries
    - ``data_dict_result`` (Dict[str, Any]): Resulting data from executed queries

    **Methods:**

    - ``update_expired_time(minutes: int) -> None``: Updates the ``expired_time``
      by adding the specified number of minutes to the current time.

    :param name: Unique identifier for this data connection
    :type name: str
    :param producer_type: Type of the producer this data connection belongs to
    :type producer_type: str
    :param producer_name: Name of the producer this data connection belongs to
    :type producer_name: str
    :param database_type: Type of the database (optional)
    :type database_type: str, optional
    :param database_name: Name of the database (optional)
    :type database_name: str, optional
    :param query: Query associated with the data connection (optional)
    :type query: str, optional
    :param is_error: Whether this connection represents an error
    :type is_error: bool, optional
    :param expired_time: Expiration timestamp (optional)
    :type expired_time: datetime, optional
    """

    def __init__(self, name: str,
                        producer_type: str, 
                        producer_name: str,
                        database_type: Optional[str] = None,
                        database_name: Optional[str] = None,
                        query: Optional[str] = None,
                        is_error: bool = False,
                        expired_time: datetime = datetime.min):

        """
        Initialize a WorkingDataConnection object.

        Args:
            name (str): Unique identifier for this data connection.
            producer_type (str): Type of the producer that this data connection belongs to.
            producer_name (str): Name of the producer that this data connection belongs to.
            database_type (str, optional): Type of the database that this data connection belongs to. Defaults to None.
            database_name (str, optional): Name of the database that this data connection belongs to. Defaults to None.
            query (str, optional): Query associated with this data connection. Defaults to None.
            is_error (bool, optional): Whether this data connection is associated with an error. Defaults to False.
            expired_time (datetime, optional): Time at which the data associated with this data connection will expire. Defaults to datetime.min.

        Attributes:
            name (str): Unique identifier for this data connection.
            producer_type (str): Type of the producer that this data connection belongs to.
            producer_name (str): Name of the producer that this data connection belongs to.
            database_type (str): Type of the database that this data connection belongs to.
            database_name (str): Name of the database that this data connection belongs to.
            query (str): Query associated with this data connection.
            is_error (bool): Whether this data connection is associated with an error.
            expired_time (datetime): Time at which the data associated with this data connection will expire.
            query_in_progress (bool): Whether a query is currently in progress for this data connection.
            is_updated (bool): Whether the data associated with this data connection has been updated.
            data_dict_match (Dict[str, Any]): Dictionary containing the data that matched the query associated with this data connection.
            data_dict_query_source (Dict[str, Any]): Dictionary containing the data that was used to generate the query associated with this data connection.
            data_dict_result (Dict[str, Any]): Dictionary containing the data that was generated by the query associated with this data connection.
        """
        self.name: str = name
        self.producer_type: str = producer_type
        self.producer_name: str = producer_name
        self.database_type: str  = database_type
        self.database_name: str  = database_name
        self.query: str = query
        self.is_error: bool = is_error

        # working data
        self.expired_time: datetime = expired_time
        self.query_in_progress: bool = False
        self.is_updated: bool = True
        self.data_dict_match: Dict[str, Any] = None
        self.data_dict_query_source: Dict[str, Any] = None
        self.data_dict_result: Dict[str, Any] = None

    def update_expired_time(self, minutes: int) -> None:
        """
        Updates the expired time for this data connection by adding the given number of minutes to the current time.

        Args:
            minutes (int): The number of minutes to add to the current time.
        """
        self.expired_time = datetime.now() + timedelta(minutes=minutes)