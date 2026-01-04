from typing import Any, Dict
from concurrent.futures import Future

class Query:
    """
    Represents an asynchronous query with parameters and retry tracking.

    This class encapsulates a query string along with its parameters, 
    manages retry attempts, and provides a Future object to handle 
    asynchronous execution results.

    Attributes:
        query (str): The query string to be executed.
        params (Dict[str, Any]): A dictionary of parameters for the query.
        retries (int): The number of times the query has been retried.
        future (Future): A Future object representing the result of the query.
    """
    def __init__(self, query: str, params: Dict[str, Any]) -> None:
        """
        Initializes a Query instance with the given query string and parameters.

        Args:
            query (str): The query string to be executed.
            params (Dict[str, Any]): A dictionary of parameters to use with the query.

        Attributes:
            query (str): Stores the query string.
            params (Dict[str, Any]): Stores the query parameters.
            retries (int): Initialized to 0, counts how many times the query has been retried.
            future (Future): A Future object used to hold the result of the query asynchronously.
        """
        self.query = query
        self.params = params
        self.retries = 0
        self.future: Future = Future()

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"query={self.query!r}, "
            f"params={self.params!r}, "
            f"retries={self.retries!r}, "
            f"future_done={self.future.done()}"
            f")"
        )
