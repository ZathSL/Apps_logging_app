from typing import Any
class Message:
    """
    Represents a message with a topic, type flags, content, and retry count.

    This class is used to encapsulate information about a message, including 
    whether it represents an error or warning, the actual message content, 
    and the number of times it has been retried.

    Attributes:
        topic (str): The topic or category of the message.
        is_error (bool): Indicates whether the message is an error.
        is_warning (bool): Indicates whether the message is a warning.
        message (Any): The content of the message.
        retries (int): The number of times this message has been retried. Defaults to 0.
    """
    def __init__(self, topic: str, is_error: bool, is_warning: bool, message: Any) -> None:
        """
        Initializes a Message instance with the given topic, flags, and content.

        Args:
            topic (str): The topic or category of the message.
            is_error (bool): Whether the message represents an error.
            is_warning (bool): Whether the message represents a warning.
            message (Any): The content of the message.

        Attributes:
            topic (str): The topic or category of the message.
            is_error (bool): Indicates if the message is an error.
            is_warning (bool): Indicates if the message is a warning.
            message (Any): The content of the message.
            retries (int): The number of times this message has been retried. Initialized to 0.
        """
        self.topic = topic
        self.is_error = is_error
        self.is_warning = is_warning
        self.message = message
        self.retries = 0