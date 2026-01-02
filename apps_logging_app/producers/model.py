from pydantic import BaseModel, field_validator
from typing import List

class BaseProducerConfig(BaseModel):
    """
    Configuration model for a producer.

    This class defines the basic configuration parameters required to
    initialize a producer. It uses Pydantic for data validation and
    ensures that the configuration values conform to expected types and constraints.

    Attributes:
        type (str): The type of the producer.
        name (str): The name of the producer.
        topics (List[str], optional): A list of topics that this producer can handle. Defaults to None.
        max_retries (int, optional): Maximum number of retries for producer operations. Defaults to 5.
    """
    type: str
    name: str
    topics: List[str] = None
    max_retries: int = 5
    
    @field_validator('max_retries')
    def validate_max_retries(cls, value):
        """
        Validates that the `max_retries` field is greater than 0.

        This validator is automatically called by Pydantic when a `BaseProducerConfig`
        instance is created. It ensures that the maximum number of retries is a positive integer.

        Args:
            cls (Type[BaseProducerConfig]): The class being validated.
            value (int): The value provided for `max_retries`.

        Returns:
            int: The validated `max_retries` value if it passes the check.

        Raises:
            ValueError: If `value` is less than or equal to 0.
        """
        if value <= 0:
            raise ValueError("Max retries must be greater than 0")
        return value
