from pydantic import BaseModel, field_validator

class BaseProducerConfig(BaseModel):
    """
    Base configuration model for a producer.

    This class defines the common configuration fields required by all producers
    in the system. It is intended to be subclassed or used as a base for type-specific
    producer configurations.

    Attributes
    ----------
    type : str
        The type of the producer. This is typically used to look up the corresponding
        producer class in the registry.
    name : str
        The unique name of the producer instance.
    max_retries : int, default=5
        The maximum number of times a message should be retried in case of failure.
        Must be greater than 0.

    Validators
    ----------
    validate_max_retries
        Ensures that `max_retries` is greater than 0. Raises a ValueError otherwise.

    Notes
    -----
    - This class uses Pydantic for data validation and type enforcement.
    - Subclasses can extend this configuration model with additional fields specific
      to their producer type.
    """
    type: str
    name: str
    max_retries: int = 5
    
    @field_validator('max_retries')
    def validate_max_retries(cls, value):
        """
        Validates the max retries field of the BaseProducerConfig model.

        :param value: The value of the max retries field.
        :raises ValueError: If the max retries is less than or equal to 0.
        :return: The validated max retries value.
        """
        if value <= 0:
            raise ValueError("Max retries must be greater than 0")
        return value
