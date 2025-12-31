from pydantic import BaseModel, field_validator

class BaseProducerConfig(BaseModel):
    type: str
    name: str
    max_retries: int = 5


    @field_validator('type')
    def validate_type(cls, value):
        if value is None:
            raise ValueError("Type cannot be None")
        return value
    
    @field_validator('name')
    def validate_name(cls, value):
        if value is None:
            raise ValueError("Name cannot be None")
        return value
    
    @field_validator('max_retries')
    def validate_max_retries(cls, value):
        if value <= 0:
            raise ValueError("Max retries must be greater than 0")
        return value
