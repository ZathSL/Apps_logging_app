from pydantic import BaseModel, field_validator, model_validator
from typing import List, Optional, Pattern, Tuple
from pathlib import Path
import re


class PathFileConfig(BaseModel):
    name: str
    path: Path
    cursor: int = 0
    id: Tuple | None = None
    
    @field_validator('name')
    def validate_name(cls, value) -> str:
        if value is None:
            raise ValueError("Name cannot be None")
        return value
    
    @field_validator('path')
    def validate_path(cls, value) -> Path:
        if value is None:
            raise ValueError("Path cannot be None")
        if not isinstance(value, Path):
            raise ValueError("Path must be a Path object")
        if not value.exists():
            raise ValueError("Path does not exist")
        return value
    
    @field_validator('id')
    def validate_inode(cls, value) -> int | None:
        if value is not None and type(value) is not int:
            raise ValueError("id must be an integer")
        return value
    

class RegexPatternConfig(BaseModel):
    path_file_name: str
    regex_pattern: Pattern[str]
    
    @field_validator('regex_pattern', mode='before')
    def compile_regex(cls, v) -> Pattern[str]:
        if isinstance(v, str):
            return re.compile(v)
        return v


class QueryConfig(BaseModel):
    type: str
    name: str
    query: str

    @field_validator('type')
    def validate_type(cls, value) -> str:
        if value is None:
            raise ValueError("Type cannot be None")
        return value
    
    @field_validator('name')
    def validate_name(cls, value) -> str:
        if value is None:
            raise ValueError("Name cannot be None")
        return value
    
    @field_validator('query')
    def validate_query(cls, value) -> str:
        if value is None:
            raise ValueError("Query cannot be None")
        return value

class DataConnectionConfig(BaseModel):
    name: str
    is_error: bool
    source_ref: RegexPatternConfig = None
    destination_ref: Optional[QueryConfig] = None
    expired_time_int: Optional[int] = None

    @field_validator('name')
    def validate_name(cls, value) -> str:
        if value is None:
            raise ValueError("Name cannot be None")
        return value

    @field_validator('is_error')
    def validate_is_error(cls, value) -> bool:
        if type(value) is not bool:
            raise ValueError("is_error must be a boolean")
        return value
    
    @field_validator('expired_time_int')
    def validate_expired_time(cls, value) -> int | None:
        if value is not None and type(value) is not int:
            raise ValueError("expired_time_int must be an integer")
        return value
    
class ProducerConnectionConfig(BaseModel):
    type: str
    name: str
    data_connections: List[DataConnectionConfig]

    @field_validator('type')
    def validate_type(cls, value) -> str:
        if value is None:
            raise ValueError("Type cannot be None")
        return value
    
    @field_validator('name')
    def validate_name(cls, value) -> str:
        if value is None:
            raise ValueError("Name cannot be None")
        return value


class BaseAgentConfig(BaseModel):
    type: str # type of agent
    name: str
    buffer_rows: Optional[int] = 500
    path_files: Optional[List[PathFileConfig]] = None
    producer_connections: List[ProducerConnectionConfig]
    pool_interval: float = 120
    

    @field_validator('type')
    def validate_type(cls, value) -> str:
        if value is None:
            raise ValueError("Type cannot be None")
        return value
    
    @field_validator('name')
    def validate_name(cls, value) -> str:
        if value is None:
            raise ValueError("Name cannot be None")
        return value

    @field_validator('buffer_rows')
    def validate_buffer_rows(cls, value) -> int:
        if value <= 0:
            raise ValueError("Buffer rows must be greater than 0")
        return value
    
    @field_validator('pool_interval')
    def validate_pool_interval(cls, value) -> float:
        if value <= 0:
            raise ValueError("Pool interval must be greater than 0")
        return value
    

    @model_validator(mode='after')
    def validate_regex_path_references(self) -> "BaseAgentConfig":
        if not self.path_files:
            return self

        valid_names = {pf.name for pf in self.path_files}

        for producer in self.producer_connections:
            for dc in producer.data_connections:
                if dc.source_ref:
                    if dc.source_ref.path_file_name not in valid_names:
                        raise ValueError(
                            f"Invalid path_file_name '{dc.source_ref.path_file_name}'. "
                            f"Valid values: {valid_names}"
                        )
        return self
