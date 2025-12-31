from pydantic import BaseModel, field_validator
from typing import Optional, Dict, Any
import re
from concurrent.futures import Future


class QueryTask:
    def __init__(self, query: str, params: Optional[Dict[str, Any]] = None):
        self.query = query
        self.params = params
        self.future: Future = Future()
        self.retries = 0

class ConnectionConfig(BaseModel):
    host: str
    port: int
    service_name: Optional[str]

    @field_validator('host')
    def validate_host(cls, v):
        if not v or not v.strip():
            raise ValueError('Host cannot be empty')
        # Basic validation for hostname or IP
        if not re.match(r'^[a-zA-Z0-9.-]+$', v):
            raise ValueError('Host must be a valid hostname or IP address')
        return v

    @field_validator('port')
    def validate_port(cls, v):
        if not isinstance(v, int) or not 1 <= v <= 65535:
            raise ValueError('Port must be an integer between 1 and 65535')
        return v

class BaseDatabaseConfig(BaseModel):
    type: str
    name: str
    username: str
    password: str
    primary: ConnectionConfig
    replica: Optional[ConnectionConfig] = None
    max_retries: int = 5
    max_workers: int = 10

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

    @field_validator('username')
    def validate_username(cls, value):
        if value is None:
            raise ValueError("Username cannot be None")
        return value
    
    @field_validator('password')
    def validate_password(cls, value):
        if value is None:
            raise ValueError("Password cannot be None")
        return value
    
    @field_validator('max_retries')
    def validate_max_retries(cls, value):
        if value <= 0:
            raise ValueError("Max retries must be greater than 0")
        return value
    
    @field_validator('max_workers')
    def validate_max_workers(cls, value):
        if value <= 0:
            raise ValueError("Max workers must be greater than 0")
        return value
