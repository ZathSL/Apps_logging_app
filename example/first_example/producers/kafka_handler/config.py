from ..base import BaseProducerConfig
from pydantic import field_validator
from typing import List

class KafkaHandlerConfig(BaseProducerConfig):
    brokers: List[str]
    security_protocol: str = "SSL"
    ssl_cafile: str
    ssl_certfile: str
    ssl_keyfile: str
    ssl_password: str
    topic: str
    acks: str = "all"
    retries: int = 3
    batch_size: int = 16384
    linger_ms: int = 5
    buffer_memory: int = 33554432

    # Field validator brokers
    @field_validator('brokers')
    def validate_brokers(cls, v):
        if not v or not isinstance(v, list) or len(v) == 0:
            raise ValueError('Brokers must be a non-empty list of broker addresses')
        for broker in v:
            if not isinstance(broker, str) or not broker.strip():
                raise ValueError('Each broker address must be a non-empty string')
        return v
    
    # Field validator security_protocol
    @field_validator('security_protocol')
    def validate_security_protocol(cls, v):
        valid_protocols = {"SSL", "PLAINTEXT", "SASL_SSL", "SASL_PLAINTEXT"}
        if v not in valid_protocols:
            raise ValueError(f"Security protocol must be one of {valid_protocols}")
        return v

    # Field validator acks
    @field_validator('acks')
    def validate_acks(cls, v):
        valid_acks = {"all", "0", "1"}
        if v not in valid_acks:
            raise ValueError(f"Acks must be one of {valid_acks}")
        return v
    
    # Field validator retries, batch_size, linger_ms, buffer_memory
    @field_validator('retries', 'batch_size', 'linger_ms', 'buffer_memory')
    def validate_positive_int(cls, v, field):
        if v < 0:
            raise ValueError(f"{field.name} must be a non-negative integer")
        return v
