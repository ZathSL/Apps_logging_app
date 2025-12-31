from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from concurrent.futures import Future
from enum import Enum
from .model import DataConnectionConfig

class WorkingDataStatus(Enum):
    READY = "ready"
    QUERY_RUNNING = "query_running"
    UPDATED = "updated"
    EXPIRED = "expired"

class WorkingDataConnection:
    def __init__(self, name: str,
                        producer_type: str, 
                        producer_name: str,
                        database_type: Optional[str] = None,
                        database_name: Optional[str] = None,
                        query: Optional[str] = None,
                        is_error: bool = False,
                        expired_time: datetime = None) -> None:
        # static
        self.name: str = name
        self.producer_type: str = producer_type
        self.producer_name: str = producer_name
        self.database_type: str  = database_type
        self.database_name: str  = database_name
        self.query: str = query
        self.is_error: bool = is_error

        # working data
        self.status: WorkingDataStatus = WorkingDataStatus.READY
        self.expired_time: Optional[datetime] = expired_time
        self.data_dict_match: Optional[Dict[str, Any]] = None
        self.data_dict_query_source: Optional[Dict[str, Any]] = None
        self.data_dict_result: Optional[List[Dict[str, Any]]] = None
        self.list_data_dict_query_result: Optional[List[Dict[str, Any]]] = None
    @classmethod
    def from_config(cls, producer_type: str, producer_name: str, cfg: DataConnectionConfig) -> "WorkingDataConnection":
        expired_time = (
            datetime.now() + timedelta(minutes=cfg.expired_time_int)
            if cfg.expired_time_int is not None
            else None
        )

        return cls(
            name=cfg.name,
            producer_type=producer_type,
            producer_name=producer_name,
            database_type=cfg.destination_ref.type if cfg.destination_ref else None,
            database_name=cfg.destination_ref.name if cfg.destination_ref else None,
            query=cfg.destination_ref.query if cfg.destination_ref else None,
            is_error=cfg.is_error,
            expired_time=expired_time,
        )

    def update_expired_time(self, minutes: int) -> None:
        self.expired_time = datetime.now() + timedelta(minutes=minutes)


    def on_query_done(self, fut: Future[List[Dict[str, Any]]]) -> None:
        try:
            result: List[Dict[str, Any]] = fut.result()
            if result and self.list_data_dict_query_result != result:
                self.list_data_dict_query_result = result
                self.status = WorkingDataStatus.UPDATED
            elif result and self.list_data_dict_query_result == result:
                self.status = WorkingDataStatus.READY
        except Exception:
            self.status = WorkingDataStatus.READY


    def check_expired_time(self) -> None:
        if self.expired_time is None:
            return
        if datetime.now() > self.expired_time:
            self.status = WorkingDataStatus.EXPIRED

