
from typing import Dict, Any, Optional, List
from ..registry import register_database
from .config import OracleDatabaseConfig
from ..base import BaseDatabase
from ..model import QueryTask
import oracledb

@register_database(
    database_type="oracle",
    config_model=OracleDatabaseConfig,
)
class OracleDatabase(BaseDatabase):
    def __init__(self, config: OracleDatabaseConfig) -> None:
        """
        Initializes an OracleDatabase instance with the given configuration.

        Args:
            config (OracleDatabaseConfig): The configuration for the database.

        """
        super().__init__(config)

    def _build_dsn(self) -> str:
        """
        Builds a DSN string for connecting to the database.

        The DSN string will contain the primary host and port, as well as the
        replica host and port if specified.

        Returns:
            str: The DSN string for connecting to the database.
        """
        addresses = [
            f"(ADDRESS=(PROTOCOL=TCP)(HOST={self.config.primary.host})(PORT={self.config.primary.port}))"
        ]

        if self.config.replica:
            addresses.append(
                f"(ADDRESS=(PROTOCOL=TCP)(HOST={self.config.replica.host})(PORT={self.config.replica.port}))"
            )

        address_list = "".join(addresses)

        return f"""
        (DESCRIPTION=
            (ADDRESS_LIST=
                (LOAD_BALANCE=OFF)
                (FAILOVER=ON)
                {address_list}
            )
            (CONNECT_DATA=
                (SERVICE_NAME={self.config.primary.service_name})
            )
        )
        """

    def connect(self) -> None:
        """
        Connects to the Oracle Database using the built DSN string and configuration.

        Connects to the Oracle Database using the built DSN string and configuration.
        The connection pool is created with the following parameters:

        - min: 1
        - max: self.config.max_workers + 1
        - increment: self.config.max_workers/2
        - timeout: 60
        - ping_interval: 60

        If the connection fails, an exception is raised with the error message.
        """
        self.dsn = self._build_dsn()
        try:
            self.pool = oracledb.create_pool(
                user=self.config.username,
                password=self.config.password,
                dsn=self.dsn,
                min=1,
                max=self.config.max_workers + 1,
                increment=self.config.max_workers/2,
                timeout=60,
                ping_interval=60
            )
        except Exception as e:
            self.logger.error(f"Client DB {self.config.type}-{self.config.name}: Failed to connect: {e}")
            raise

    def close(self) -> None:
        self.pool.close()

    def _query(self, task: QueryTask) -> List[Dict[str, Any]]:
        try:
            with self.pool.acquire() as conn:
                with conn.cursor() as cur:
                    cur.execute(task.query, task.params or {})
                    columns = [col[0].lower() for col in cur.description]
                    rows = cur.fetchall()
                    return [dict(zip(columns, row)) for row in rows]
        except Exception:
            raise
