import cx_Oracle
from contextlib import contextmanager
from typing import Generator, List, Dict, Any
import pandas as pd
from rich.console import Console
from .models import DatabaseConfig, SQLOperation

console = Console()


class DatabaseManager:
    """数据库管理类"""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        self._connect()

    def _connect(self) -> None:
        """建立数据库连接"""
        try:
            dsn = cx_Oracle.makedsn(
                self.config.host,
                self.config.port,
                service_name=self.config.service_name,
            )
            self.connection = cx_Oracle.connect(
                self.config.username, self.config.password, dsn
            )
            self.connection.autocommit = False
        except cx_Oracle.Error as e:
            raise ConnectionError(f"Failed to connect to database: {e}")

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """事务管理器"""
        try:
            yield
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def backup_data(self, operation: SQLOperation) -> None:
        """备份数据"""
        try:
            # 构建备份SQL
            backup_sql = f"""
                INSERT INTO {operation.table_name}_bak 
                SELECT t.*, SYSTIMESTAMP as backup_time 
                FROM {operation.table_name} t 
                WHERE {operation.get_where_clause()}
            """

            with self.connection.cursor() as cursor:
                cursor.execute(backup_sql)

        except cx_Oracle.Error as e:
            raise RuntimeError(f"Failed to backup data: {e}")

    def fetch_data(self, sql: str) -> pd.DataFrame:
        """执行查询并返回DataFrame"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                columns = [desc[0].lower() for desc in cursor.description]
                data = cursor.fetchall()
                return pd.DataFrame(data, columns=columns)
        except cx_Oracle.Error as e:
            raise RuntimeError(f"Failed to fetch data: {e}")

    def execute_operation(self, operation: SQLOperation) -> int:
        """执行SQL操作"""
        with self.connection.cursor() as cursor:
            try:
                # 如果启用了备份，先备份数据
                self.backup_data(operation)

                # 执行操作
                cursor.execute(operation.get_sql())
                return cursor.rowcount

            except cx_Oracle.Error as e:
                raise RuntimeError(f"Failed to execute SQL: {e}")

    def close(self) -> None:
        """关闭数据库连接"""
        if self.connection:
            try:
                self.connection.close()
            except cx_Oracle.Error as e:
                console.print(
                    f"[yellow]Warning: Error closing database connection: {e}[/yellow]"
                )
