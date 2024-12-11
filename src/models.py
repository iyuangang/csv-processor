from dataclasses import dataclass
from typing import Set, Any, Dict, Optional, List
from enum import Enum


class CommandType(Enum):
    """命令类型枚举"""

    DELETE = "delete"
    UPDATE = "update"


@dataclass
class UnmatchedData:
    """未匹配数据模型"""

    column: str
    values: Set[Any]
    condition: str


@dataclass
class DatabaseConfig:
    """数据库配置模型"""

    username: str
    password: str
    host: str
    port: str
    service_name: str

    @classmethod
    def from_dict(cls, config: Dict[str, str]) -> "DatabaseConfig":
        """从字典创建配置对象"""
        return cls(
            username=config["username"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            service_name=config["service_name"],
        )


@dataclass
class SQLOperation:
    """SQL操作模型"""

    command_type: CommandType
    table_name: str
    conditions: Dict[str, Any]  # 条件字段和值的映射
    update_values: Optional[Dict[str, Any]] = None  # 更新字段和值的映射
    affected_rows: Optional[int] = None

    def _format_value(self, column: str, value: Any) -> str:
        """格式化值，处理特殊类型"""
        if value is None:
            return "NULL"
        elif isinstance(value, (int, float)):
            return str(value)
        elif "date" in column.lower():  # 处理日期类型
            return f"TO_DATE('{value}', 'YYYY-MM-DD')"
        else:
            return f"'{str(value)}'"

    def get_where_clause(self) -> str:
        """生成WHERE子句"""
        if not self.conditions:
            return "1=1"  # 如果没有条件，返回永真条件

        conditions = []
        for column, value in self.conditions.items():
            if value is None:
                conditions.append(f"{column} IS NULL")
            else:
                formatted_value = self._format_value(column, value)
                conditions.append(f"{column} = {formatted_value}")
        return " AND ".join(conditions)

    def get_select_sql(self) -> str:
        """生成查询SQL语句"""
        return f"SELECT * FROM {self.table_name} WHERE {self.get_where_clause()}"

    def get_sql(self) -> str:
        """生成操作SQL语句"""
        if self.command_type == CommandType.DELETE:
            return f"DELETE FROM {self.table_name} WHERE {self.get_where_clause()}"

        elif self.command_type == CommandType.UPDATE:
            if not self.update_values:
                raise ValueError("Update values are required for UPDATE operation")

            updates = []
            for column, value in self.update_values.items():
                formatted_value = self._format_value(column, value)
                updates.append(f"{column} = {formatted_value}")

            return f"UPDATE {self.table_name} SET {', '.join(updates)} WHERE {self.get_where_clause()}"

        raise ValueError(f"Unsupported command type: {self.command_type}")

    def get_backup_table_name(self) -> str:
        """获取备份表名"""
        return f"{self.table_name}_bak"
