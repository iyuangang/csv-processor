from dataclasses import dataclass, field
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
class TableConfig:
    """表配置模型"""

    primary_key: str
    date_columns: List[str]
    number_columns: List[str]
    backup_enabled: bool = True
    columns_mapping: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "TableConfig":
        return cls(
            primary_key=config["primary_key"],
            date_columns=config.get("date_columns", []),
            number_columns=config.get("number_columns", []),
            backup_enabled=config.get("backup_enabled", True),
            columns_mapping=config.get("columns_mapping", {}),
        )

    def map_column(self, csv_column: str) -> str:
        """将CSV列名映射到实际表列名"""
        return self.columns_mapping.get(csv_column, csv_column)


@dataclass
class SQLOperation:
    """SQL操作模型"""

    command_type: CommandType
    table_name: str
    conditions: Dict[str, Any]
    table_config: TableConfig
    update_values: Optional[Dict[str, Any]] = None
    affected_rows: Optional[int] = None

    def _process_append_value(self, column: str, value: str) -> str:
        """处理追加值的特殊语法"""
        if not isinstance(value, str) or not value.startswith("+"):
            return value

        append_text = value[1:]  # 移除 '+' 前缀
        if not append_text:  # 如果只有 '+'，忽略这个更新
            return None

        # 使用 CONCAT 函数（Oracle 使用 ||）
        return f"{column} || '{append_text}'"

    def _format_value(self, column: str, value: Any) -> str:
        """格式化值，处理特殊类型"""
        if value is None:
            return "NULL"

        # 处理追加文本的情况
        if isinstance(value, str) and value.startswith("+"):
            processed_value = self._process_append_value(column, value)
            if processed_value is None:
                return column  # 返回原列名，相当于不更新
            return processed_value

        # 处理其他类型
        elif column in self.table_config.number_columns:
            return str(value)
        elif column in self.table_config.date_columns:
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
                if formatted_value != column:  # 只有当值不等于列名时才添加更新
                    updates.append(f"{column} = {formatted_value}")

            if not updates:  # 如果没有有效的更新，抛出异常
                raise ValueError("No valid update values provided")

            return f"UPDATE {self.table_name} SET {', '.join(updates)} WHERE {self.get_where_clause()}"

        raise ValueError(f"Unsupported command type: {self.command_type}")

    def get_backup_table_name(self) -> str:
        """获取备份表名"""
        return f"{self.table_name}_bak"
