from dataclasses import dataclass
from typing import Set, Any, Dict, Optional
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
    def from_dict(cls, config: Dict[str, str]) -> 'DatabaseConfig':
        """从字典创建配置对象"""
        return cls(
            username=config['username'],
            password=config['password'],
            host=config['host'],
            port=config['port'],
            service_name=config['service_name']
        )

@dataclass
class SQLOperation:
    """SQL操作模型"""
    sql: str
    conditions: str
    affected_rows: Optional[int] = None
