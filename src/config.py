import json
from pathlib import Path
from typing import Dict, Any
from .models import DatabaseConfig
from dataclasses import dataclass


@dataclass
class ProcessorConfig:
    """处理器配置"""

    table_name: str = "test_table"
    batch_size: int = 1000
    backup_enabled: bool = True
    preview_enabled: bool = True
    require_confirmation: bool = True
    date_format: str = "YYYY-MM-DD"

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "ProcessorConfig":
        return cls(**{k: v for k, v in config.items() if k in cls.__annotations__})


class ConfigManager:
    """配置管理类"""

    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.config: Dict = {}
        self._load_config()

    def _load_config(self) -> None:
        """加载配置文件"""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        try:
            with self.config_path.open("r") as f:
                self.config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in config file: {e}")

    def get_database_config(self, environment: str) -> DatabaseConfig:
        """获取指定环境的数据库配置"""
        try:
            db_config = self.config["databases"][environment]
            return DatabaseConfig.from_dict(db_config)
        except KeyError:
            raise ValueError(f"Environment '{environment}' not found in config")

    def get_processor_config(self) -> ProcessorConfig:
        """获取处理器配置"""
        processor_config = self.config.get("processor", {})
        return ProcessorConfig.from_dict(processor_config)
