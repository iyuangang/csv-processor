import json
from pathlib import Path
from typing import Dict
from .models import DatabaseConfig

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
            with self.config_path.open('r') as f:
                self.config = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format in config file: {e}")

    def get_database_config(self, environment: str) -> DatabaseConfig:
        """获取指定环境的数据库配置"""
        try:
            db_config = self.config['databases'][environment]
            return DatabaseConfig.from_dict(db_config)
        except KeyError:
            raise ValueError(f"Environment '{environment}' not found in config")
