import yaml
from typing import List, Dict, Any
import pandas as pd
from pathlib import Path
from rich.console import Console
from .models import YAMLOperation, YAMLBatch

console = Console()


class YAMLProcessor:
    """YAML处理器"""

    def __init__(self, data_processor):
        self.data_processor = data_processor

    def load_yaml(self, yaml_path: str) -> Dict[str, Any]:
        """加载YAML文件"""
        try:
            with open(yaml_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            console.print(f"[red]Error parsing YAML file: {e}[/red]")
            raise

    def validate_yaml(self, data: Dict[str, Any]) -> None:
        """验证YAML格式"""
        required_fields = {"version": str, "description": str, "batches": list}

        for field, field_type in required_fields.items():
            if field not in data:
                raise ValueError(f"Missing required field: {field}")
            if not isinstance(data[field], field_type):
                raise ValueError(f"Invalid type for {field}: expected {field_type}")

    def process_yaml(self, yaml_path: str) -> None:
        """处理YAML文件"""
        data = self.load_yaml(yaml_path)
        self.validate_yaml(data)

        console.print(f"[cyan]Processing YAML file: {yaml_path}[/cyan]")
        console.print(f"Version: {data['version']}")
        console.print(f"Description: {data['description']}")

        for batch_data in data["batches"]:
            batch = YAMLBatch.from_dict(batch_data)
            self._process_batch(batch)

    def _process_batch(self, batch: YAMLBatch) -> None:
        """处理批次操作"""
        console.print(f"\n[cyan]Processing batch: {batch.id}[/cyan]")
        if batch.description:
            console.print(f"Description: {batch.description}")

        # 转换为DataFrame处理
        operations_data = []
        for op in batch.operations:
            row_data = {"table": op.table, "command": op.command}
            row_data.update(op.conditions)
            if op.new_values:
                row_data.update({f"new_{k}": v for k, v in op.new_values.items()})
            operations_data.append(row_data)

        df = pd.DataFrame(operations_data)
        self.data_processor._process_batch(df)

    def generate_template(self, output_path: str) -> None:
        """生成YAML模板文件"""
        template = {
            "version": "1.0",
            "description": "Database operations template",
            "batches": [
                {
                    "id": "BATCH_001",
                    "description": "Batch description",
                    "operations": [
                        {
                            "table": "table_name",
                            "command": "update/delete",
                            "description": "Operation description",
                            "conditions": {"column1": "value1", "column2": "value2"},
                            "new_values": {
                                "column1": "new_value1",
                                "column2": "new_value2",
                            },
                        }
                    ],
                }
            ],
        }

        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(template, f, default_flow_style=False, allow_unicode=True)

    def validate_operation(self, operation: YAMLOperation) -> None:
        """验证单个操作"""
        # 验证表是否存在
        if operation.table not in self.data_processor.tables_config:
            raise ValueError(f"Unknown table: {operation.table}")

        # 验证命令类型
        if operation.command not in [cmd.value for cmd in CommandType]:
            raise ValueError(f"Invalid command: {operation.command}")

        # 验证条件字段
        table_config = self.data_processor.tables_config[operation.table]
        for column in operation.conditions:
            if column not in table_config.columns_mapping:
                raise ValueError(f"Unknown column in conditions: {column}")

        # 验证更新字段
        if operation.new_values:
            for column in operation.new_values:
                if column not in table_config.columns_mapping:
                    raise ValueError(f"Unknown column in new_values: {column}")
