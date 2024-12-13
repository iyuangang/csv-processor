from typing import List, Dict, Set, Tuple, Any
import pandas as pd
from rich.console import Console
from rich.panel import Panel
import click
from .models import UnmatchedData, SQLOperation, CommandType, TableConfig
from .database import DatabaseManager
from pathlib import Path

console = Console()


class DataProcessor:
    """数据处理类"""

    def __init__(self, db_manager: DatabaseManager, config: Dict[str, Any]):
        self.db_manager = db_manager
        self.config = config
        self.tables_config = {
            name: TableConfig.from_dict(cfg)
            for name, cfg in config.get("tables", {}).items()
        }
        self.batch_size = config.get("processor", {}).get("batch_size", 1000)
        self.preview_enabled = config.get("processor", {}).get("preview_enabled", True)
        self.require_confirmation = config.get("processor", {}).get(
            "require_confirmation", True
        )

    def _prepare_operation(self, row: pd.Series) -> SQLOperation:
        """准备SQL操作"""
        table_name = row["table"]
        table_config = self.tables_config.get(table_name)
        if not table_config:
            raise ValueError(f"Unknown table: {table_name}")

        command = CommandType(row["command"].lower())

        # 获取条件列（排除table、command和new_开头的列）
        condition_cols = [
            col
            for col in row.index
            if col not in ["table", "command"]
            and not col.startswith("new_")
            and pd.notna(row[col])
        ]

        # 映射列名并构建条件字典
        conditions = {table_config.map_column(col): row[col] for col in condition_cols}

        if command == CommandType.DELETE:
            return SQLOperation(
                command_type=command,
                table_name=table_name,
                conditions=conditions,
                table_config=table_config,
            )

        elif command == CommandType.UPDATE:
            # 获取更新列
            update_cols = [
                col
                for col in row.index
                if col.startswith("new_") and pd.notna(row[col])
            ]

            # 映射列名并构建更新值字典
            update_values = {
                table_config.map_column(col.replace("new_", "")): row[col]
                for col in update_cols
            }

            return SQLOperation(
                command_type=command,
                table_name=table_name,
                conditions=conditions,
                update_values=update_values,
                table_config=table_config,
            )

    def process_file(self, file_path: str) -> None:
        """处理输入文件"""
        file_type = Path(file_path).suffix.lower()

        if file_type == ".yaml" or file_type == ".yml":
            from .yaml_processor import YAMLProcessor

            yaml_processor = YAMLProcessor(self)
            yaml_processor.process_yaml(file_path)
        elif file_type == ".csv":
            self._process_csv(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    def _process_csv(self, csv_path: str) -> None:
        """处理CSV文件"""
        try:
            df = pd.read_csv(csv_path)
            self._validate_dataframe(df)

            # 按表名和命令类型分组处理
            for table_name in df["table"].unique():
                df_table = df[df["table"] == table_name]
                for command_type in CommandType:
                    df_cmd = df_table[
                        df_table["command"].str.lower() == command_type.value
                    ]
                    if not df_cmd.empty:
                        self._process_batch(df_cmd)

        except Exception as e:
            console.print(f"[red bold]Error processing CSV file: {str(e)}[/red bold]")
            raise

    def _process_batch(self, df: pd.DataFrame) -> None:
        """批量处理数据"""
        # 如果是删除操作且条件列相同，合并为一个IN查询
        if all(df["command"].str.lower() == "delete"):
            # 获取第一行数据来确定表名和条件列
            first_row = df.iloc[0]
            table_name = first_row["table"]
            table_config = self.tables_config[table_name]

            # 获取除table和command外的条件列
            condition_cols = [
                col for col in df.columns if col not in ["table", "command"]
            ]

            # 检查所有行的条件列是否相同
            if condition_cols and all(df[condition_cols].notna().all()):
                # 获取映射后的列名
                db_column = table_config.columns_mapping.get(
                    condition_cols[0], condition_cols[0]
                )

                # 收集所有行的条件值
                values = set()
                for _, row in df.iterrows():
                    values.add(row[condition_cols[0]])

                # 创建一个合并的操作
                operation = SQLOperation(
                    command_type=CommandType.DELETE,
                    table_name=table_name,
                    conditions={db_column: list(values)},
                    table_config=table_config,
                )
                self._execute_operation(operation)
                return

        # 其他情况按原方式处理
        for i in range(0, len(df), self.batch_size):
            batch = df.iloc[i : i + self.batch_size]
            console.print(
                f"\n[cyan]Processing batch {i//self.batch_size + 1}...[/cyan]"
            )

            for _, row in batch.iterrows():
                operation = self._prepare_operation(row)
                self._execute_operation(operation)

    def _validate_dataframe(self, df: pd.DataFrame) -> None:
        """验证DataFrame格式"""
        if "command" not in df.columns:
            raise ValueError("CSV must contain 'command' column")

        invalid_commands = set(df["command"]) - {cmd.value for cmd in CommandType}
        if invalid_commands:
            raise ValueError(f"Invalid commands found: {invalid_commands}")

    def _execute_operation(self, operation: SQLOperation) -> None:
        """执行操作"""
        try:
            # 获取受影响的数据
            df_before = self.db_manager.fetch_data(operation.get_select_sql())

            if df_before.empty:
                console.print(
                    f"[yellow]No matching data found for conditions:[/yellow]"
                )
                console.print(
                    Panel(
                        operation.get_where_clause(),
                        title="[bold yellow]Unmatched Conditions[/bold yellow]",
                    )
                )
                return

            # 显示数据和SQL
            if self.preview_enabled:
                console.print("\n[bold cyan]Affected data:[/bold cyan]")
                console.print(df_before.to_string())
                console.print(
                    Panel(
                        operation.get_sql(),
                        title="[bold yellow]SQL to execute[/bold yellow]",
                    )
                )

            # 用户确认
            if self.require_confirmation and not click.confirm(
                "Do you want to proceed with this operation?"
            ):
                console.print("[yellow]Operation cancelled by user[/yellow]")
                return

            # 获取表配置和主键列名
            table_config = operation.table_config
            primary_key = table_config.primary_key

            # 确保列名大小写匹配
            pk_column = next(
                col for col in df_before.columns if col.lower() == primary_key.lower()
            )
            affected_ids = df_before[pk_column].tolist()

            # 执行操作
            affected_rows = self.db_manager.execute_operation(operation)

            # 使用主键查询更新后的数据
            id_conditions = []
            for id_val in affected_ids:
                if isinstance(id_val, (int, float)):
                    id_conditions.append(str(id_val))
                else:
                    id_conditions.append(f"'{id_val}'")

            id_list = ",".join(id_conditions)
            df_after = self.db_manager.fetch_data(
                f"SELECT * FROM {operation.table_name} WHERE {primary_key} IN ({id_list})"
            )

            # 根据操作类型验证结果
            if operation.command_type == CommandType.DELETE:
                if df_after.empty:
                    console.print(
                        f"[green]Successfully deleted {affected_rows} rows[/green]"
                    )
                else:
                    console.print(
                        Panel(
                            df_after.to_string(),
                            title="[bold red]Warning: Some rows were not deleted![/bold red]",
                        )
                    )
                    raise RuntimeError("Delete operation failed: Some rows still exist")
            else:
                if df_after.empty:
                    console.print(
                        "[red]Warning: Cannot find updated rows for verification[/red]"
                    )
                else:
                    console.print("\n[bold cyan]Data after update:[/bold cyan]")
                    console.print(df_after.to_string())

                    # 显示数据变化
                    changes = self._get_data_changes(df_before, df_after)
                    if changes:
                        console.print("\n[bold green]Changes made:[/bold green]")
                        for change in changes:
                            console.print(change)
                    else:
                        console.print(
                            "[yellow]Warning: No changes detected in the data[/yellow]"
                        )

        except Exception as e:
            console.print(f"[red]Error executing operation: {str(e)}[/red]")
            raise

    def _get_data_changes(
        self, df_before: pd.DataFrame, df_after: pd.DataFrame
    ) -> List[str]:
        """比较更新前后的数据变化"""
        changes = []

        # 确保列名小写，便于统一处理
        df_before.columns = df_before.columns.str.lower()
        df_after.columns = df_after.columns.str.lower()

        # 对每一行进行较
        for idx in df_after.index:
            if idx < len(df_before):
                row_before = df_before.iloc[idx]
                row_after = df_after.iloc[idx]

                # 比较每一列的值
                for col in df_after.columns:
                    # 转换为字符串进行比较，避免数据类型不一致的问题
                    before_val = str(row_before[col])
                    after_val = str(row_after[col])
                    if before_val != after_val:
                        changes.append(
                            f"Column {col}: "
                            f"[red]{before_val}[/red] → "
                            f"[green]{after_val}[/green]"
                        )

        return changes

    @staticmethod
    def _generate_in_clause(values: Set[Any]) -> str:
        """生成IN子句"""
        if all(isinstance(v, (int, float)) for v in values):
            return f"({','.join(str(v) for v in values)})"
        return f"""({','.join(f"'{str(v)}'" for v in values)})"""

    @staticmethod
    def _display_operation_info(df: pd.DataFrame, operation: SQLOperation) -> None:
        """显示操作信息"""
        console.print("\n[bold cyan]Affected data:[/bold cyan]")
        console.print(df.to_string())
        console.print(
            Panel(operation.sql, title="[bold yellow]SQL to execute[/bold yellow]")
        )
