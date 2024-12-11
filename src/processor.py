from typing import List, Dict, Set, Tuple, Any
import pandas as pd
from rich.console import Console
from rich.panel import Panel
import click
from .models import UnmatchedData, SQLOperation, CommandType
from .database import DatabaseManager

console = Console()


class DataProcessor:
    """数据处理类"""

    def __init__(self, db_manager: DatabaseManager, config: Dict[str, Any] = None):
        self.db_manager = db_manager
        self.config = config or {}
        self.table_name = self.config.get("table_name", "test_table")
        self.batch_size = self.config.get("batch_size", 1000)
        self.backup_enabled = self.config.get("backup_enabled", True)
        self.preview_enabled = self.config.get("preview_enabled", True)
        self.require_confirmation = self.config.get("require_confirmation", True)

        # 确保备份表存在
        if self.backup_enabled:
            self._ensure_backup_table_exists()

    def _ensure_backup_table_exists(self) -> None:
        """确保备份表存在"""
        try:
            # 检查备份表是否存在
            check_sql = f"""
                SELECT COUNT(*) 
                FROM user_tables 
                WHERE table_name = '{self.table_name.upper()}_BAK'
            """
            result = self.db_manager.fetch_data(check_sql)

            if result.iloc[0, 0] == 0:
                # 如果备份表不存在，创建它
                create_sql = f"""
                    CREATE TABLE {self.table_name}_bak AS 
                    SELECT t.*, SYSTIMESTAMP as backup_time 
                    FROM {self.table_name} t 
                    WHERE 1=0
                """
                with self.db_manager.connection.cursor() as cursor:
                    cursor.execute(create_sql)
                console.print(
                    f"[green]Created backup table: {self.table_name}_bak[/green]"
                )

        except Exception as e:
            console.print(
                f"[yellow]Warning: Failed to ensure backup table exists: {str(e)}[/yellow]"
            )
            self.backup_enabled = False

    def _prepare_operation(self, row: pd.Series) -> SQLOperation:
        """准备SQL操作"""
        command = CommandType(row["command"].lower())

        # 获取条件列（command列之前的所有列）
        command_idx = list(row.index).index("command")
        condition_cols = [col for col in row.index[:command_idx] if pd.notna(row[col])]

        # 构建条件字典
        conditions = {col: row[col] for col in condition_cols}

        if command == CommandType.DELETE:
            return SQLOperation(
                command_type=command, table_name=self.table_name, conditions=conditions
            )

        elif command == CommandType.UPDATE:
            # 获取更新列（command列之后的所有new_开头的列）
            update_cols = [
                col
                for col in row.index[command_idx:]
                if col.startswith("new_") and pd.notna(row[col])
            ]

            # 构建更新值字典
            update_values = {col.replace("new_", ""): row[col] for col in update_cols}

            return SQLOperation(
                command_type=command,
                table_name=self.table_name,
                conditions=conditions,
                update_values=update_values,
            )

    def process_file(self, csv_path: str) -> None:
        """处理CSV文件"""
        try:
            df = pd.read_csv(csv_path)
            self._validate_dataframe(df)

            # 按命令类型分组处理
            for command_type in CommandType:
                df_cmd = df[df["command"].str.lower() == command_type.value]
                if not df_cmd.empty:
                    self._process_batch(df_cmd)

        except Exception as e:
            console.print(f"[red bold]Error processing file: {str(e)}[/red bold]")
            raise

    def _process_batch(self, df: pd.DataFrame) -> None:
        """批量处理数据"""
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

            # 获取受影响行的ID列表（处理大小写问题）
            id_column = next(col for col in df_before.columns if col.lower() == "id")
            affected_ids = df_before[id_column].tolist()

            # 执行操作
            affected_rows = self.db_manager.execute_operation(operation)

            # 使用ID查询更新后的数据
            id_list = ",".join(map(str, affected_ids))
            df_after = self.db_manager.fetch_data(
                f"SELECT * FROM {operation.table_name} WHERE id IN ({id_list})"
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

        # 确保两个DataFrame使用相同的索引
        id_column = "id"  # 统一使用小写
        df_before = df_before.set_index(id_column)
        df_after = df_after.set_index(id_column)

        # 对每一行进行比较
        for idx in df_after.index:
            if idx in df_before.index:
                row_before = df_before.loc[idx]
                row_after = df_after.loc[idx]

                # 比较每一列的值
                for col in df_after.columns:
                    # 转换为字符串进行比较，避免数据类型不一致的问题
                    before_val = str(row_before[col])
                    after_val = str(row_after[col])
                    if before_val != after_val:
                        changes.append(
                            f"Row {idx} - {col}: "
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
