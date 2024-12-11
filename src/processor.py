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

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def process_file(self, csv_path: str) -> None:
        """处理CSV文件"""
        try:
            df = pd.read_csv(csv_path)
            self._validate_dataframe(df)
            
            # 处理删除命令
            df_delete = df[df['command'] == CommandType.DELETE.value]
            if not df_delete.empty:
                self._process_delete_commands(df_delete)

            # 处理更新命令
            df_update = df[df['command'] == CommandType.UPDATE.value]
            if not df_update.empty:
                self._process_update_commands(df_update)

        except pd.errors.EmptyDataError:
            raise ValueError("CSV file is empty")
        except pd.errors.ParserError:
            raise ValueError("Invalid CSV format")

    def _validate_dataframe(self, df: pd.DataFrame) -> None:
        """验证DataFrame格式"""
        if 'command' not in df.columns:
            raise ValueError("CSV must contain 'command' column")
        
        invalid_commands = set(df['command']) - {cmd.value for cmd in CommandType}
        if invalid_commands:
            raise ValueError(f"Invalid commands found: {invalid_commands}")

    def _process_delete_commands(self, df_delete: pd.DataFrame) -> None:
        """处理删除命令"""
        condition_columns = [col for col in df_delete.columns if col != 'command']
        grouped_conditions = self._group_conditions(df_delete, condition_columns)

        for conditions in grouped_conditions:
            operation = self._prepare_delete_operation(conditions)
            self._execute_operation(operation)

    def _process_update_commands(self, df_update: pd.DataFrame) -> None:
        """处理更新命令"""
        columns = [col for col in df_update.columns if col != 'command']
        command_index = list(df_update.columns).index('command')
        condition_columns = [col for col in columns[:command_index] if not col.startswith('new_')]
        update_columns = [col for col in columns[command_index:] if col.startswith('new_')]

        for _, row in df_update.iterrows():
            operation = self._prepare_update_operation(row, condition_columns, update_columns)
            self._execute_operation(operation)

    def _group_conditions(self, df: pd.DataFrame, columns: List[str]) -> List[Dict[str, Set]]:
        """分组条件"""
        grouped = {}
        for _, row in df.iterrows():
            key = tuple(col for col in columns if pd.notna(row[col]))
            if key not in grouped:
                grouped[key] = {'columns': [], 'values': {}}
            
            for col in key:
                if col not in grouped[key]['columns']:
                    grouped[key]['columns'].append(col)
                if col not in grouped[key]['values']:
                    grouped[key]['values'][col] = set()
                grouped[key]['values'][col].add(row[col])
        
        return list(grouped.values())

    def _prepare_delete_operation(self, conditions: Dict) -> SQLOperation:
        """准备删除操作"""
        in_conditions = []
        for col in conditions['columns']:
            values = conditions['values'][col]
            in_clause = self._generate_in_clause(values)
            in_conditions.append(f"{col} IN {in_clause}")
        
        where_clause = " AND ".join(in_conditions)
        return SQLOperation(
            sql=f"DELETE FROM test_table WHERE {where_clause}",
            conditions=where_clause
        )

    def _prepare_update_operation(self, row: pd.Series, 
                                condition_cols: List[str], 
                                update_cols: List[str]) -> SQLOperation:
        """准备更新操作"""
        conditions = " AND ".join([
            f"{col} = '{row[col]}'" 
            for col in condition_cols 
            if pd.notna(row[col])
        ])
        
        # 处理更新值，对日期类型特殊处理
        updates = []
        for col in update_cols:
            if pd.notna(row[col]) and col.startswith('new_'):
                actual_col = col.replace('new_', '')
                value = row[col]
                
                # 日期类型特殊处理
                if 'date' in actual_col.lower():
                    updates.append(f"{actual_col} = TO_DATE('{value}', 'YYYY-MM-DD')")
                else:
                    updates.append(f"{actual_col} = '{value}'")
        
        updates_sql = ", ".join(updates)
        
        return SQLOperation(
            sql=f"UPDATE test_table SET {updates_sql} WHERE {conditions}",
            conditions=conditions
        )

    def _execute_operation(self, operation: SQLOperation) -> None:
        """执行操作"""
        # 显示将要操作的数据
        df_affected = self.db_manager.fetch_data(
            f"SELECT * FROM test_table WHERE {operation.conditions}"
        )
        
        if df_affected.empty:
            console.print(f"[yellow]No matching data found for conditions:[/yellow]")
            console.print(Panel(
                operation.conditions,
                title="[bold yellow]Unmatched Conditions[/bold yellow]"
            ))
            return

        # 显示数据和SQL
        self._display_operation_info(df_affected, operation)

        # 用户确认
        if not click.confirm("Do you want to proceed with this operation?"):
            console.print("[yellow]Operation cancelled by user[/yellow]")
            return

        # 执行操作
        affected_rows = self.db_manager.execute_operation(operation)
        console.print(f"[green]Successfully affected {affected_rows} rows[/green]")

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
        console.print(Panel(
            operation.sql, 
            title="[bold yellow]SQL to execute[/bold yellow]"
        ))
