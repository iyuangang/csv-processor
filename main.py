import click
from rich.console import Console
from pathlib import Path
from src.config import ConfigManager
from src.database import DatabaseManager
from src.processor import DataProcessor
from src.yaml_processor import YAMLProcessor

console = Console()


@click.group()
def cli():
    """数据处理程序"""
    pass


@cli.command()
@click.option(
    "--env",
    type=click.Choice(["prod", "dev", "test"]),
    required=True,
    help="Environment to use",
)
@click.option(
    "--input-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    required=True,
    help="Path to input file (CSV or YAML)",
)
@click.option(
    "--config-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    default="config.json",
    help="Path to configuration file",
)
@click.option(
    "--preview/--no-preview",
    default=True,
    help="Preview changes before executing",
)
@click.option(
    "--auto-confirm/--no-auto-confirm",
    default=False,
    help="Automatically confirm all operations",
)
def process(
    env: str,
    input_file: str,
    config_file: str,
    preview: bool,
    auto_confirm: bool,
) -> None:
    """处理数据文件"""
    try:
        # 加载配置
        config_manager = ConfigManager(config_file)
        db_config = config_manager.get_database_config(env)
        processor_config = config_manager.get_processor_config()

        # 更新处理器配置
        processor_config.preview_enabled = preview
        processor_config.require_confirmation = not auto_confirm

        # 初始化数据库连接
        db_manager = DatabaseManager(db_config)
        console.log(f"[blue]Connected to {env} database[/blue]")

        try:
            # 初始化处理器
            processor = DataProcessor(
                db_manager,
                {
                    "processor": processor_config.__dict__,
                    "tables": config_manager.config.get("tables", {}),
                },
            )

            # 处理输入文件
            with db_manager.transaction():
                processor.process_file(input_file)
            console.log("[green]All operations completed successfully[/green]")

        finally:
            db_manager.close()
            console.log("[bold]Database connection closed[/bold]")

    except Exception as e:
        console.print(f"[red bold]Error: {str(e)}[/red bold]")
        raise click.Abort()


@cli.command()
@click.option(
    "--type",
    "template_type",
    type=click.Choice(["csv", "yaml"]),
    required=True,
    help="Template type to generate",
)
@click.option(
    "--output",
    type=click.Path(file_okay=True, dir_okay=False),
    help="Output path for template file",
)
def template(template_type: str, output: str) -> None:
    """生成模板文件"""
    try:
        if not output:
            output = f"template.{template_type}"

        if template_type == "csv":
            template_content = "table,command,conditions,new_values\n"
            template_content += "employees,update,employee_id=1001,salary=8000\n"
            template_content += "departments,delete,id=D001,\n"

            with open(output, "w", encoding="utf-8") as f:
                f.write(template_content)

        elif template_type == "yaml":
            template_content = """version: "1.0"
description: "Database operations batch file"
batches:
  - id: "BATCH_001"
    description: "Update employee salaries"
    operations:
      - table: "employees"
        command: "update"
        description: "Increase salary for employee 1001"
        conditions:
          employee_id: 1001
        new_values:
          salary: 8000
          
      - table: "employees"
        command: "update"
        description: "Update department for employee 1002"
        conditions:
          employee_id: 1002
        new_values:
          department_id: 3
"""
            with open(output, "w", encoding="utf-8") as f:
                f.write(template_content)

        console.print(f"[green]Template file generated: {output}[/green]")

    except Exception as e:
        console.print(f"[red bold]Error generating template: {str(e)}[/red bold]")
        raise click.Abort()


if __name__ == "__main__":
    cli()
