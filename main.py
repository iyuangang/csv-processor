import click
from rich.console import Console
from src.config import ConfigManager
from src.database import DatabaseManager
from src.processor import DataProcessor

console = Console()


@click.command()
@click.option(
    "--env",
    type=click.Choice(["prod", "dev", "test"]),
    required=True,
    help="Environment to use",
)
@click.option(
    "--csv-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    required=True,
    help="Path to CSV file",
)
@click.option(
    "--config-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    default="config.json",
    help="Path to configuration file",
)
def main(env: str, csv_file: str, config_file: str) -> None:
    """CSV处理程序入口"""
    try:
        # 加载配置
        config_manager = ConfigManager(config_file)
        db_config = config_manager.get_database_config(env)

        # 初始化数据库连接
        db_manager = DatabaseManager(db_config)
        console.log(f"[blue]Connected to {env} database[/blue]")

        try:
            # 处理数据
            processor = DataProcessor(db_manager)
            with db_manager.transaction():
                processor.process_file(csv_file)
            console.log("[green]All operations completed successfully[/green]")

        finally:
            db_manager.close()
            console.log("[bold]Database connection closed[/bold]")

    except Exception as e:
        console.print(f"[red bold]Error: {str(e)}[/red bold]")
        raise click.Abort()


if __name__ == "__main__":
    main()
