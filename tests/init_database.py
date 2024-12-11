import cx_Oracle
import pandas as pd
import json


def init_test_database(config_file: str = "config.json", env: str = "dev"):
    """初始化测试数据库"""
    # 读取配置
    with open(config_file, "r", encoding="utf-8") as f:
        config = json.load(f)
    db_config = config["databases"][env]

    # 连接数据库
    dsn = cx_Oracle.makedsn(
        db_config["host"], db_config["port"], service_name=db_config["service_name"]
    )
    connection = cx_Oracle.connect(db_config["username"], db_config["password"], dsn)

    try:
        cursor = connection.cursor()

        # 执行建表脚本，使用 utf-8 编码读取
        with open("tests/create_test_tables.sql", "r", encoding="utf-8") as f:
            sql_commands = f.read().split(";")
            for sql in sql_commands:
                if sql.strip():
                    try:
                        cursor.execute(sql)
                    except cx_Oracle.DatabaseError as e:
                        # 如果表已存在，继续执行
                        if "ORA-00955" not in str(
                            e
                        ):  # ORA-00955: name is already used by an existing object
                            raise

        # 导入测试数据
        test_data = pd.read_csv("tests/test_table_data.csv")

        # 清空现有数据
        cursor.execute("TRUNCATE TABLE test_table")

        # 插入新数据
        for _, row in test_data.iterrows():
            cursor.execute(
                """
                INSERT INTO test_table (id, name, status, department, salary, join_date)
                VALUES (:1, :2, :3, :4, :5, TO_DATE(:6, 'YYYY-MM-DD'))
            """,
                (
                    row["id"],
                    row["name"],
                    row["status"],
                    row["department"],
                    row["salary"],
                    row["join_date"],
                ),
            )

        connection.commit()
        print("Test database initialized successfully")

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    init_test_database()
