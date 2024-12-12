import unittest
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.config import ConfigManager
from src.database import DatabaseManager
from src.processor import DataProcessor
from src.models import CommandType, SQLOperation
from tests.generate_test_data import generate_test_data


class TestIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """测试类初始化"""
        # 加载测试配置
        cls.config_manager = ConfigManager("config.json")
        cls.db_config = cls.config_manager.get_database_config("dev")
        cls.db_manager = DatabaseManager(cls.db_config)

        # 生成测试数据
        generate_test_data()

        # 初始化数据处理器
        processor_config = cls.config_manager.config.get("processor", {})
        tables_config = cls.config_manager.config.get("tables", {})
        cls.processor = DataProcessor(
            cls.db_manager, {"processor": processor_config, "tables": tables_config}
        )

    def setUp(self):
        """每个测试用例前执行"""
        # 清空并重导入测试数据
        self._reset_test_data()

    def _reset_test_data(self):
        """重置测试数据"""
        # 读取测试数据
        df_employees = pd.read_csv("tests/data/employees.csv")
        df_departments = pd.read_csv("tests/data/departments.csv")

        # 清空表
        with self.db_manager.connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE employees")
            cursor.execute("TRUNCATE TABLE departments")
            cursor.execute("TRUNCATE TABLE employees_bak")
            cursor.execute("TRUNCATE TABLE departments_bak")

            # 插入员工数据
            for _, row in df_employees.iterrows():
                cursor.execute(
                    """
                    INSERT INTO employees (emp_id, emp_name, department_id, salary, 
                                        hire_date, status)
                    VALUES (:1, :2, :3, :4, TO_DATE(:5, 'YYYY-MM-DD'), :6)
                """,
                    (
                        row["employee_id"],
                        row["name"],
                        row["dept"],
                        row["salary"],
                        row["hire_date"],
                        row["status"],
                    ),
                )

            # 插入部门数据
            for _, row in df_departments.iterrows():
                cursor.execute(
                    """
                    INSERT INTO departments (dept_id, dept_name, manager_id, 
                                          create_date, status)
                    VALUES (:1, :2, :3, TO_DATE(:4, 'YYYY-MM-DD'), :5)
                """,
                    (
                        row["id"],
                        row["name"],
                        row["manager"],
                        row["create_date"],
                        row["status"],
                    ),
                )

        self.db_manager.connection.commit()

    def test_update_employee_salary(self):
        """测试更新员工工资"""
        # 准备测试数据
        test_data = pd.DataFrame(
            [
                {
                    "table": "employees",
                    "employee_id": 1001,
                    "command": "update",
                    "new_salary": 8000,
                }
            ]
        )
        test_csv = "tests/data/test_update.csv"
        test_data.to_csv(test_csv, index=False)

        # 执行更新
        self.processor.process_file(test_csv)

        # 验证结果
        result = self.db_manager.fetch_data(
            "SELECT salary FROM employees WHERE emp_id = 1001"
        )
        self.assertEqual(result.iloc[0]["salary"], 8000)

        # 验证备份
        backup = self.db_manager.fetch_data(
            "SELECT * FROM employees_bak WHERE emp_id = 1001"
        )
        self.assertFalse(backup.empty)

    def test_delete_employee(self):
        """测试删除员工"""
        # 准备测试数据
        test_data = pd.DataFrame(
            [{"table": "employees", "employee_id": 1001, "command": "delete"}]
        )
        test_csv = "tests/data/test_delete.csv"
        test_data.to_csv(test_csv, index=False)

        # 执行删除
        self.processor.process_file(test_csv)

        # 验证结果
        result = self.db_manager.fetch_data(
            "SELECT * FROM employees WHERE emp_id = 1001"
        )
        self.assertTrue(result.empty)

    def test_batch_update(self):
        """测试批量更新"""
        # 准备测试数据
        test_data = pd.DataFrame(
            [
                {
                    "table": "employees",
                    "employee_id": 1001,
                    "command": "update",
                    "new_salary": 8000,
                },
                {
                    "table": "departments",
                    "id": "D001",
                    "command": "update",
                    "new_manager": 1002,
                },
            ]
        )
        test_csv = "tests/data/test_batch.csv"
        test_data.to_csv(test_csv, index=False)

        # 执行批量更新
        self.processor.process_file(test_csv)

        # 验证结果
        emp_result = self.db_manager.fetch_data(
            "SELECT salary FROM employees WHERE emp_id = 1001"
        )
        dept_result = self.db_manager.fetch_data(
            "SELECT manager_id FROM departments WHERE dept_id = 'D001'"
        )

        self.assertEqual(emp_result.iloc[0]["salary"], 8000)
        self.assertEqual(dept_result.iloc[0]["manager_id"], 1002)

    def test_update_multiple_fields(self):
        """测试更新多个字段"""
        test_data = pd.DataFrame(
            [
                {
                    "table": "employees",
                    "employee_id": 1001,
                    "command": "update",
                    "new_salary": 8500,
                    "new_status": "inactive",
                    "new_department_id": 3,
                }
            ]
        )
        test_csv = "tests/data/test_multi_update.csv"
        test_data.to_csv(test_csv, index=False)

        self.processor.process_file(test_csv)

        result = self.db_manager.fetch_data("""
            SELECT salary, status, department_id 
            FROM employees WHERE emp_id = 1001
        """)
        self.assertEqual(result.iloc[0]["salary"], 8500)
        self.assertEqual(result.iloc[0]["status"], "inactive")
        self.assertEqual(result.iloc[0]["department_id"], 3)

    def test_delete_by_condition(self):
        """测试按条件删除"""
        test_data = pd.DataFrame(
            [
                {
                    "table": "employees",
                    "status": "inactive",
                    "department_id": 2,
                    "command": "delete",
                }
            ]
        )
        test_csv = "tests/data/test_condition_delete.csv"
        test_data.to_csv(test_csv, index=False)

        # 先更新一些员工状态
        self.db_manager.execute_operation(
            SQLOperation(
                command_type=CommandType.UPDATE,
                table_name="employees",
                conditions={"department_id": 2},
                update_values={"status": "inactive"},
                table_config=self.processor.tables_config["employees"],
            )
        )

        initial_count = self.db_manager.fetch_data(
            "SELECT COUNT(*) as cnt FROM employees WHERE status = 'inactive' AND department_id = 2"
        ).iloc[0]["cnt"]

        self.processor.process_file(test_csv)

        final_count = self.db_manager.fetch_data(
            "SELECT COUNT(*) as cnt FROM employees WHERE status = 'inactive' AND department_id = 2"
        ).iloc[0]["cnt"]

        self.assertEqual(final_count, 0)
        self.assertGreater(initial_count, 0)

    def test_update_date_field(self):
        """测试更新日期字段"""
        new_date = "2024-01-01"
        test_data = pd.DataFrame(
            [
                {
                    "table": "employees",
                    "employee_id": 1001,
                    "command": "update",
                    "new_hire_date": new_date,
                }
            ]
        )
        test_csv = "tests/data/test_date_update.csv"
        test_data.to_csv(test_csv, index=False)

        self.processor.process_file(test_csv)

        result = self.db_manager.fetch_data("""
            SELECT TO_CHAR(hire_date, 'YYYY-MM-DD') as hire_date 
            FROM employees WHERE emp_id = 1001
        """)
        self.assertEqual(result.iloc[0]["hire_date"], new_date)

    def test_batch_delete(self):
        """测试批量删除"""
        test_data = pd.DataFrame(
            [
                {"table": "employees", "employee_id": 1001, "command": "delete"},
                {"table": "employees", "employee_id": 1002, "command": "delete"},
                {"table": "employees", "employee_id": 1003, "command": "delete"},
            ]
        )
        test_csv = "tests/data/test_batch_delete.csv"
        test_data.to_csv(test_csv, index=False)

        # 记录SQL执行
        executed_sql = []
        original_execute = self.db_manager.execute_operation

        def mock_execute(operation):
            executed_sql.append(operation.get_sql())
            return original_execute(operation)

        self.db_manager.execute_operation = mock_execute

        self.processor.process_file(test_csv)

        # 验证只执行了一条SQL
        self.assertEqual(len(executed_sql), 1)
        expected_sql = "DELETE FROM employees WHERE emp_id IN (1001,1002,1003)"
        self.assertEqual(executed_sql[0], expected_sql)

        # 验证结果
        result = self.db_manager.fetch_data(
            "SELECT COUNT(*) as cnt FROM employees WHERE emp_id IN (1001, 1002, 1003)"
        )
        self.assertEqual(result.iloc[0]["cnt"], 0)

    def test_mixed_operations(self):
        """测试混合操作"""
        test_data = pd.DataFrame(
            [
                {
                    "table": "employees",
                    "employee_id": 1001,
                    "command": "update",
                    "new_salary": 9000,
                },
                {"table": "employees", "employee_id": 1002, "command": "delete"},
                {
                    "table": "departments",
                    "id": "D001",
                    "command": "update",
                    "new_manager": 1003,
                },
            ]
        )
        test_csv = "tests/data/test_mixed_ops.csv"
        test_data.to_csv(test_csv, index=False)

        self.processor.process_file(test_csv)

        # 验证更新
        emp_result = self.db_manager.fetch_data(
            "SELECT salary FROM employees WHERE emp_id = 1001"
        )
        self.assertEqual(emp_result.iloc[0]["salary"], 9000)

        # 验证删除
        emp_deleted = self.db_manager.fetch_data(
            "SELECT * FROM employees WHERE emp_id = 1002"
        )
        self.assertTrue(emp_deleted.empty)

        # 验证部门更新
        dept_result = self.db_manager.fetch_data(
            "SELECT manager_id FROM departments WHERE dept_id = 'D001'"
        )
        self.assertEqual(dept_result.iloc[0]["manager_id"], 1003)

    def test_invalid_data(self):
        """测试无效数据"""
        # 测试不存在的表
        with self.assertRaises(ValueError):
            test_data = pd.DataFrame(
                [
                    {
                        "table": "invalid_table",
                        "id": 1,
                        "command": "update",
                        "new_value": "test",
                    }
                ]
            )
            test_csv = "tests/data/test_invalid_table.csv"
            test_data.to_csv(test_csv, index=False)
            self.processor.process_file(test_csv)

        # 测试不存在的命令
        with self.assertRaises(ValueError):
            test_data = pd.DataFrame(
                [
                    {
                        "table": "employees",
                        "employee_id": 1001,
                        "command": "invalid_command",
                    }
                ]
            )
            test_csv = "tests/data/test_invalid_command.csv"
            test_data.to_csv(test_csv, index=False)
            self.processor.process_file(test_csv)

    def test_backup_functionality(self):
        """测试备份功能"""
        test_data = pd.DataFrame(
            [
                {
                    "table": "employees",
                    "employee_id": 1001,
                    "command": "update",
                    "new_salary": 8000,
                }
            ]
        )
        test_csv = "tests/data/test_backup.csv"
        test_data.to_csv(test_csv, index=False)

        # 获取原始数据
        original_data = self.db_manager.fetch_data(
            "SELECT * FROM employees WHERE emp_id = 1001"
        )

        self.processor.process_file(test_csv)

        # 验证备份数据
        backup_data = self.db_manager.fetch_data("""
            SELECT emp_id, emp_name, salary 
            FROM employees_bak 
            WHERE emp_id = 1001 
            ORDER BY backup_time DESC
        """)

        self.assertFalse(backup_data.empty)
        self.assertEqual(backup_data.iloc[0]["salary"], original_data.iloc[0]["salary"])

    def test_append_text(self):
        """测试文本追加功能"""
        # 准备测试数据
        test_data = pd.DataFrame(
            [
                {
                    "table": "employees",
                    "employee_id": 1001,
                    "command": "update",
                    "new_emp_name": "+_INACTIVE",
                    "new_status": "+*",
                },
                {
                    "table": "departments",
                    "id": "D001",
                    "command": "update",
                    "new_dept_name": "+_ARCHIVED",
                },
            ]
        )
        test_csv = "tests/data/test_append.csv"
        test_data.to_csv(test_csv, index=False)

        # 获取原始数据
        emp_before = self.db_manager.fetch_data(
            "SELECT emp_name, status FROM employees WHERE emp_id = 1001"
        )
        dept_before = self.db_manager.fetch_data(
            "SELECT dept_name FROM departments WHERE dept_id = 'D001'"
        )

        # 执行更新
        self.processor.process_file(test_csv)

        # 验证结果
        emp_after = self.db_manager.fetch_data(
            "SELECT emp_name, status FROM employees WHERE emp_id = 1001"
        )
        dept_after = self.db_manager.fetch_data(
            "SELECT dept_name FROM departments WHERE dept_id = 'D001'"
        )

        # 验证文本追加
        self.assertEqual(
            emp_after.iloc[0]["emp_name"], emp_before.iloc[0]["emp_name"] + "_INACTIVE"
        )
        self.assertEqual(
            emp_after.iloc[0]["status"], emp_before.iloc[0]["status"] + "*"
        )
        self.assertEqual(
            dept_after.iloc[0]["dept_name"],
            dept_before.iloc[0]["dept_name"] + "_ARCHIVED",
        )

    @classmethod
    def tearDownClass(cls):
        """测试类清理"""
        cls.db_manager.close()
        # 清理测试CSV文件
        for file in [
            "test_update.csv",
            "test_delete.csv",
            "test_batch.csv",
            "test_multi_update.csv",
            "test_condition_delete.csv",
            "test_date_update.csv",
            "test_batch_delete.csv",
            "test_mixed_ops.csv",
            "test_invalid_table.csv",
            "test_invalid_command.csv",
            "test_backup.csv",
        ]:
            if os.path.exists(f"tests/data/{file}"):
                os.remove(f"tests/data/{file}")


if __name__ == "__main__":
    unittest.main()
