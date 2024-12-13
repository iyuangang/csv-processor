# tests/test_yaml_processor.py
import unittest
import pandas as pd
import os
import yaml
from pathlib import Path
from src.config import ConfigManager
from src.database import DatabaseManager
from src.processor import DataProcessor
from src.yaml_processor import YAMLProcessor
from tests.generate_test_data import generate_test_data


class TestYAMLProcessor(unittest.TestCase):
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

        # 初始化YAML处理器
        cls.yaml_processor = YAMLProcessor(cls.processor)

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

    def _create_test_yaml(self, data: dict, filename: str) -> str:
        """创建测试YAML文件"""
        test_file = f"tests/data/{filename}"
        with open(test_file, "w", encoding="utf-8") as f:
            yaml.dump(data, f)
        return test_file

    def test_basic_yaml_update(self):
        """测试基本的YAML更新操作"""
        yaml_data = {
            "version": "1.0",
            "description": "Test basic update",
            "batches": [
                {
                    "id": "TEST_001",
                    "description": "Update employee salary",
                    "operations": [
                        {
                            "table": "employees",
                            "command": "update",
                            "conditions": {"employee_id": 1001},
                            "new_values": {"salary": 8000},
                        }
                    ],
                }
            ],
        }

        test_file = self._create_test_yaml(yaml_data, "test_basic_update.yaml")
        self.yaml_processor.process_yaml(test_file)

        # 验证结果
        result = self.db_manager.fetch_data(
            "SELECT salary FROM employees WHERE emp_id = 1001"
        )
        self.assertEqual(result.iloc[0]["salary"], 8000)

    def test_batch_operations(self):
        """测试批量操作"""
        yaml_data = {
            "version": "1.0",
            "description": "Test batch operations",
            "batches": [
                {
                    "id": "BATCH_001",
                    "description": "Multiple updates and deletes",
                    "operations": [
                        {
                            "table": "employees",
                            "command": "update",
                            "conditions": {"employee_id": 1001},
                            "new_values": {"salary": 8500},
                        },
                        {
                            "table": "employees",
                            "command": "update",
                            "conditions": {"employee_id": 1002},
                            "new_values": {"department_id": 3},
                        },
                        {
                            "table": "employees",
                            "command": "delete",
                            "conditions": {"employee_id": 1003},
                        },
                    ],
                }
            ],
        }

        test_file = self._create_test_yaml(yaml_data, "test_batch_ops.yaml")
        self.yaml_processor.process_yaml(test_file)

        # 验证更新结果
        salary_result = self.db_manager.fetch_data(
            "SELECT salary FROM employees WHERE emp_id = 1001"
        )
        dept_result = self.db_manager.fetch_data(
            "SELECT department_id FROM employees WHERE emp_id = 1002"
        )
        delete_result = self.db_manager.fetch_data(
            "SELECT * FROM employees WHERE emp_id = 1003"
        )

        self.assertEqual(salary_result.iloc[0]["salary"], 8500)
        self.assertEqual(dept_result.iloc[0]["department_id"], 3)
        self.assertTrue(delete_result.empty)

    def test_invalid_yaml_format(self):
        """测试无效的YAML格式"""
        # 缺少必要字段
        invalid_yaml = {"description": "Missing version"}

        test_file = self._create_test_yaml(invalid_yaml, "test_invalid.yaml")
        with self.assertRaises(ValueError):
            self.yaml_processor.process_yaml(test_file)

    def test_multiple_batches(self):
        """测试多个批次操作"""
        yaml_data = {
            "version": "1.0",
            "description": "Test multiple batches",
            "batches": [
                {
                    "id": "BATCH_001",
                    "description": "Update employees",
                    "operations": [
                        {
                            "table": "employees",
                            "command": "update",
                            "conditions": {"employee_id": 1001},
                            "new_values": {"salary": 9000},
                        }
                    ],
                },
                {
                    "id": "BATCH_002",
                    "description": "Update departments",
                    "operations": [
                        {
                            "table": "departments",
                            "command": "update",
                            "conditions": {"id": "D001"},
                            "new_values": {"manager_id": 1002},
                        }
                    ],
                },
            ],
        }

        test_file = self._create_test_yaml(yaml_data, "test_multi_batch.yaml")
        self.yaml_processor.process_yaml(test_file)

        # 验证结果
        emp_result = self.db_manager.fetch_data(
            "SELECT salary FROM employees WHERE emp_id = 1001"
        )
        dept_result = self.db_manager.fetch_data(
            "SELECT manager_id FROM departments WHERE dept_id = 'D001'"
        )

        self.assertEqual(emp_result.iloc[0]["salary"], 9000)
        self.assertEqual(dept_result.iloc[0]["manager_id"], 1002)

    def test_yaml_validation(self):
        """测试YAML验证功能"""
        # 测试无效的表名
        invalid_table_yaml = {
            "version": "1.0",
            "description": "Test invalid table",
            "batches": [
                {
                    "id": "TEST_001",
                    "operations": [
                        {
                            "table": "invalid_table",
                            "command": "update",
                            "conditions": {"id": 1},
                            "new_values": {"value": "test"},
                        }
                    ],
                }
            ],
        }

        test_file = self._create_test_yaml(
            invalid_table_yaml, "test_invalid_table.yaml"
        )
        with self.assertRaises(ValueError):
            self.yaml_processor.process_yaml(test_file)

        # 测试无效的命令
        invalid_command_yaml = {
            "version": "1.0",
            "description": "Test invalid command",
            "batches": [
                {
                    "id": "TEST_001",
                    "operations": [
                        {
                            "table": "employees",
                            "command": "invalid_command",
                            "conditions": {"employee_id": 1001},
                        }
                    ],
                }
            ],
        }

        test_file = self._create_test_yaml(
            invalid_command_yaml, "test_invalid_command.yaml"
        )
        with self.assertRaises(ValueError):
            self.yaml_processor.process_yaml(test_file)

    @classmethod
    def tearDownClass(cls):
        """测试类清理"""
        cls.db_manager.close()
        # 清理测试YAML文件
        for file in [
            "test_basic_update.yaml",
            "test_batch_ops.yaml",
            "test_invalid.yaml",
            "test_multi_batch.yaml",
            "test_invalid_table.yaml",
            "test_invalid_command.yaml",
        ]:
            file_path = Path(f"tests/data/{file}")
            if file_path.exists():
                file_path.unlink()


if __name__ == "__main__":
    unittest.main()
