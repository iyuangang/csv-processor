import os

import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_test_data():
    """生成测试数据"""
    # 创建测试数据目录
    os.makedirs("tests/data", exist_ok=True)

    # 删除已存在的测试数据文件
    for file in ["employees.csv", "departments.csv"]:
        if os.path.exists(f"tests/data/{file}"):
            os.remove(f"tests/data/{file}")

    # 生成员工数据
    num_employees = 100
    employees_data = {
        "employee_id": range(1001, 1001 + num_employees),
        "name": [f"Employee{i}" for i in range(num_employees)],
        "dept": np.random.randint(1, 6, num_employees),
        "salary": np.random.randint(3000, 10000, num_employees),
        "hire_date": [
            (datetime.now() - timedelta(days=np.random.randint(0, 1000))).strftime(
                "%Y-%m-%d"
            )
            for _ in range(num_employees)
        ],
        "status": np.random.choice(["active", "inactive"], num_employees),
    }

    df_employees = pd.DataFrame(employees_data)
    df_employees.to_csv("tests/data/employees.csv", index=False)

    # 生成部门数据
    departments_data = {
        "id": [f"D00{i}" for i in range(1, 6)],
        "name": [f"Department{i}" for i in range(1, 6)],
        "manager": np.random.choice(range(1001, 1001 + num_employees), 5),
        "create_date": [
            (datetime.now() - timedelta(days=np.random.randint(0, 1000))).strftime(
                "%Y-%m-%d"
            )
            for _ in range(5)
        ],
        "status": ["active"] * 5,
    }

    df_departments = pd.DataFrame(departments_data)
    df_departments.to_csv("tests/data/departments.csv", index=False)


if __name__ == "__main__":
    generate_test_data()
