import pandas as pd
import random
from datetime import datetime, timedelta

def generate_test_table_data(num_records: int = 100) -> pd.DataFrame:
    """生成测试表数据"""
    data = {
        'id': range(1, num_records + 1),
        'name': [f'User{i}' for i in range(1, num_records + 1)],
        'status': random.choices(['active', 'inactive', 'pending'], k=num_records),
        'department': random.choices(['IT', 'HR', 'Sales', 'Marketing'], k=num_records),
        'salary': [random.randint(3000, 10000) for _ in range(num_records)],
        'join_date': [(datetime.now() - timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d') 
                     for _ in range(num_records)]
    }
    return pd.DataFrame(data)

def generate_test_commands() -> pd.DataFrame:
    """生成测试命令CSV"""
    # 删除命令数据
    delete_data = [
        # 单条件删除
        {'id': 1, 'name': None, 'status': None, 'department': None, 'salary': None, 'join_date': None, 'command': 'delete'},
        # 多条件删除
        {'id': None, 'name': None, 'status': 'inactive', 'department': 'IT', 'salary': None, 'join_date': None, 'command': 'delete'},
        # 批量删除
        {'id': None, 'name': None, 'status': None, 'department': 'HR', 'salary': None, 'join_date': None, 'command': 'delete'},
    ]

    # 更新命令数据
    update_data = [
        # 单字段更新
        {'id': 5, 'name': None, 'status': None, 'department': None, 'salary': None, 'join_date': None, 
         'command': 'update', 'new_salary': 8000},
        # 多字段更新
        {'id': None, 'name': None, 'status': 'active', 'department': 'Sales', 'salary': None, 'join_date': None,
         'command': 'update', 'new_status': 'inactive', 'new_salary': 7000},
        # 日期更新
        {'id': 10, 'name': None, 'status': None, 'department': None, 'salary': None, 'join_date': None,
         'command': 'update', 'new_join_date': '2023-01-01'}
    ]

    return pd.DataFrame(delete_data + update_data)

def main():
    """生成测试数据"""
    # 生成测试表数据
    test_data = generate_test_table_data()
    test_data.to_csv('tests/test_table_data.csv', index=False)
    print("Generated test table data:")
    print(test_data.head())
    print("\nTotal records:", len(test_data))

    # 生成测试命令
    test_commands = generate_test_commands()
    test_commands.to_csv('tests/test_commands.csv', index=False)
    print("\nGenerated test commands:")
    print(test_commands)

if __name__ == "__main__":
    main() 
