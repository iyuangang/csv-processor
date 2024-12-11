# CSV 格式说明

## 基本格式

CSV 文件必须包含以下必需列：
- table: 要操作的表名
- command: 操作类型（DELETE/UPDATE）

## 示例

### 单表操作
```csv
table,employee_id,command,new_salary
employees,1001,update,8000
employees,1002,delete,
```

### 多表操作
```csv
table,id,command,new_manager
departments,D001,update,1001
table,employee_id,command,new_salary
employees,1001,update,8000
```

## 列说明

### 必需列
- table: 表名
- command: 操作类型

### 条件列
- 用于指定要操作的记录
- 可以使用多个条件列

### 更新列
- 以 new_ 开头
- 指定要更新的值 
