# CSV Data Processor 用户手册

## 目录

1. [简介](#简介)
2. [安装](#安装)
3. [配置](#配置)
4. [使用方法](#使用方法)
5. [操作流程](#操作流程)
6. [注意事项](#注意事项)
7. [故障排除](#故障排除)
8. [常见问题](#常见问题)

## 简介

CSV Data Processor 是一个用于批量处理数据库操作的工具，支持多表操作、数据备份和操作验证。它可以通过 CSV 文件定义要执行的数据库操作，支持 DELETE 和 UPDATE 操作。

## 安装

### 系统要求

- Python 3.8+
- Oracle 客户端

### 安装步骤

1. 克隆仓库：
   ```bash
   git clone https://github.com/yourusername/csv-processor.git
   cd csv-processor
   ```

2. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

3. 配置数据库连接：
   - 创建 `config.json` 文件
   - 配置数据库连接信息

详细步骤见 [安装指南](installation.md)

## 配置

### 配置文件结构

配置文件用于定义数据库连接、表结构和处理器选项。

#### 数据库配置

```json
{
    "databases": {
        "环境名": {
            "username": "用户名",
            "password": "密码",
            "host": "主机地址",
            "port": "端口",
            "service_name": "服务名"
        }
    }
}
```

#### 表配置

```json
{
    "tables": {
        "表名": {
            "primary_key": "主键列名",
            "date_columns": ["日期列1", "日期列2"],
            "number_columns": ["数字列1", "数字列2"],
            "backup_enabled": true,
            "columns_mapping": {
                "CSV列名": "数据库列名"
            }
        }
    }
}
```

详细配置说明见 [配置指南](configuration.md)

## 使用方法

### 命令行参数

- `--env`: 指定要使用的环境（必需）
- `--csv-file`: 指定要处理的 CSV 文件路径（必需）
- `--config`: 指定配置文件路径（可选，默认为 `config.json`）

### 示例

```bash
python main.py --env dev --csv-file operations.csv
```

### CSV 文件格式

CSV 文件必须包含以下列：
- `table`: 要操作的表名
- `command`: 操作类型（DELETE 或 UPDATE）
- 条件列：用于指定要操作的记录
- 更新列：对于 UPDATE 操作，以 `new_` 开头的列表示要更新的值

详细格式说明见 [CSV格式说明](csv_format.md)

## 操作流程

1. **准备数据**
   - 创建 CSV 文件
   - 检查数据格式

2. **执行操作**
   - 运行程序
   - 确认操作
   - 查看结果

3. **验证结果**
   - 检查更新后的数据
   - 查看操作日志

## 注意事项

### 数据备份

- 每个操作前会自动备份数据
- 备份表名格式为：原表名_bak
- 备份包含时间戳信息

### 安全建议

- 确保数据库用户权限正确
- 在测试环境中验证操作
- 定期检查备份数据

## 故障排除

### 常见问题

1. **连接错误**
   - 检查数据库连接信息
   - 确保网络连接正常

2. **数据格式问题**
   - 确保 CSV 文件格式正确
   - 检查日期和数字格式

3. **权限问题**
   - 确保数据库用户有足够的权限
   - 检查表和列的访问权限

### 解决方案

- 查看详细的错误信息
- 检查日志文件
- 联系技术支持

## 常见问题

### 如何添加新表？

1. 在配置文件中添加表配置
2. 指定主键和列映射
3. 设置数据类型信息

### 如何处理大数据量？

- 增加 `batch_size` 配置项
- 使用更高效的数据库连接池

### 如何确保数据安全？

- 启用数据备份
- 定期检查备份数据
- 使用事务管理

### 如何调试程序？

- 使用 `--debug` 参数查看详细日志
- 检查数��库日志
- 使用测试用例验证功能

## 特殊更新语法

### 文本追加功能

在更新操作中，可以使用特殊的语法来追加文本到现有值：

1. **追加固定文本**
   - 语法：`+TEXT`
   - 示例：`new_name: +_DELETED` 会在原name值后追加 "_DELETED"

2. **追加特殊字符**
   - 语法：`+字符`
   - 示例：`new_status: +*` 会在原status值后追加 "*"

### 示例

```csv
table,id,command,new_name,new_status
employees,1001,update,+_INACTIVE,+*
departments,D001,update,+_OLD,+_ARCHIVED
```

这将产生以下效果：
- 员工1001的name会变成 "原名_INACTIVE"，status会变成 "原状态*"
- 部门D001的name会变成 "原名_OLD"，status会变成 "原状态_ARCHIVED"

### 注意事项

1. 追加功能只适用于文本类型的列
2. 如果只写 "+" 而没有后续文本，该字段将保持不变
3. 数字和日期类型的列不支持追加功能

## 联系我们

如有任何问题或建议，请通过以下方式联系我们：
- 邮箱：support@example.com
- 电话：+123-456-7890 
