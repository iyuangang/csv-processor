# 配置指南

## 配置文件结构

### 数据库配置
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

### 表配置
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

## 详细说明

### 数据库配置项
- username: 数据库用户名
- password: 数据库密码
- host: 数据库主机地址
- port: 数据库端口
- service_name: Oracle服务名

### 表配置项
- primary_key: 表的主键列名
- date_columns: 日期类型的列名列表
- number_columns: 数字类型的列名列表
- backup_enabled: 是否启用备份
- columns_mapping: CSV列名到数据库列名的映射 
