# CSV Data Processor

一个用于通过 CSV 文件批量处理数据库操作的工具。支持多表操作、数据备份和操作验证。

## 功能特点

- 支持多表操作
- 支持 DELETE 和 UPDATE 操作
- 自动数据备份
- 操作预览和确认
- 结果验证和展示
- 批量处理优化
- 灵活的配置系统

## 快速开始

1. 克隆仓库：
```bash
git clone https://github.com/iyuangang/csv-processor.git
cd csv-processor
```

2. 安装依赖：
```bash
pip install -r requirements.txt
```

3. 创建配置文件：
```bash
cp config.example.json config.json
# 编辑 config.json 配置数据库连接信息
```

4. 运行程序：
```bash
python main.py --env dev --csv-file operations.csv
```

## 文档

- [用户手册](docs/user_manual.md)
- [配置指南](docs/configuration.md)
- [CSV格式说明](docs/csv_format.md)
- [开发指南](docs/development.md)

## 开发

运行测试：
```bash
python tests/run_tests.py
```

## 贡献

欢迎提交 Pull Request 或创建 Issue。

## 许可证

MIT License
