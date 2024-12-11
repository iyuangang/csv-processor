# 开发指南

## 项目结构
```
csv-processor/
├── src/
│   ├── __init__.py
│   ├── processor.py
│   ├── database.py
│   └── models.py
├── tests/
│   ├── __init__.py
│   ├── test_integration.py
│   └── test_unit.py
├── docs/
│   ├── user_manual.md
│   └── configuration.md
└── main.py
```

## 开发环境设置

1. 克隆仓库
2. 创建虚拟环境
3. 安装开发依赖

## 测试

### 运行测试
```bash
python tests/run_tests.py
```

### 添加测试
1. 创建测试类
2. 实现测试方法
3. 运行测试套件

## 代码规范

- 遵循 PEP 8
- 添加类型注解
- 编写文档字符串

## 贡献指南

1. Fork 仓库
2. 创建特性分支
3. 提交变更
4. 创建 Pull Request 
