-- 创建测试表
CREATE TABLE test_table (
    id NUMBER PRIMARY KEY,
    name VARCHAR2(100),
    status VARCHAR2(20),
    department VARCHAR2(50),
    salary NUMBER,
    join_date DATE
);

-- 创建备份表
CREATE TABLE test_table_bak (
    id NUMBER,
    name VARCHAR2(100),
    status VARCHAR2(20),
    department VARCHAR2(50),
    salary NUMBER,
    join_date DATE,
    backup_time TIMESTAMP DEFAULT SYSTIMESTAMP
);

-- 创建测试数据序列
CREATE SEQUENCE test_table_seq START WITH 1 INCREMENT BY 1;
