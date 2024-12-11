-- 创建员工表
CREATE TABLE employees (
    emp_id NUMBER PRIMARY KEY,
    emp_name VARCHAR2(100),
    department_id NUMBER,
    salary NUMBER(10,2),
    hire_date DATE,
    birth_date DATE,
    status VARCHAR2(20)
);

-- 创建部门表
CREATE TABLE departments (
    dept_id VARCHAR2(10) PRIMARY KEY,
    dept_name VARCHAR2(100),
    manager_id NUMBER,
    create_date DATE,
    status VARCHAR2(20)
);

-- 创建备份表
CREATE TABLE employees_bak (
    emp_id NUMBER,
    emp_name VARCHAR2(100),
    department_id NUMBER,
    salary NUMBER(10,2),
    hire_date DATE,
    birth_date DATE,
    status VARCHAR2(20),
    backup_time TIMESTAMP
);

CREATE TABLE departments_bak (
    dept_id VARCHAR2(10),
    dept_name VARCHAR2(100),
    manager_id NUMBER,
    create_date DATE,
    status VARCHAR2(20),
    backup_time TIMESTAMP
);
