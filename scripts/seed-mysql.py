import csv
from datetime import datetime
from pathlib import Path

data = Path(__file__).resolve().parents[1] / "data"


def rows(filename):
    with (data / filename).open(newline="", encoding="utf-8") as stream:
        yield from csv.DictReader(stream)


def literal(value):
    return "NULL" if value in ("", r"\000") else "'" + value.replace("'", "''") + "'"


print("""
CREATE DATABASE IF NOT EXISTS sau_pro3 CHARACTER SET utf8mb4;
CREATE USER IF NOT EXISTS 'pro3'@'127.0.0.1' IDENTIFIED BY 'pro3-local';
GRANT SELECT ON sau_pro3.* TO 'pro3'@'127.0.0.1';
USE sau_pro3;
CREATE TABLE IF NOT EXISTS dept (
    deptno INT PRIMARY KEY, dname VARCHAR(32) NOT NULL, loc VARCHAR(32) NOT NULL
);
CREATE TABLE IF NOT EXISTS emp (
    empno INT PRIMARY KEY, ename VARCHAR(32) NOT NULL, job VARCHAR(32) NOT NULL,
    mgr INT NULL, hiredate DATE NOT NULL, sal DECIMAL(12,2) NOT NULL, comm DECIMAL(12,2),
    deptno INT NOT NULL, img VARCHAR(128) NOT NULL, FOREIGN KEY (deptno) REFERENCES dept(deptno)
);
START TRANSACTION;
""")

for row in rows("dept.csv"):
    values = ", ".join(literal(row[column]) for column in ("deptno", "dname", "loc"))
    print(f"INSERT IGNORE INTO dept (deptno, dname, loc) VALUES ({values});")

for row in rows("emp.csv"):
    row["hiredate"] = datetime.strptime(row["hiredate"], "%d-%b-%Y").date().isoformat()
    columns = ("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno", "img")
    values = ", ".join(literal(row[column]) for column in columns)
    print(f"INSERT IGNORE INTO emp ({', '.join(columns)}) VALUES ({values});")

print("COMMIT;")
