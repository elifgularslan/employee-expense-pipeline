import json
import os
import subprocess
import time
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

root = Path(__file__).resolve().parents[1]
home = Path(os.environ.get("PRO3_HOME", Path.home() / ".local/share/sau-pro3"))
hdfs = Path(os.environ.get("HADOOP_HOME", Path.home() / "hadoop")) / "bin/hdfs"
kafka = home / "dist/kafka_2.13-3.9.1/bin/kafka-topics.sh"
cqlsh = home / "dist/apache-cassandra-5.0.9/bin/cqlsh"
web = "http://127.0.0.1:8083"


def command(*args, **kwargs):
    return subprocess.run(args, cwd=root, check=True, text=True, timeout=60, **kwargs)


def report(empno, page=1):
    with urlopen(f"{web}/api/employees/{empno}/expenses?page={page}", timeout=50) as response:
        return json.load(response, parse_float=Decimal)


def expect_error(path, status):
    try:
        urlopen(web + path, timeout=15).close()
    except HTTPError as error:
        assert error.code == status, (path, error.code)
    else:
        raise AssertionError(f"{path} should return {status}")


def verify_photo():
    photo_path = "/sau-pro3/photos/scott.jpg"
    exists = subprocess.run([hdfs, "dfs", "-test", "-e", photo_path], timeout=20).returncode == 0
    with urlopen(f"{web}/employees/7788/photo", timeout=20) as image:
        if exists:
            assert image.headers.get_content_type() == "image/jpeg"
            assert image.read(2) == b"\xff\xd8"
            print("PASS HDFS existing employee image")
            return
        assert image.headers.get_content_type() == "image/png"
        assert image.read() == (root / "data/placeholder.png").read_bytes()

    fixture = root / "src/test/resources/upload-test.jpg"
    try:
        result = command("curl", "-fsS", "--max-time", "20", "-o", "/dev/null", "-w", "%{http_code}",
                         "-F", f"photo=@{fixture};type=image/jpeg", f"{web}/employees/7788/photo",
                         capture_output=True)
        assert result.stdout == "302", result.stdout
        with urlopen(f"{web}/employees/7788/photo", timeout=20) as image:
            assert image.headers.get_content_type() == "image/jpeg"
            assert image.read() == fixture.read_bytes()
        print("PASS HDFS placeholder, multipart upload and byte-for-byte download")
    finally:
        if subprocess.run([hdfs, "dfs", "-test", "-e", photo_path], timeout=20).returncode == 0:
            command(hdfs, "dfs", "-rm", photo_path, capture_output=True)


def main():
    topics = command(kafka, "--bootstrap-server", "127.0.0.1:9092", "--list", capture_output=True)
    assert {"accounting", "research", "sales", "operations"} <= set(topics.stdout.splitlines())
    print("PASS four department topics")

    before = {empno: report(empno) for empno in (7788, 7499, 7839)}
    marker = f"smoke-{int(time.time())}"
    rows = [
        f'7788,2026-11-23T12:30:00+03:00,"{marker}, coffee",food,2,12.50',
        f"7788,2026-11-23 12:31:00,{marker}-invalid,food,1,1.999",
        f"7788,2026-11-23 12:32:00,{marker}-research,books,1,37.25",
        f"7499,2026-11-23T12:33:00,{marker}-sales,food,1,5.00",
        f"7839,2026-11-23T12:34:00,{marker}-accounting,food,1,8.25",
    ]
    published = command("bash", "scripts/pos.sh", "--stdin", input="\n".join(rows) + "\n", capture_output=True)
    for topic in ("research", "sales", "accounting"):
        assert f"{topic} " in published.stdout

    expected = {7788: (2, Decimal("49.75")), 7499: (1, Decimal("5.00")), 7839: (1, Decimal("8.25"))}
    deadline = time.monotonic() + 90
    while True:
        after = {empno: report(empno) for empno in expected}
        if all(after[e]["count"] >= before[e]["count"] + expected[e][0] for e in expected):
            break
        if time.monotonic() >= deadline:
            raise AssertionError(f"Spark reports did not receive Kafka records: {after}")
        time.sleep(2)
    for empno, (count, total) in expected.items():
        assert after[empno]["count"] == before[empno]["count"] + count, after[empno]
        assert after[empno]["total"] == before[empno]["total"] + total, after[empno]

    statement = "SELECT topic, description, payment FROM sau_pro3.expenses WHERE empno = 7788;"
    storage = command(cqlsh, "127.0.0.1", "9042", "-e", statement, capture_output=True).stdout
    assert f"{marker}, coffee" in storage and f"{marker}-research" in storage
    assert f"{marker}-invalid" not in storage
    print("PASS POS routing, CSV parsing, Cassandra writes, employee isolation and Spark totals")
    print(f"SCOTT added 49.75 across 2 expenses, cumulative total {after[7788]['total']}")

    second_page = report(7788, 2)
    assert second_page["total"] == after[7788]["total"] and second_page["count"] == after[7788]["count"]
    expect_error("/api/employees/7788/expenses?page=0", 400)
    expect_error("/api/employees/999999/expenses", 404)
    with urlopen(f"{web}/employees/7788", timeout=50) as response:
        html = response.read().decode("utf-8")
        for value in ('lang="en"', "Employee expense report", "Upload photo", "Total expenses",
                      "SCOTT", "JONES", "RESEARCH", "3000.00", "expense-total"):
            assert value in html, value
    print("PASS pagination, error codes and rendered MySQL/Spark profile")
    verify_photo()


if __name__ == "__main__":
    os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
    main()
