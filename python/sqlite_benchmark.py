import apsw
import time

class Executor:
    def __init__(self):
        self.conn = apsw.Connection(":memory:")
        self.cursor = self.conn.cursor()

    def exec(self, sql):
        return list(self.cursor.execute(sql))


def assert_row_count(rows, expected):
    cnt = len(rows)
    if cnt != expected:
        raise AssertionError(f"Expected {expected} rows, got {cnt}")
    print(f"Row count OK: {cnt}")


def main():
    exec = Executor()

    exec.exec("CREATE TABLE A (id INTEGER)")
    exec.exec("CREATE TABLE B (id INTEGER)")
    exec.exec("CREATE TABLE C (id INTEGER)")

    start = time.perf_counter()

    for i in range(1, 10001):
        exec.exec(f"INSERT INTO A VALUES ({i})")

    for i in range(5000, 20001):
        exec.exec(f"INSERT INTO B VALUES ({i})")

    for i in range(8000, 12001):
        exec.exec(f"INSERT INTO C VALUES ({i})")

    duration = time.perf_counter() - start
    print(f"Query time: {duration * 1000:.3f} ms")

    query = """
        SELECT id FROM (
            SELECT id FROM A
            INTERSECT
            SELECT id FROM B
        ) INTERSECT SELECT id FROM C
    """


    start = time.perf_counter()
    result = exec.exec(query)
    assert_row_count(result, 2001)

    duration = time.perf_counter() - start

    print(f"Query time: {duration * 1000:.3f} ms")


if __name__ == "__main__":
    main()
