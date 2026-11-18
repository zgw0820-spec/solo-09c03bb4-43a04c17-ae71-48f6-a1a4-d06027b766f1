"""End-to-end self-test for the read-only query API.

Runs entirely on the Python standard library so it works inside the slim
application container as well as on a developer machine:

    python -m tests.selftest

Scenarios:
  1. health + normal SELECT returns the analyst's own rows
  2. read-only guard rails (multi-statement, INSERT, non-SELECT rejected)
  3. single slow query times out, is cancelled, receipt is final
  4. a queued request whose budget expires while waiting never executes
  5. after two concurrent slow queries, later requests still get their own
     results (connection reuse after confirmed rollback)
  6. pool capacity really is two (observed via pg_stat_activity)
  7. /health stays healthy while both business connections are busy
  8. concurrent /health checks share one dedicated probe connection
  9. a probe connection killed out-of-band is rebuilt transparently
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000"

PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def post(path: str, payload: dict, timeout: float = 30.0):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        BASE + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    except urllib.error.URLError as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def get(path: str, timeout: float = 10.0):
    try:
        with urllib.request.urlopen(BASE + path, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    except urllib.error.URLError as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def run_case(title: str, fn) -> None:
    print(f"\n=== {title} ===")
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - test harness must keep going
        check(f"{title} 未抛异常", False, repr(exc))


def case_health_and_basic() -> None:
    status, body = get("/health")
    check("健康检查 200", status == 200, body)
    check("状态为 healthy", body["status"] == "healthy", body)
    check("本次探测成功且为有界延迟",
          body["probe"]["ok"] is True
          and isinstance(body["probe"]["latency_ms"], (int, float)), body)
    check("连接池容量为 2", body["pool"]["size"] == 2, body)
    check("启动即建满两条连接", body["pool"]["idle"] == 2, body)

    status, body = post(
        "/query",
        {"sql": "SELECT count(*) AS n FROM fault_logs", "timeout_ms": 5000},
    )
    check("普通 SELECT 200", status == 200, body)
    check("返回 ok 且已执行", body["status"] == "ok" and body["executed"], body)
    check("预置 7 条故障日志", body["rows"][0][0] == 7, body)

    status, body = post(
        "/query",
        {
            "sql": "SELECT device_id, severity FROM fault_logs "
                   "WHERE severity = 'CRITICAL' ORDER BY id",
            "timeout_ms": 5000,
        },
    )
    check("过滤查询返回自身结果", status == 200 and body["row_count"] == 2, body)
    check(
        "结果内容正确",
        [r[0] for r in body["rows"]] == ["SW-CORE-01", "SRV-DB-07"],
        body,
    )


def case_read_only_guards() -> None:
    for name, sql in [
        ("多语句", "SELECT 1; SELECT 2"),
        ("INSERT", "INSERT INTO fault_logs (device_id) VALUES ('x')"),
        ("UPDATE", "UPDATE fault_logs SET site = 'z'"),
        ("非 SELECT", "SHOW transaction_read_only"),
        ("SELECT INTO", "SELECT 1 INTO TEMP t"),
    ]:
        status, body = post("/query", {"sql": sql, "timeout_ms": 5000})
        check(f"{name} 被拒绝(400)", status == 400, body)
        check(f"{name} 错误码 invalid_sql",
              body.get("error", {}).get("code") == "invalid_sql", body)

    # pg_sleep 是允许的只读函数，用于慢查询演示。
    status, body = post(
        "/query", {"sql": "SELECT pg_sleep(0.1)", "timeout_ms": 5000}
    )
    check("pg_sleep 只读函数允许执行", status == 200 and body["status"] == "ok", body)


def case_single_timeout() -> None:
    _, before = get("/health")
    discarded_before = before["pool"]["total_discarded"]
    t0 = time.monotonic()
    status, body = post(
        "/query",
        {"sql": "SELECT pg_sleep(3)", "timeout_ms": 800},
        timeout=15,
    )
    elapsed = time.monotonic() - t0
    check("超时返回 504", status == 504, body)
    check("错误码 query_timeout",
          body.get("error", {}).get("code") == "query_timeout", body)
    check("回执 status=timeout", body.get("status") == "timeout", body)
    check("语句确实已发送执行", body.get("executed") is True, body)
    check("已发起取消", body.get("cancelled") is True, body)
    check("及时返回 (<2.5s)", elapsed < 2.5, f"elapsed={elapsed:.2f}")

    # Give the server a moment, then prove the slow statement is gone and
    # the connection was reused for a fresh, independent query.
    time.sleep(0.5)
    status, body = post(
        "/query", {"sql": "SELECT 42 AS answer", "timeout_ms": 5000}
    )
    check("超时后连接可复用且返回自身结果",
          status == 200 and body["rows"] == [[42]], body)

    status, health = get("/health")
    check("池容量仍为 2", health["pool"]["size"] == 2, health)
    check("确认事务结束后复用，本用例无新增弃置",
          health["pool"]["total_discarded"] == discarded_before, health)


def case_queued_expiry() -> None:
    results: list[tuple] = []

    def slow(marker: str, sleep_s: float, budget_ms: int) -> None:
        t0 = time.monotonic()
        status, body = post(
            "/query",
            {"sql": f"SELECT '{marker}', pg_sleep({sleep_s})",
             "timeout_ms": budget_ms},
            timeout=20,
        )
        results.append((marker, status, body, round(time.monotonic() - t0, 2)))

    # Two slow queries occupy both connections for ~2.5s.
    t1 = threading.Thread(target=slow, args=("A", 2.5, 5000))
    t2 = threading.Thread(target=slow, args=("B", 2.5, 5000))
    t1.start()
    time.sleep(0.3)
    t2.start()
    time.sleep(0.3)

    # Third request: only 500ms total budget, both backends are busy.
    t0 = time.monotonic()
    status, body = post(
        "/query",
        {"sql": "SELECT 'SHOULD-NOT-RUN', pg_sleep(0)", "timeout_ms": 500},
        timeout=10,
    )
    waited = time.monotonic() - t0
    check("排队过期返回 503", status == 503, body)
    check("错误码 acquire_timeout",
          body.get("error", {}).get("code") == "acquire_timeout", body)
    check("排队等待本身计入预算 (~0.5s)", 0.4 < waited < 1.5,
          f"waited={waited:.2f}")

    t1.join()
    t2.join()
    ok = [r for r in results if r[1] == 200 and r[2]["status"] == "ok"]
    check("两条占池慢查询最终各自成功", len(ok) == 2, results)
    check(
        "两条慢查询拿到各自的标记结果",
        {r[2]["rows"][0][0] for r in ok} == {"A", "B"},
        results,
    )

    # After the backlog drains, pool must be fully reusable.
    status, body = post(
        "/query", {"sql": "SELECT 'AFTER-BACKLOG'", "timeout_ms": 5000}
    )
    check("积压清空后新查询正常",
          status == 200 and body["rows"][0][0] == "AFTER-BACKLOG", body)


def case_capacity_and_reuse() -> None:
    barrier = threading.Barrier(3)
    finished: list = []

    def worker(i: int) -> None:
        barrier.wait()
        status, body = post(
            "/query",
            {"sql": f"SELECT {i}, pg_sleep(1.2)", "timeout_ms": 8000},
            timeout=15,
        )
        finished.append((i, status, body))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    _, before = get("/health")
    discarded_before = before["pool"]["total_discarded"]
    for t in threads:
        t.start()
    t0 = time.monotonic()
    for t in threads:
        t.join()
    wall = time.monotonic() - t0

    check("三个并发请求全部成功",
          len(finished) == 3 and all(s == 200 for _, s, _ in finished),
          finished)
    check(
        "每个请求拿到自身结果(无串结果)",
        {b["rows"][0][0] for _, _, b in finished} == {0, 1, 2},
        finished,
    )
    # Capacity 2 -> three 1.2s sleeps must take > 1.8s (two waves).
    check("容量 2 导致第三个请求排队 (>1.8s)", wall > 1.8, f"wall={wall:.2f}")

    status, health = get("/health")
    check("结束后两条连接全部空闲", health["pool"]["idle"] == 2, health)
    check("容量保持 2、本用例无新增弃置",
          health["pool"]["size"] == 2
          and health["pool"]["total_discarded"] == discarded_before, health)


def _admin_dsn() -> str | None:
    admin_dsn = os.environ.get("ADMIN_DSN")
    if not admin_dsn:
        dsn_file = os.path.join(tempfile.gettempdir(), "faultdb_admin_dsn")
        if os.path.exists(dsn_file):
            admin_dsn = open(dsn_file, encoding="utf-8").read().strip()
    return admin_dsn or None


def case_health_under_full_load() -> None:
    """Both business connections busy, database fine -> still healthy."""
    barrier = threading.Barrier(2)
    outcomes: list = []

    def slow(i: int) -> None:
        barrier.wait()
        outcomes.append(post(
            "/query",
            {"sql": f"SELECT {i}, pg_sleep(2)", "timeout_ms": 8000},
            timeout=15,
        ))

    threads = [threading.Thread(target=slow, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    time.sleep(0.5)  # let both queries actually start on the backends

    t0 = time.monotonic()
    status, body = get("/health", timeout=10)
    elapsed = time.monotonic() - t0
    check("业务连接全忙时健康检查仍 200", status == 200, body)
    check("状态为 healthy", body.get("status") == "healthy", body)
    check("探测成功", body.get("probe", {}).get("ok") is True, body)
    check("两条业务连接都在占用中", body["pool"]["in_use"] == 2, body)
    check("探测未被业务查询阻塞 (<1.5s)", elapsed < 1.5,
          f"elapsed={elapsed:.2f}")

    for t in threads:
        t.join()
    check("占池慢查询自身不受影响",
          all(s == 200 for s, _ in outcomes), outcomes)


def case_concurrent_health_checks() -> None:
    """Concurrent checks share at most one dedicated probe connection."""
    barrier = threading.Barrier(6)
    results: list = []

    def hit() -> None:
        barrier.wait()
        results.append(get("/health", timeout=10))

    threads = [threading.Thread(target=hit) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("6 个并发健康检查全部 200",
          len(results) == 6 and all(s == 200 for s, _ in results), results)
    check("并发结果均为 healthy",
          all(b.get("status") == "healthy" for _, b in results), results)

    admin_dsn = _admin_dsn()
    if not admin_dsn:
        print("  SKIP  探测连接数核对（未设置 ADMIN_DSN）")
        return
    try:
        import psycopg
    except ImportError:
        print("  SKIP  探测连接数核对（无 psycopg）")
        return
    with psycopg.connect(admin_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE application_name = 'health_probe'"
        )
        probe_backends = cur.fetchone()[0]
    check("专用探测连接最多一条", probe_backends <= 1,
          f"probe_backends={probe_backends}")


def case_probe_backend_killed() -> None:
    """Probe connection killed out-of-band -> next check rebuilds, still 200."""
    admin_dsn = _admin_dsn()
    if not admin_dsn:
        print("  SKIP  探测连接被杀恢复测试（未设置 ADMIN_DSN）")
        return
    try:
        import psycopg
    except ImportError:
        print("  SKIP  探测连接被杀恢复测试（无 psycopg）")
        return

    status, body = get("/health")
    check("前置健康检查 200", status == 200, body)
    with psycopg.connect(admin_dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE application_name = 'health_probe'"
        )
        killed = [r[0] for r in cur.fetchall()]
    check("管理连接成功终止探测后端", killed and killed[0] is True, killed)

    t0 = time.monotonic()
    status, body = get("/health", timeout=10)
    elapsed = time.monotonic() - t0
    check("探测连接被杀后仍报健康(弃旧建新)",
          status == 200 and body.get("status") == "healthy", body)
    check("重建探测有界 (<2s)", elapsed < 2.0, f"elapsed={elapsed:.2f}")


def case_discard_and_rebuild() -> None:
    """Kill a pooled backend out-of-band; the API must discard and rebuild."""
    admin_dsn = _admin_dsn()
    if not admin_dsn:
        print("  SKIP  弃置补建混沌测试（未设置 ADMIN_DSN）")
        return
    try:
        import psycopg
    except ImportError:
        print("  SKIP  弃置补建混沌测试（无 psycopg）")
        return

    with psycopg.connect(admin_dsn) as conn, conn.cursor() as cur:
        # Exclude the dedicated health-probe connection: it lives outside
        # the two-connection business pool by design.
        cur.execute(
            "SELECT pid FROM pg_stat_activity "
            "WHERE usename = 'fault_ro' AND pid <> pg_backend_pid() "
            "AND application_name <> 'health_probe'"
        )
        pids = [r[0] for r in cur.fetchall()]
        check("池内存在两条 fault_ro 业务后端", len(pids) == 2, pids)
        victim = pids[0]
        cur.execute("SELECT pg_terminate_backend(%s)", (victim,))
        check("管理连接成功终止一个后端", cur.fetchone()[0] is True)

    # Run several sequential queries so both pooled backends are cycled;
    # whichever request draws the dead backend must transparently discard,
    # rebuild and retry — every client call still succeeds with its own rows.
    statuses, markers = [], []
    for i in range(4):
        status, body = post(
            "/query",
            {"sql": f"SELECT 'CYCLE-{i}'", "timeout_ms": 8000},
        )
        statuses.append(status)
        if status == 200:
            markers.append(body["rows"][0][0])
    check("循环借出期间所有请求均成功(客户端无感)",
          statuses == [200] * 4, statuses)
    check("每次都返回自身结果",
          markers == [f"CYCLE-{i}" for i in range(4)], markers)

    status, health = get("/health")
    check("弃置计数增加",
          health["pool"]["total_discarded"] >= 1, health)
    check("补建后容量恢复为 2 且全空闲",
          health["pool"]["size"] == 2 and health["pool"]["idle"] == 2, health)

    # Physical capacity check: while two slow queries run, the database must
    # see at most two fault_ro *business* backends — no third connection is
    # ever opened for queries (the health probe is a separate, dedicated
    # connection and is excluded here).
    observed: list[int] = []
    barrier = threading.Barrier(2)

    def slow(i: int) -> None:
        barrier.wait()
        post("/query",
             {"sql": f"SELECT {i}, pg_sleep(2)", "timeout_ms": 8000},
             timeout=15)

    threads = [threading.Thread(target=slow, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    deadline = time.monotonic() + 3
    with psycopg.connect(admin_dsn) as conn, conn.cursor() as cur:
        while time.monotonic() < deadline:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE usename='fault_ro' "
                "AND application_name <> 'health_probe'"
            )
            observed.append(cur.fetchone()[0])
            time.sleep(0.1)
    for t in threads:
        t.join()
    check("数据库侧 fault_ro 业务后端数从未超过 2",
          observed and max(observed) <= 2, f"max={max(observed)}")


def main() -> int:
    print(f"self-test against {BASE}")
    run_case("1. 健康检查与普通查询", case_health_and_basic)
    run_case("2. 只读约束", case_read_only_guards)
    run_case("3. 单条慢查询超时与取消", case_single_timeout)
    run_case("4. 排队期间过期不执行", case_queued_expiry)
    run_case("5. 容量为 2 与接续查询", case_capacity_and_reuse)
    run_case("6. 弃置并补建 / 物理容量", case_discard_and_rebuild)
    run_case("7. 业务满载时健康检查仍报健康", case_health_under_full_load)
    run_case("8. 并发健康检查共用一条探测连接", case_concurrent_health_checks)
    run_case("9. 探测连接被杀后自动重建", case_probe_backend_killed)
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
