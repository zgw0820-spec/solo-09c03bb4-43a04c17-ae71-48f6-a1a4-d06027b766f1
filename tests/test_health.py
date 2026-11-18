"""Chaos tests for the /health readiness probe (no Docker required).

Boots an ephemeral PostgreSQL via ``pgserver`` plus the real FastAPI app
under uvicorn, then drives the failure modes the on-call operator cares
about:

* baseline: healthy while the database is up;
* full load: both business connections busy -> /health still 200, fast;
* concurrency: many simultaneous /health checks share ONE probe backend;
* disconnect: postmaster SIGSTOPped (blackhole) -> /health 503 with a
  reason, bounded in time, late results never reported as healthy, and no
  leftover probe backend afterwards;
* recovery: SIGCONT -> /health 200 again;
* full stop/start of the server -> 503 while down, 200 after restart,
  and /query recovers too.

Run:  python tests/test_health.py
"""
from __future__ import annotations

import json
import os
import pathlib
import signal
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg  # noqa: E402
import pgserver  # noqa: E402

BASE = "http://127.0.0.1:8011"
PROBE_BUDGET_MS = 1500
# Worst case /health latency: budget + cancel grace + task grace + slack.
HEALTH_BOUND_S = PROBE_BUDGET_MS / 1000 + 2.0 + 1.5

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def get(path: str, timeout: float = 15.0):
    try:
        with urllib.request.urlopen(BASE + path, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    except urllib.error.URLError as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def post(path: str, payload: dict, timeout: float = 30.0):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        BASE + path, data=data,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    except urllib.error.URLError as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


# --------------------------------------------------------------------- #
# PostgreSQL process control (SIGSTOP = blackhole, SIGCONT = resume)
# --------------------------------------------------------------------- #
def pg_pids(data_dir: str) -> set[int]:
    """Postmaster pid plus its direct children (backends)."""
    pm_pid = int(
        open(os.path.join(data_dir, "postmaster.pid"), encoding="utf-8")
        .readline()
        .strip()
    )
    pids = {pm_pid}
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat", "rb") as fh:
                rest = fh.read().rsplit(b")", 1)[1].split()
            if int(rest[1]) == pm_pid:  # ppid field
                pids.add(int(name))
        except Exception:
            continue
    return pids


def signal_pg(data_dir: str, sig: int) -> set[int]:
    pids = pg_pids(data_dir)
    for pid in pids:
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            pass
    return pids


def probe_backend_count(admin_dsn: str) -> int:
    with psycopg.connect(admin_dsn, connect_timeout=5) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE application_name = 'health_probe'"
        )
        return cur.fetchone()[0]


def wait_probe_backends(admin_dsn: str, want: int, timeout: float = 5.0) -> int:
    deadline = time.monotonic() + timeout
    seen = -1
    while time.monotonic() < deadline:
        seen = probe_backend_count(admin_dsn)
        if seen == want:
            break
        time.sleep(0.1)
    return seen


def main() -> int:
    data_dir = tempfile.mkdtemp(prefix="healthchaos-")
    print(f"[test_health] ephemeral PostgreSQL in {data_dir}")
    pg = pgserver.get_server(data_dir, cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    ddl = (ROOT / "db" / "01_init.sql").read_text(encoding="utf-8")
    with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("CREATE DATABASE faultdb")
    from urllib.parse import urlparse, urlunparse

    fault_admin = urlunparse(urlparse(admin_dsn)._replace(path="/faultdb"))
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(ddl)
        cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
    ro = urlunparse(
        urlparse(fault_admin)._replace(
            netloc=urlparse(fault_admin).netloc.replace(
                urlparse(fault_admin).username or "postgres", "fault_ro", 1
            )
        )
    )

    # Point the application at the ephemeral database BEFORE importing it.
    os.environ["DATABASE_DSN"] = ro
    os.environ["POOL_SIZE"] = "2"
    os.environ["HEALTH_PROBE_TIMEOUT_MS"] = str(PROBE_BUDGET_MS)

    import uvicorn  # noqa: E402

    from app.main import app  # noqa: E402

    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=8011, log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 15
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.05)
    check("uvicorn 启动", server.started)

    try:
        print("=== A. 基线：数据库正常 ===")
        t0 = time.monotonic()
        status, body = get("/health")
        elapsed = time.monotonic() - t0
        check("健康检查 200", status == 200, body)
        check("status=healthy", body.get("status") == "healthy", body)
        check("probe.ok 且延迟有界",
              body.get("probe", {}).get("ok") is True
              and elapsed < 1.5, f"elapsed={elapsed:.2f} body={body}")
        check("池容量 2 且两条空闲",
              body["pool"]["size"] == 2 and body["pool"]["idle"] == 2, body)

        print("=== B. 业务满载：两条业务连接全忙 ===")
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
        time.sleep(0.5)
        t0 = time.monotonic()
        status, body = get("/health")
        elapsed = time.monotonic() - t0
        check("全忙时健康检查仍 200/healthy",
              status == 200 and body.get("status") == "healthy", body)
        check("in_use=2 且探测未被阻塞 (<1.5s)",
              body["pool"]["in_use"] == 2 and elapsed < 1.5,
              f"elapsed={elapsed:.2f} body={body}")
        for t in threads:
            t.join()
        check("业务慢查询自身成功",
              len(outcomes) == 2
              and all(s == 200 for s, _ in outcomes), outcomes)

        print("=== C. 并发健康检查共用一条探测连接 ===")
        barrier = threading.Barrier(8)
        results: list = []

        def hit() -> None:
            barrier.wait()
            results.append(get("/health"))

        threads = [threading.Thread(target=hit) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        check("8 个并发健康检查全部 200/healthy",
              len(results) == 8
              and all(s == 200 and b.get("status") == "healthy"
                      for s, b in results),
              results)
        n = probe_backend_count(fault_admin)
        check("专用探测后端恰好一条", n == 1, f"probe_backends={n}")
        with psycopg.connect(fault_admin) as c, c.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE usename = 'fault_ro'"
            )
            total_ro = cur.fetchone()[0]
        check("fault_ro 后端总数 = 2 业务 + 1 探测",
              total_ro == 3, f"total={total_ro}")

        print("=== D. 断连：冻结数据库（SIGSTOP，黑洞）===")
        frozen = signal_pg(data_dir, signal.SIGSTOP)
        check("已冻结 postmaster 及后端", len(frozen) >= 3, frozen)
        try:
            t0 = time.monotonic()
            status, body = get("/health", timeout=HEALTH_BOUND_S + 5)
            elapsed = time.monotonic() - t0
            check("冻结期间健康检查 503", status == 503, body)
            check("status=unhealthy 且给出原因",
                  body.get("status") == "unhealthy"
                  and bool(body.get("detail")), body)
            check(f"响应有界 (<{HEALTH_BOUND_S:.1f}s)",
                  elapsed < HEALTH_BOUND_S, f"elapsed={elapsed:.2f}")
            check("迟到结果未报为健康（probe.ok=False）",
                  body.get("probe", {}).get("ok") is False, body)

            # A second check while frozen takes the reconnect path and
            # must also fail bounded.
            t0 = time.monotonic()
            status, body = get("/health", timeout=HEALTH_BOUND_S + 5)
            elapsed = time.monotonic() - t0
            check("冻结期间再次检查仍 503（建连路径）", status == 503, body)
            check(f"建连路径同样有界 (<{HEALTH_BOUND_S:.1f}s)",
                  elapsed < HEALTH_BOUND_S, f"elapsed={elapsed:.2f}")
        finally:
            signal_pg(data_dir, signal.SIGCONT)

        print("=== E. 恢复：解冻后不遗留后端、再次检查报健康 ===")
        n = wait_probe_backends(fault_admin, 0, timeout=5.0)
        check("超时探测未遗留后端（旧探测连接已关闭）", n == 0,
              f"probe_backends={n}")
        status, body = get("/health")
        check("恢复后健康检查 200/healthy",
              status == 200 and body.get("status") == "healthy", body)
        n = probe_backend_count(fault_admin)
        check("恢复后重建恰好一条探测连接", n == 1, f"probe_backends={n}")

        print("=== F. 完全停库 -> 503；重启 -> 200 且 /query 恢复 ===")
        pg.cleanup()
        t0 = time.monotonic()
        status, body = get("/health")
        elapsed = time.monotonic() - t0
        check("停库后健康检查 503", status == 503, body)
        check("停库后给出失败原因",
              body.get("status") == "unhealthy"
              and bool(body.get("detail")), body)
        check("连接拒绝快速失败 (<5s)", elapsed < 5.0,
              f"elapsed={elapsed:.2f}")

        pg2 = pgserver.get_server(data_dir, cleanup_mode="stop")
        check("数据库重启成功", pg2 is not None)
        deadline = time.monotonic() + 15
        recovered = False
        while time.monotonic() < deadline:
            status, body = get("/health", timeout=5)
            if status == 200:
                recovered = True
                break
            time.sleep(0.3)
        check("重启后健康检查恢复 200/healthy",
              recovered and body.get("status") == "healthy", body)
        # The first /query bears the cost of discovering the two dead pooled
        # backends: the executor retries once by design, so with BOTH
        # backends stale the first request can fail; the pool then consists
        # of fresh connections and subsequent queries succeed with their own
        # rows.  (Pre-existing lazy-recovery semantics, unchanged by this
        # feature.)
        recovered_query = None
        for _ in range(4):
            status, body = post(
                "/query",
                {"sql": "SELECT count(*) FROM fault_logs", "timeout_ms": 8000},
            )
            if status == 200 and body.get("rows") == [[7]]:
                recovered_query = (status, body)
                break
            time.sleep(0.3)
        check("重启后 /query 经弃旧建新恢复且结果正确",
              recovered_query is not None,
              f"last={status} {body}")
        pg2.cleanup()
    finally:
        server.should_exit = True
        thread.join(timeout=5)

    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
