"""Readiness prober behind ``GET /health``.

Pool counters alone cannot prove the service can take work: after a
PostgreSQL outage the two *dead* business connections still show up as
``idle=2`` and the old endpoint wrongly reported ``healthy``.  This module
therefore verifies, on every call and within a bounded time, that the
database can actually execute a read-only query.

Design rules, mapped to the operational requirements:

* The probe never borrows a business connection.  A single *dedicated*
  probe connection is lazily built outside the two-connection pool, so a
  health check neither occupies a business backend nor cancels a business
  query (``PQcancel`` only ever targets the probe's own backend).  With
  both business connections busy and the database fine, the probe still
  succeeds and the service reports healthy.
* Concurrent ``/health`` checks are serialised on one lock, so at most one
  dedicated probe connection exists at any moment.
* Every check is bounded by ``health_probe_timeout_ms`` plus short,
  constant cleanup grace periods.  The probe statement runs as a shielded
  task: on timeout the verdict is 503 no matter what arrives later — a
  late result is audit-logged but never reported as healthy — and the
  probe connection is cancelled and closed so no half-broken backend
  lingers.  The next check lazily builds a fresh one.
* A stale standing probe connection (e.g. killed during an outage) is
  discarded and the probe retried once on a fresh connection within the
  same budget, so the first check *after* a recovery reports healthy.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from dataclasses import dataclass

import psycopg
from psycopg import AsyncConnection

from .config import Settings

logger = logging.getLogger("healthprobe")

# Minimal read-only statement: proves the backend can execute a query.
PROBE_SQL = "SELECT 1"

# Bounded cleanup grace periods after a probe timeout.  They keep the
# total /health latency capped at budget + ~2s even when the database
# stops responding mid-probe.
_CANCEL_GRACE_S = 1.0
_TASK_GRACE_S = 1.0


@dataclass
class ProbeResult:
    ok: bool
    detail: str
    latency_ms: float | None = None


class ProbeConnectError(Exception):
    """The dedicated probe connection could not be (re)built in time."""


class HealthProber:
    """Owns the single dedicated probe connection and runs bounded probes."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._conn: AsyncConnection | None = None
        # Serialises concurrent checks: at most one probe at a time, hence
        # at most one dedicated probe connection.
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def close(self) -> None:
        async with self._lock:
            await self._drop_connection()

    async def _drop_connection(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()

    # ------------------------------------------------------------------ #
    # Public entry point
    # ------------------------------------------------------------------ #
    async def check(self) -> ProbeResult:
        """Probe the database once, bounded in time, sharing one connection."""
        budget = self._settings.health_probe_timeout_ms / 1000.0
        deadline = time.monotonic() + budget
        try:
            await asyncio.wait_for(self._lock.acquire(), timeout=budget)
        except TimeoutError:
            # Another check's probe (incl. its bounded cleanup) is still
            # running; we could not verify within our own bounded time.
            return ProbeResult(
                False, "上一次健康探测尚未结束，本次未能在有界时间内完成验证"
            )
        try:
            return await self._probe(deadline)
        finally:
            self._lock.release()

    # ------------------------------------------------------------------ #
    # Probe internals (called with the lock held)
    # ------------------------------------------------------------------ #
    async def _probe(self, deadline: float) -> ProbeResult:
        budget_ms = self._settings.health_probe_timeout_ms
        for attempt in (1, 2):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return ProbeResult(
                    False, f"探测未能在 {budget_ms}ms 预算内完成"
                )
            try:
                conn = await self._ensure_connection(remaining)
            except ProbeConnectError as exc:
                return ProbeResult(False, f"无法建立探测连接: {exc}")

            started = time.monotonic()
            # Shielded like the business executor: once the deadline
            # passes, a late result must not change the verdict.
            task = asyncio.ensure_future(self._run_probe_query(conn))
            try:
                await asyncio.wait_for(
                    asyncio.shield(task),
                    timeout=deadline - time.monotonic(),
                )
            except (asyncio.TimeoutError, TimeoutError):
                await self._abort_timed_out_probe(conn, task)
                return ProbeResult(
                    False,
                    f"探测查询超过 {budget_ms}ms 预算，已按失败处理"
                    "（迟到结果不会被采信）",
                )
            except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
                # Connection-level failure.  If this was the stale standing
                # connection, rebuild once within the same budget so the
                # first check after a recovery reports healthy.
                reason = f"探测连接失效: {type(exc).__name__}: {exc}"
                logger.warning(
                    "health probe connection failed (attempt %d): %s",
                    attempt,
                    exc,
                )
                await self._drop_connection()
                if attempt == 1 and deadline - time.monotonic() > 0.05:
                    continue
                return ProbeResult(False, reason)
            except Exception as exc:  # statement-level or unexpected
                logger.warning("health probe query failed: %s", exc)
                await self._drop_connection()
                return ProbeResult(
                    False, f"探测查询失败: {type(exc).__name__}: {exc}"
                )

            latency_ms = round((time.monotonic() - started) * 1000, 3)
            return ProbeResult(True, "数据库可执行只读查询，服务可接单", latency_ms)

        return ProbeResult(False, "探测失败")  # pragma: no cover

    async def _ensure_connection(self, remaining: float) -> AsyncConnection:
        conn = self._conn
        if conn is not None and not conn.closed:
            return conn
        # Drop the stale reference before rebuilding.
        self._conn = None
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()

        # libpq connect_timeout takes whole seconds; the wait_for below is
        # the hard backstop so the check stays bounded even if the network
        # blackholes the connection attempt.
        connect_timeout = max(
            1, math.ceil(min(remaining, self._settings.connect_timeout))
        )
        try:
            conn = await asyncio.wait_for(
                AsyncConnection.connect(
                    self._settings.database_dsn,
                    connect_timeout=connect_timeout,
                    autocommit=True,
                    application_name="health_probe",
                ),
                timeout=remaining,
            )
        except Exception as exc:
            raise ProbeConnectError(f"{type(exc).__name__}: {exc}") from exc
        self._conn = conn
        logger.info("dedicated health-probe connection established")
        return conn

    @staticmethod
    async def _run_probe_query(conn: AsyncConnection) -> None:
        cur = await conn.execute(PROBE_SQL)
        row = await cur.fetchone()
        if row != (1,):
            raise RuntimeError(f"探测查询返回异常结果: {row!r}")

    async def _abort_timed_out_probe(
        self, conn: AsyncConnection, task: asyncio.Task
    ) -> None:
        """After a timeout: never trust a late result, never keep the backend.

        Cancel best-effort, give the shielded task a short bounded grace to
        end, then close the probe connection for good so no half-broken
        backend lingers; the next check lazily builds a fresh one.
        """
        logger.warning(
            "health probe timed out; cancelling and dropping probe connection"
        )
        # cancel_safe() uses the non-blocking libpq >= 17 cancellation API
        # with its own bounded timeout.  (A blocking PQcancel via to_thread
        # would hold the GIL while waiting for an unresponsive server and
        # wedge the whole process.)
        with contextlib.suppress(Exception):
            await conn.cancel_safe(timeout=_CANCEL_GRACE_S)
        try:
            late = await asyncio.wait_for(
                asyncio.shield(task), timeout=_TASK_GRACE_S
            )
        except BaseException:  # cancelled / timed out / driver error
            late = None
        else:
            # The verdict for this check is already 503; audit-log only.
            logger.warning(
                "late probe result arrived after timeout and is ignored: %r",
                late,
            )
        await self._drop_connection()
        if not task.done():
            task.cancel()
        # Reap the task so no coroutine or 'exception never retrieved'
        # warning is left behind; bounded so cleanup itself cannot hang.
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(task, timeout=_TASK_GRACE_S)
        if not task.done():
            logger.error("probe task still pending after connection close")
