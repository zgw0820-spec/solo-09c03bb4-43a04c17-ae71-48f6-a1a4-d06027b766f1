"""Request/response schemas surfaced in the OpenAPI (Swagger) document."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    sql: str = Field(
        ...,
        description=(
            "单条只读 SELECT 语句（允许 WITH/VALUES/TABLE，"
            "禁止多语句、写操作、SELECT INTO 等）"
        ),
        examples=["SELECT id, device_id, severity FROM fault_logs LIMIT 5"],
    )
    timeout_ms: int = Field(
        ...,
        ge=1,
        le=60_000,
        description=(
            "总截止时间（毫秒），从请求进入起计时，包含排队等连接的时间"
        ),
    )


class QueryResponse(BaseModel):
    status: Literal["ok", "timeout"]
    executed: bool = Field(
        description="语句是否真正发送到了数据库后端"
    )
    wait_ms: float | None = None
    execution_ms: float | None = None
    row_count: int | None = None
    rows: list[list[Any]] | None = None
    cancelled: bool | None = None
    connection_discarded: bool | None = None
    late_result_ignored: bool | None = None
    detail: str | None = None


class ErrorResponse(BaseModel):
    error: dict[str, str]


class HealthResponse(BaseModel):
    status: Literal["healthy", "unhealthy"] = Field(
        description=(
            "healthy：本次探测确认数据库可在有界时间内执行只读查询；"
            "unhealthy：探测失败（HTTP 503），detail 给出原因"
        )
    )
    detail: str = Field(description="本次就绪探测的结论或失败原因")
    probe: dict[str, Any] = Field(
        description="专用探测连接本次检查的结果（ok/latency_ms/error）"
    )
    pool: dict[str, Any] = Field(
        description="业务连接池计数（容量、空闲/占用、累计创建与弃置）"
    )
