# 机房设备故障日志只读查询 API

值班分析员共用的**只读 SQL 入口**：固定两条 PostgreSQL 连接，提交单条
`SELECT` 与**总截止时间**，排队等待也计入预算；超时立即取消数据库语句，
确认事务结束才复用连接，无法确认则弃置连接并补建；迟到结果不会改写超时
回执。后续查询始终拿到属于自己的结果。

## 技术栈

- FastAPI（Swagger UI：`/docs`，ReDoc：`/redoc`）
- psycopg 3（异步连接 + libpq `PQcancel`）
- PostgreSQL 16
- Docker Compose 编排，预置 7 条故障日志与只读角色 `fault_ro`

## 快速开始（Docker Compose）

```bash
docker compose up --build
# API:    http://localhost:8000
# Swagger: http://localhost:8000/docs
```

自测（另开终端，容器内或本机均可）：

```bash
docker compose exec api python -m tests.selftest
```

## 接口

### `POST /query`

```json
{
  "sql": "SELECT id, device_id, severity FROM fault_logs LIMIT 5",
  "timeout_ms": 2000
}
```

`timeout_ms` 是**包含排队时间**的总预算（单调时钟），从请求进入即计时。

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 成功 | 200 | `status=ok`，`rows` 为本次查询自身结果，附 `wait_ms`/`execution_ms` |
| 执行超时 | 504 | `error.code=query_timeout`，`status=timeout`，`cancelled`、`connection_discarded`、`late_result_ignored` |
| 排队期间预算耗尽 | 503 | `error.code=acquire_timeout`，语句**从未发送**到数据库 |
| SQL 不合法 | 400 | `error.code=invalid_sql` |

### `GET /health`

就绪检查（readiness）：**每次**调用都在有界时间内实测数据库能否执行
只读查询（`SELECT 1`），而不是只看连接池计数——PostgreSQL 断连后池里
两条失效连接仍会被计为 `idle=2`，仅凭计数会误报健康。

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 探测成功 | 200 | `status=healthy`，`probe.latency_ms` 为本次实测延迟 |
| 探测失败/超时 | 503 | `status=unhealthy`，`detail`/`probe.error` 给出原因 |

回执同时携带连接池容量、空闲/占用数、累计创建与弃置数（`pool`）。
探测预算由 `HEALTH_PROBE_TIMEOUT_MS` 控制（默认 2000ms；超时后的
清理宽限也是有界常量）。

## 关键设计

### 1. 容量恒为 2，排队计入总预算

`QueryConnectionPool` 用容量为 2 的信号量限制**物理后端数**，第三个请求
排队等待而非新开连接。`acquire(deadline)` 接收绝对单调时钟截止时间，
等待与执行共用同一预算；排队期间过期则直接返回 503，不发送任何语句。

### 2. 单条只读 SELECT 的三层防护

1. `app/validation.py` 仅允许单条 `SELECT`/`WITH`/`TABLE`/`VALUES`，拒绝
   多语句（额外分号）、写关键字、`SELECT ... INTO` 等。
2. 每个请求在显式 `BEGIN READ ONLY` 事务中执行，会话默认
   `default_transaction_read_only=on`，并设置局部 `statement_timeout` 兜底。
3. 数据库角色 `fault_ro` 仅有 `SELECT` 权限，且角色默认只读；直连也无法
   写入（`InsufficientPrivilege`）。

### 3. 超时处理协议（`app/executor.py`）

1. 语句作为 **shielded task** 运行，超时回执一旦确定就不会被迟到结果覆盖。
2. 截止时间到，经 `cancel_safe()`（libpq ≥ 17 的非阻塞取消 API，自带
   有界超时）请求服务器中止语句；阻塞式 `PQcancel` 在等待无响应服务器
   时会持有 GIL，可能冻结整个进程，因此不使用。
3. `await` 该 shielded 任务直至真正结束（收到 `QueryCanceled` 或拿到迟到
   结果），杜绝协程残留。
4. 执行 `ROLLBACK` 并核对 `transaction_status == IDLE`，**确认事务结束**才
   把连接放回池；无法确认则关闭弃置并补建，保持容量为 2。
5. 迟到完成的结果只做审计日志，回执固定为 504 且不携带 `rows`。

### 4. 连接级故障的弃置补建与无感重试

后端被带外终止（如 `pg_terminate_backend`）时，连接级错误触发弃置并由池
补建；只读语句在同一总预算内安全重试一次，客户端无感知。若补建暂时失败，
池临时缩水并在下一次获取时懒建补回，不会死锁。

### 5. 就绪探测（`app/health.py`）

1. 探测使用一条**专用连接**（`application_name=health_probe`），独立于
   两条业务连接：不占用业务后端、不对业务查询发取消；两条业务连接全忙
   而数据库正常时照样报健康。
2. 并发 `/health` 检查在一把锁上串行共用这条探测连接，任何时刻最多一条
   专用探测连接。
3. 每次检查的总预算为 `HEALTH_PROBE_TIMEOUT_MS`（含按需建连与一次
   `SELECT 1`）；探测语句作为 shielded task 运行，超时即定 503，**迟到
   结果只记日志、绝不改报健康**。
4. 探测超时后对探测连接做有界清理（`cancel_safe` 取消 + 短宽限等待），
   随后**关闭弃置**，不遗留半坏后端；下次检查懒建新连接。
5. 常驻探测连接若已陈旧（断连期间被杀），在同一预算内弃旧建新重试一次，
   因此数据库恢复后的第一次检查即可报健康。

## 无 Docker 的本地运行与自测

本机若无容器运行时，可用 `pgserver`（PyPI 上的免 root PostgreSQL 16）：

```bash
pip install -r requirements.txt pgserver
python tests/local_run.py        # 拉起 PG16 + uvicorn
python -m tests.selftest         # 端到端自测（61 项）
python tests/test_protocol.py    # 超时/迟到结果/排队协议直测（11 项）
python tests/test_health.py      # 健康探测混沌测试：断连/满载/恢复（27 项）
```

## 目录结构

```
app/            FastAPI 应用（config/pool/executor/health/validation/schemas/main）
db/01_init.sql  建表、预置日志、只读角色（Compose 首次初始化执行）
tests/          selftest.py（HTTP 端到端）、test_protocol.py（协议直测）、
                test_health.py（健康探测混沌测试）、local_run.py（免 Docker 启动器）
Dockerfile / docker-compose.yml / requirements.txt
```
