# ratelimit-platform

多维度（**租户 / 用户 / 接口**）令牌桶限流服务：Go + Gin 提供限流 API，单个
Redis Lua 脚本完成原子判定，PostgreSQL 保存策略。无前端。

## 1. 令牌桶语义（可核对）

- **标准令牌桶，整数记账**：内部单位为毫令牌（1 token = 1000 mT）与毫秒，
  全程 `int64`，无浮点漂移。
  - 突发容量 `capacity_mt`：桶**首次出现时为满**，且时间戳锚定在"本次判定
    时刻"——首次放行不会因为桶的未知历史时间而白送补充令牌。
  - 补充速率 `refill_mtps`（毫令牌/秒），与容量独立配置；补充公式
    `refill = floor(elapsed_ms * rate_mtps / 1000)`，上限钳制为容量。
  - 速率为 0 时，`wait_ms=0` 表示"永不放行"（`never_retry=true`）。
- **时钟源**：所有记账时间都取自 Lua 脚本内的 `redis.call('TIME')`（服务端
  秒+微秒 → 整数毫秒）。调用方时钟**只用于偏差观测**，响应中返回
  `redis_now_ms` / `local_now_ms` / `clock_skew_ms`，绝不参与扣减。
- **策略版本**：策略更新会 bump `version`；脚本发现桶内 `ver` 与传入版本不
  一致时，先把存量令牌钳制到新容量，再按新速率补充。

## 2. 原子的"全或无"扣减

`internal/ratelimit/decide.lua` 在一个脚本内完成：

1. **只读遍历**全部相关桶（tenant → user → api 固定顺序），逐桶补充、判断；
2. 只要**任一**桶不足 → 返回 `DENY`，**不写任何桶**（连时间戳都不更新）；
3. 只有全部充足才进入写遍历，从**每一个**桶扣减相同 `cost`。

因此不存在"租户桶扣了、用户桶没扣"的部分扣减。判定拒绝时返回首个不足层
（`limited_by_layer` / `limited_by_name`）与可重试时间
（`retry_after_ms`，以及 HTTP `Retry-After`）。

同一租户的所有桶 key 带相同 hash tag `rl:bucket:{t:<tenant>}:...`，Redis
Cluster 下多 key EVAL 落在同一 slot。空闲 key 在"补满所需时长 + 下限"后
过期，避免无限增长。

## 3. 两个相互独立的幂等/去重概念

| | 判定重试幂等 | 业务请求去重 |
|---|---|---|
| 端点 | `POST /api/v1/ratelimit/check` 的 `idempotency_key` | `POST /api/v1/business/dedup` |
| Key 命名空间 | `rl:idem:{tenant}:<key>` | `rl:dedup:{tenant}:<key>` |
| 含义 | 同一判定请求重放**原结论**，不再计费 | 同一业务请求只被接受一次 |
| TTL | 有界（允许默认 60s、拒绝默认 5s，可配置） | 业务窗口（默认 24h，单次请求最长 7 天） |
| 指纹 | 请求规范化后 sha256，写进快照；同 key 不同负载原子返回 `409 CONFLICT`，旧快照绝不覆盖 | 业务负载 sha256；同 key 不同负载 `409` |

窗口到期后即重新正常判定，**不会无限缓存所有请求**。

## 4. Redis 故障策略按接口风险配置

接口在 PostgreSQL（或内存后端）中有 `fail_policy`：

- `closed`（高风险，如 login/pay）：Redis 不可用 → `503`，`allowed=false`；
- `open`（低风险，如 search/docs）：→ `200` 放行，但显式返回
  `"degraded": true`、`clock_source:"unavailable"`，表示本次未计费，便于审计。

## 5. API

### 判定

```
POST /api/v1/ratelimit/check
{
  "tenant": "acme", "user": "alice", "api": "login",
  "cost_tokens": 1.0,                 // 可选，最多 3 位小数
  "idempotency_key": "client-req-1"  // 可选，判定重试窗口
}
```

- `200`：`allowed:true`，含三层桶扣减后的剩余量（令牌与毫令牌两种表示）、
  Redis 时钟、与本地时钟偏差；
- `429`：`allowed:false`、`limited_by_layer`/`limited_by_name`、
  `retry_after_ms`、`Retry-After` 头、各桶当前余量；
- `409`：幂等键被不同负载复用；
- `503`：限流器不可用且接口策略为 fail-closed；
- `200 + degraded:true`：不可用但 fail-open。

### 业务去重

```
POST /api/v1/business/dedup
{"tenant":"acme","dedup_key":"order-42","payload":"<canonical body>","ttl_seconds":86400}
```
→ `{"first":true}` / `{"duplicate":true}` / `409`（同 key 不同 payload）。

### 管理端（策略存储）

- `GET/POST /admin/v1/policies`，`PUT/DELETE /admin/v1/policies/:id`
  （`capacity_mt`/`refill_mtps`，或 `capacity_tokens`/`refill_per_sec`
  十进制字段）；
- `GET/PUT/DELETE /admin/v1/endpoints/:id`（`fail_policy: open|closed`）；
- `GET/PUT/DELETE /admin/v1/bindings`（layer=tenant|user|api；subject 留空
  表示该层默认策略；具体绑定优先于默认）；
- `POST /admin/v1/cache/refresh`：手动刷新读模型快照。正常情况下 PostgreSQL
  表级触发器 `pg_notify('rl_policy_changed', ...)` → 监听器 → 缓存热更新，
  定时刷新仅作兜底。

### 时钟核对

`GET /api/v1/clock` 返回 `redis:TIME` 毫秒、本地毫秒及偏差。

## 6. 运行

无外部依赖开发模式（策略在内存，Redis 仍必需）：

```bash
redis-server --save '' --appendonly no
go run ./cmd/server            # POLICY_BACKEND=memory, 自动种入演示策略
```

PostgreSQL：

```bash
docker compose -f deployments/docker-compose.yml up --build
# 或
POSTGRES_DSN='postgres://ratelimit:ratelimit@127.0.0.1:5432/ratelimit?sslmode=disable' \
  go run ./cmd/server
```

环境变量见 `.env.example`。Schema 见 `internal/policy/schema.sql`（启动自动
迁移）。

## 7. 压测与测试（核对的是允许量上界，不是平均 QPS）

压测工具对 200ms / 500ms / 1s / 2s / 5s **多个滑动窗口**统计放行数，并与
令牌桶上界 `capacity + rate*T (+1 离散余量)` 逐一比较；另检查首秒突发不超过
`cap+rate`：

```bash
go run ./cmd/stress -url http://127.0.0.1:8080 \
  -tenant stress-t -user stress-u -api search \
  -capacity 50 -rate 20 -duration 6s -concurrency 32
```

实测（32 并发，6s，约 10.7 万次判定）各窗口全部满足上界。

测试矩阵：

- 引擎（miniredis，时钟冻结/快进）：突发上界、拒绝零写入、毫秒级补充、
  策略缩容钳制/扩容、判定幂等窗口与指纹冲突、业务去重独立 TTL、cost 静态
  拒绝、200 并发精确放行 = 突发量（`-race` 通过）；
- 引擎（**真实 redis-server 7.4**）：真实 TIME 突发/补充、并发上界、拒绝
  不创建其他层 key、多层取最紧层、策略热切换；
- 时钟偏差：Redis 时钟**回拨**不产生负补充/丢令牌；前跳 2s 最多多给
  `rate*2` 令牌（标准偏差上界）；
- 策略变更：HTTP 管理端改策略 → 刷快照 → 桶按新版本钳制；PostgreSQL 触发器
  版本递增、NOTIFY 失效通知；
- HTTP：429 层归属与 Retry-After、fail-open/closed、业务去重 409、时钟偏差
  只观测不计账；
- PostgreSQL（embedded 真实实例）：迁移、CRUD、默认/具体绑定唯一约束、
  NOTIFY。

```bash
go test ./... -timeout 180s          # 全部
RL_REQUIRE_REDIS=1 go test ./...      # 强制要求真实 redis-server
RL_REQUIRE_POSTGRES=1 go test ./...   # 强制要求嵌入式 PostgreSQL
```

## 8. 关键文件

- `internal/ratelimit/decide.lua` — 原子多桶判定脚本
- `internal/ratelimit/dedup.lua` — 业务去重脚本（独立命名空间）
- `internal/ratelimit/engine.go` — Go 引擎、整数编码、错误语义
- `internal/policy/` — 领域模型、内存/PostgreSQL 存储、快照缓存、迁移 SQL
- `internal/api/` — Gin 处理器与管理端
- `cmd/server`、`cmd/stress` — 服务端与带上界审计的压测工具
