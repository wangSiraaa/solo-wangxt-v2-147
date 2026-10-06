# 限流服务平台

多层级（租户 / 用户 / 接口）请求速率限制服务。Go + Gin 提供 API，Redis Lua 在服务端原子完成判定，PostgreSQL 持久化策略。不含前端。

## 架构

```
client ──► Gin API ──► Policy Resolver（内存缓存 + PostgreSQL）
              │
              └──► Redis EVAL（单个 Lua 脚本，原子判定全部桶）
```

- `internal/limiter/`：Lua 脚本与 Go 封装，全有或全无的令牌桶判定
- `internal/policy/`：策略模型、PostgreSQL 存储、带缓存的解析器
- `internal/api/`：Gin 路由（判定 + 策略 CRUD + 时钟核对）
- `cmd/server/`：装配、迁移、种子策略、优雅退出

## 令牌桶语义（可核对）

每个桶是 Redis 中的一个 HASH：`{ tokens, ts }`。

| 概念 | 语义 |
|---|---|
| 突发容量 `capacity` | 令牌数上限，也是任意时刻可消耗的最大突发量 |
| 补充速率 `refill_per_sec` | 每秒线性补充的令牌数（浮点），0 = 不补充 |
| 消耗 `cost` | 单次判定从**每个**相关桶扣除的令牌数，缺省 1 |
| 时钟源 | **Redis 服务器 `TIME`**（Lua 脚本内取取），应用实例与客户端时钟不参与判定 |
| 补充公式 | `tokens = min(capacity, tokens + (now_ms - ts) * rate / 1000)`，仅当 `now > ts` |
| 初始化 | 新桶按满桶（`tokens = capacity`）初始化 |
| 状态过期 | 桶键 TTL = 两次满桶补充时间 + 1 分钟余量，闲置键自动清理 |

时钟偏差处理：`now < ts`（上一个写入者时钟更快）时**不回退、不补充**，保持原状；
`ts` 远在过去时补充被封顶在 `capacity`。两个方向都不会超发。
`GET /v1/clock` 可核对 Redis 时间与本机时间的偏差。

## 原子性：全部允许才消耗

一次判定涉及的所有桶（已配置策略的层级）在**同一个 Lua 脚本**内求值：

1. 读取并按确定性公式补充所有桶（此阶段不写回）；
2. 仅当**每个**桶都有 `>= cost` 的令牌时，才从每个桶各扣 `cost`；
3. 否则**任何桶都不扣**（拒绝路径只回写补充结果，不消耗令牌）。

Redis 单线程执行脚本，判定天然原子。`TestAllOrNothingNoPartialConsumption` 验证：
租户桶不足时，用户桶、接口桶一个令牌都不少。

桶键带租户 hash tag（`rl:{tenant}:t` / `rl:{tenant}:u:{user}` / `rl:{tenant}:e:{endpoint}`），
同一判定的所有键落在同一 slot，兼容 Redis Cluster。接口桶为**租户内**维度；
若需跨租户的全局接口桶，需单机 Redis 或代理层（见“限制与取舍”）。

## 判定幂等（与业务去重分离）

- 客户端在 `POST /v1/check` 中携带 `idempotency_key`：同一键在**幂等窗口**内重试，
  直接返回首次判定结果（`replay: true`），**不重复扣减**——用于网络超时后的安全重试。
- 窗口有界：默认 3s，服务端钳制到上限 60s（`MaxIdemTTL`），**不无限缓存**；
  只有显式携带键的判定才会被缓存。
- 拒绝结果的缓存 TTL 还会被钳到 `retry_after`，避免陈旧拒绝掩盖已恢复的配额。
- **边界**：这里只对“限流判定”去重，不存储、不感知业务负载。业务请求去重
  （如同一订单重复提交）属于业务服务自己的职责，应使用业务侧的唯一约束/去重表，
  不要复用本服务的幂等键。

## Redis 故障策略（按接口风险配置）

`POST /v1/check` 时若 Redis 不可用，按该接口（endpoint）策略的 `fail_open` 决定：

- `fail_open: false`（高风险接口，如支付）→ `503`，`{"allowed": false, "limited_by": "availability"}`
- `fail_open: true`（低风险接口）→ `200`，`{"allowed": true, "degraded": true}`

未配置 endpoint 策略时默认拒绝（fail-closed）。

## API

### `POST /v1/check`

```json
{
  "tenant_id": "acme",
  "user_id": "u42",
  "endpoint": "/orders",
  "cost": 1,
  "idempotency_key": "req-9f1c",
  "idempotency_ttl_ms": 3000
}
```

- `200` 允许：`{"allowed": true, "replay": false, "remaining": {"tenant": 999, "user": 9, "endpoint": 499}}`
- `429` 拒绝：`{"allowed": false, "limited_by": "user", "retry_after_ms": 250, "remaining": {...}}`，
  并带 `Retry-After` 头。`limited_by` 为约束最强的层级（等待时间最长者）；
  `retry_after_ms = -1` 表示当前策略下永远无法满足（如 `cost` 超过容量或速率为 0）。
- `503` 限流器不可用且接口配置为拒绝。

### 策略管理

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/v1/policies` | 创建（同 scope+scope_key 重复返回 409） |
| `GET` | `/v1/policies?scope=` | 列表 |
| `PUT` | `/v1/policies/:id` | 全量替换 `capacity/refill_per_sec/fail_open/enabled` |
| `DELETE` | `/v1/policies/:id` | 删除 |

策略字段：`scope` ∈ `tenant|user|endpoint`，`scope_key` 为精确键或通配
（tenant: `acme` / `*`；user: `acme:u42` / `acme:*` / `*`；endpoint: `/orders` / `*`）。
解析优先级：精确 > 租户通配 > 全局 `*`；停用的策略不参与解析；未配置策略的层级不限制。
变更立即使缓存失效（多实例部署时其余实例最迟在缓存 TTL 5s 后生效）。

注意：令牌只随时间补充，**调大容量不会追溯发放令牌**；桶状态按层级键保留，
策略变更只改变后续补充/封顶所用的参数。

### 其他

- `GET /v1/healthz` 健康检查
- `GET /v1/clock` 返回 `{redis_now_ms, app_now_ms, skew_ms}`，核对判定时钟源

## 运行

```bash
docker compose up --build        # postgres + redis + server(:8080)
# 或本地：
export DATABASE_URL="postgres://postgres:postgres@127.0.0.1:5432/ratelimit?sslmode=disable"
export REDIS_ADDR="127.0.0.1:6379"
go run ./cmd/server
```

启动时自动执行迁移（`internal/policy/migrations/001_init.sql`）并写入三层 `*` 兜底策略。

环境变量：`PORT`（8080）、`REDIS_ADDR`、`DATABASE_URL`。

## 测试

```bash
go test ./...          # 全部
go test -race ./...    # 含竞态检测
```

覆盖要点（不只核对平均 QPS，而是核对允许量上界）：

| 测试 | 核对内容 |
|---|---|
| `TestConcurrentUpperBound` | 32 协程压测：`consumed ≤ capacity + rate×elapsed + cost`，且余量永不为负；实测 consumed=80 vs 上界 81 |
| `TestConcurrentBurstExactlyCapacity` | 100 并发突发，rate=0 时允许量**恰好**等于容量 |
| `TestAllOrNothingNoPartialConsumption` | 任一层拒绝时其它桶零消耗 |
| `TestClockSkewFutureTimestamp` / `TestClockSkewPastTimestampCappedAtCapacity` | 时钟偏差两个方向都不超发、不越容量 |
| `TestRefillFormula` | 补充公式与容量封顶的确定性核对 |
| `TestIdempotencyReplayAndExpiry` / `TestDenialCachedOnlyUntilRetryAfter` | 幂等窗口内重放不重复扣减；窗口有界；拒绝缓存 TTL ≤ retry_after |
| `TestPolicyChangeTakesEffectImmediately` | 策略变更/停用立即生效，回退优先级正确 |
| `TestRedisDownFailOpen` / `TestRedisDownFailClosed` | Redis 故障按接口风险配置开放或拒绝 |
| `TestPgStoreRoundTrip` | PostgreSQL 集成（设 `DATABASE_URL` 后运行，否则跳过） |

## 限制与取舍

- 接口桶为租户内维度（hash tag 保证 Cluster 同 slot 原子性）；跨租户全局接口桶需单机 Redis 或代理。
- 多实例部署时策略变更加缓存 TTL（默认 5s）内在全集群生效；单实例写入路径立即生效。
- 令牌为浮点、存储精度 1e-6，对限流语义无影响。
- 判定幂等是短窗口重试保护，不是业务去重；业务去重请在业务侧实现。
