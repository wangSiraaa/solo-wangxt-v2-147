-- 多桶令牌桶：单次脚本内完成“全部允许才扣减”的原子判定。
--
-- 语义（可核对）：
--   * 桶状态：HASH { tokens, ts }，tokens 为浮点令牌数，ts 为毫秒时间戳
--   * 补充：  tokens = min(capacity, tokens + (now - ts) * rate / 1000)，仅当 now > ts
--   * 时钟：  Redis 服务器 TIME（所有桶、所有应用实例共用同一时钟源）
--   * 消耗：  当且仅当每个桶都有 >= cost 的令牌时，才从每个桶扣 cost；
--             否则任何桶都不扣（拒绝路径只回写确定性的补充结果，不消耗令牌）
--
-- KEYS[1..N]   N 个桶键
-- KEYS[N+1]    幂等键（仅当 idem_ttl_ms > 0 时读写）
-- ARGV[1]      N（桶数量）
-- ARGV[2]      cost（本次消耗令牌数）
-- ARGV[3]      idem_ttl_ms（0 表示不启用判定幂等缓存）
-- ARGV[4+3*(i-1) ..]  每个桶的 capacity, refill_per_sec, ttl_ms
--
-- 返回 {result, replay, rem_1..rem_N}：
--   允许：{"A", "0", ...}
--   拒绝：{"D:<layer_index>:<wait_ms>", "0", ...}，wait_ms = -1 表示当前策略下永远凑不齐
--   幂等重放：{首次判定缓存的 result, "1"}（不重复扣减）

local n        = tonumber(ARGV[1])
local cost     = tonumber(ARGV[2])
local idem_ttl = tonumber(ARGV[3])

-- 1) 幂等重放：同一判定请求在窗口内重试，直接返回首次判定，不再消耗
if idem_ttl > 0 then
  local cached = redis.call('GET', KEYS[n + 1])
  if cached then
    return {cached, '1'}
  end
end

-- 2) 权威时钟：Redis 服务器时间，客户端时钟不参与判定
local t   = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

-- 3) 读取并按确定性公式补充所有桶（此阶段不写回、不扣减）
local caps, rates, ttls, toks = {}, {}, {}, {}
local idx = 4
for i = 1, n do
  caps[i]  = tonumber(ARGV[idx])
  rates[i] = tonumber(ARGV[idx + 1])
  ttls[i]  = tonumber(ARGV[idx + 2])
  idx = idx + 3

  local d = redis.call('HMGET', KEYS[i], 'tokens', 'ts')
  local tk, ts
  if d[1] and d[2] then
    tk = tonumber(d[1])
    ts = tonumber(d[2])
  else
    tk = caps[i]   -- 新桶按满桶初始化
    ts = now
  end
  if now > ts then
    tk = math.min(caps[i], tk + (now - ts) * rates[i] / 1000)
  end
  -- now < ts：上一个写入者时钟更快（时钟偏差），不回退、不补充，保持原状
  toks[i] = tk
end

-- 4) 判定：找出“约束最强”的桶（等待时间最长者），-1 表示策略下不可满足
local allowed     = true
local limit_layer = 0
local max_wait    = 0

local function wait_gt(a, b)
  if a == -1 then return b ~= -1 end
  if b == -1 then return false end
  return a > b
end

for i = 1, n do
  if toks[i] < cost then
    allowed = false
    local wait = -1
    if cost <= caps[i] and rates[i] > 0 then
      wait = math.ceil((cost - toks[i]) * 1000 / rates[i])
    end
    if limit_layer == 0 or wait_gt(wait, max_wait) then
      limit_layer = i
      max_wait    = wait
    end
  end
end

-- 5) 全部允许才扣减；否则只回写补充后的状态，不扣任何桶
local result
if allowed then
  for i = 1, n do
    redis.call('HSET', KEYS[i],
      'tokens', string.format('%.6f', toks[i] - cost),
      'ts', string.format('%.0f', now))
    redis.call('PEXPIRE', KEYS[i], string.format('%.0f', ttls[i]))
  end
  result = 'A'
else
  for i = 1, n do
    redis.call('HSET', KEYS[i],
      'tokens', string.format('%.6f', toks[i]),
      'ts', string.format('%.0f', now))
    redis.call('PEXPIRE', KEYS[i], string.format('%.0f', ttls[i]))
  end
  result = 'D:' .. limit_layer .. ':' .. string.format('%.0f', max_wait)
end

-- 6) 幂等缓存：TTL 有界；拒绝的缓存不超过 retry_after，避免掩盖已恢复的配额
if idem_ttl > 0 then
  local ttl = idem_ttl
  if (not allowed) and max_wait > 0 and max_wait < idem_ttl then
    ttl = max_wait
  end
  redis.call('SET', KEYS[n + 1], result, 'PX', string.format('%.0f', ttl))
end

local out = {result, '0'}
for i = 1, n do
  if allowed then
    out[#out + 1] = string.format('%.6f', toks[i] - cost)
  else
    out[#out + 1] = string.format('%.6f', toks[i])
  end
end
return out
