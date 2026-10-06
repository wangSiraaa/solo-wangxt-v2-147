-- ratelimit-platform Lua: atomic multi-bucket token-bucket decision.
--
-- Clock source: Redis server TIME (seconds, microseconds) converted to
-- INTEGER MILLISECONDS. No client-supplied clock is trusted for state math.
--
-- Token math uses INTEGER MILLITOKENS (1 token = 1000 millitokens):
--   refill_mt = floor(elapsed_ms * rate_mtps / 1000)
-- All values stay well inside Lua 5.1 double-int-safe range under the
-- documented caps (capacity <= 1_000_000_000 mT, rate <= 1_000_000_000 mT/s).
--
-- ARGV layout:
--   1 cost_mt            tokens requested for THIS decision, in millitokens
--   2 idem_key           idempotency key for the DECISION ("" = disabled)
--   3 fingerprint        sha256 hex of the canonical request payload
--   4 idem_ttl_allow_ms  decision replay window after an ALLOW
--   5 idem_ttl_deny_ms   decision replay window after a DENY (bounded)
--   6 n                  number of buckets
--   main block: per bucket 5 fields (key, capacity_mt, rate_mtps, version, idle_ms)
--   tail block: per bucket 3 fields (layer, name, policy_id)
--
-- KEYS[i] mirrors bucket i (so Redis Cluster routes consistently per script).
--
-- Protocol (flat array, positions stable for the Go decoder):
--   [1] = "ALLOW" | "DENY" | "IDEM"
--   [2] = now_ms (authoritative Redis clock at script entry)
--   [3] = cost_mt
--   DENY additionally: wait_ms (ms until cost fits; 0 => never fits)
--                      limited_idx (1-based; lowest evaluated layer wins)
--                      n_bucket then per bucket:
--                      remain_mt cap_mt rate_mtps layer name
--   ALLOW additionally: n_bucket then per bucket (post-consumption state)
--   IDEM : 1 element then the stored decision snapshot JSON
--
-- Crucial atomicity guarantee: the read pass never issues writes; if ANY
-- bucket is deficient the script returns DENY having mutated nothing. Bucket
-- state is written only in the final ALLOW loop, so tokens are never consumed
-- from a subset of buckets.

-- redis.call('TIME') returns ONE 2-element array {seconds, microseconds}
-- (bulk strings in both real Redis and miniredis); convert explicitly.
local _t = redis.call('TIME')
local now_ms = tonumber(_t[1]) * 1000 + math.floor(tonumber(_t[2]) / 1000)

local cost_mt = tonumber(ARGV[1])
local idem_key = ARGV[2]
local fingerprint = ARGV[3]
local idem_ttl_allow = tonumber(ARGV[4])
local idem_ttl_deny = tonumber(ARGV[5])
local n = tonumber(ARGV[6])

local main_base = 7
local function main(i, field) return ARGV[main_base + i * 5 + field] end
local tail_base = main_base + n * 5
local function tail(i, field) return ARGV[tail_base + i * 3 + field] end

-- Idempotency fast path: a replay returns the ORIGINAL decision snapshot and
-- performs no new accounting. The fingerprint is embedded in the snapshot, so
-- key reuse with a DIFFERENT payload is rejected atomically (CONFLICT); the
-- stored decision is never overwritten. This window is scoped to decision
-- retries only and is TTL-bounded.
if idem_key ~= '' then
    local snap = redis.call('GET', idem_key)
    if snap and snap ~= false then
        if string.find(snap, '"fp":"' .. fingerprint .. '"', 1, true) ~= nil then
            return { 'IDEM', tostring(now_ms), tostring(cost_mt), snap }
        end
        return { 'CONFLICT', tostring(now_ms), tostring(cost_mt) }
    end
end

-- Read pass: evaluate every bucket against current state. No writes here.
local remain = {}
local caps = {}
local rates = {}
local ready = true
local wait_ms = -1
local limited_idx = -1

for i = 0, n - 1 do
    local bkey = main(i, 0)
    local cap_mt = tonumber(main(i, 1))
    local rate_mtps = tonumber(main(i, 2))
    local version = main(i, 3)

    local data = redis.call('HMGET', bkey, 'tokens', 'ts', 'ver')
    local tokens = tonumber(data[1])
    local ts = tonumber(data[2])
    local ver = data[3]

    if tokens == nil then
        -- First time we see this bucket: start full AND anchor its timestamp
        -- to NOW. The first request gets exactly `capacity` admitted and no
        -- phantom refill is credited for the bucket's unobserved lifetime.
        tokens = cap_mt
        ts = now_ms
    else
        if ver ~= version then
            -- Policy changed since last call: clamp to the new burst
            -- capacity; subsequent refills use the new rate.
            if tokens > cap_mt then tokens = cap_mt end
        end
        local elapsed = now_ms - ts
        if elapsed > 0 then
            local refill = math.floor(elapsed * rate_mtps / 1000)
            if refill > 0 then
                tokens = tokens + refill
                if tokens > cap_mt then tokens = cap_mt end
            end
        end
        -- Monotonic guard: a backwards clock jump yields elapsed <= 0;
        -- we never credit negative refill, and the write pass (on allow)
        -- re-anchors ts. Denies do not write, so the state stays untouched.
    end

    remain[i + 1] = tokens
    caps[i + 1] = cap_mt
    rates[i + 1] = rate_mtps

    -- First deficient bucket in evaluation order is reported; evaluation
    -- order is fixed (tenant, user, api) by the Go encoder.
    if ready and tokens < cost_mt then
        ready = false
        limited_idx = i + 1
        if rate_mtps <= 0 then
            wait_ms = 0 -- never refills: cost never fits
        else
            local need = cost_mt - tokens
            wait_ms = math.ceil(need * 1000 / rate_mtps)
        end
    end
end

local function append_rows(out)
    out[#out + 1] = tostring(n)
    for i = 0, n - 1 do
        out[#out + 1] = tostring(remain[i + 1])
        out[#out + 1] = tostring(caps[i + 1])
        out[#out + 1] = tostring(rates[i + 1])
        out[#out + 1] = tail(i, 0) -- layer
        out[#out + 1] = tail(i, 1) -- name
    end
    return out
end

-- Snapshot for the bounded decision-idempotency window. It captures the
-- ORIGINAL decision; replays never re-charge and never re-deduct.
local function snapshot(status, swait, slimited)
    local parts = {
        '"fp":"' .. fingerprint .. '"',
        '"status":"' .. status .. '"',
        '"now_ms":' .. tostring(now_ms),
        '"cost_mt":' .. tostring(cost_mt),
        '"wait_ms":' .. tostring(swait),
        '"limited_idx":' .. tostring(slimited),
    }
    local brows = {}
    for i = 0, n - 1 do
        brows[#brows + 1] = '[' .. tostring(remain[i + 1]) .. ','
            .. tostring(caps[i + 1]) .. ',' .. tostring(rates[i + 1]) .. ',"'
            .. tail(i, 0) .. '","' .. tail(i, 1) .. '","' .. tail(i, 2)
            .. '"]'
    end
    parts[#parts + 1] = '"buckets":[' .. table.concat(brows, ',') .. ']'
    return '{' .. table.concat(parts, ',') .. '}'
end

if not ready then
    -- DENY: zero bucket mutations. Only the bounded replay snapshot is stored.
    if idem_key ~= '' then
        redis.call('SET', idem_key, snapshot('DENY', wait_ms, limited_idx),
            'PX', idem_ttl_deny)
    end
    local out = { 'DENY', tostring(now_ms), tostring(cost_mt),
        tostring(wait_ms), tostring(limited_idx) }
    return append_rows(out)
end

-- Write pass: every bucket was sufficient; consume from ALL of them.
for i = 0, n - 1 do
    local bkey = main(i, 0)
    local version = main(i, 3)
    local idle_ms = tonumber(main(i, 4))
    -- Post-consumption state, also what the ALLOW response reports.
    remain[i + 1] = remain[i + 1] - cost_mt
    redis.call('HSET', bkey,
        'tokens', remain[i + 1],
        'ts', now_ms,
        'ver', version)
    -- Idle expiry: reclaimed shortly after a full refill would complete, so
    -- idle keys do not accumulate forever while bursts stay correct.
    redis.call('PEXPIRE', bkey, idle_ms)
end

if idem_key ~= '' then
    redis.call('SET', idem_key, snapshot('ALLOW', 0, 0),
        'PX', idem_ttl_allow)
end

local out = { 'ALLOW', tostring(now_ms), tostring(cost_mt) }
return append_rows(out)
