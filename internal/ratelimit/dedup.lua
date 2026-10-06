-- ratelimit-platform Lua: business-request deduplication.
--
-- Separate from decision idempotency on purpose:
--   - key namespace: rl:dedup:{tenant}:<dedup-key>  (vs rl:idem: for decisions)
--   - TTL: business window (typically hours), decision replay window (seconds)
--   - meaning: "this business request was already accepted" rather than
--     "this rate-limit decision was already computed"
--
-- Clock source is again Redis TIME; ARGV:
--   1 dedup_key
--   2 fingerprint   sha256 hex of canonical business payload
--   3 ttl_ms        business dedup window
-- Returns flat array:
--   {"OK"|"DUPLICATE"|"CONFLICT", now_ms}
--   OK        -> key did not exist, stored payload + TTL
--   DUPLICATE -> key exists with same fingerprint, no write/TTL refresh
--   CONFLICT  -> same key, different fingerprint; nothing is overwritten

local _t = redis.call('TIME')
local now_ms = tonumber(_t[1]) * 1000 + math.floor(tonumber(_t[2]) / 1000)

local dedup_key = ARGV[1]
local fingerprint = ARGV[2]
local ttl_ms = tonumber(ARGV[3])

local stored = redis.call('GET', dedup_key)
if stored == false or stored == nil then
    redis.call('SET', dedup_key, fingerprint, 'PX', ttl_ms)
    return { 'OK', tostring(now_ms) }
end

if stored == fingerprint then
    return { 'DUPLICATE', tostring(now_ms) }
end
return { 'CONFLICT', tostring(now_ms) }
