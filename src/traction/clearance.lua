-- clearance.lua — token-bucket clearance acquire (one bucket per airport, shared by all pods)
-- KEYS[1] = tr:slots:{airportId}   (hash)
-- ARGV[1] = weight in milli-tokens (1000 = one slot; a gate with cost N sends N * 1000)
-- Returns {granted, wait_ms}:
--   { 1, 0 }       granted, weight deducted
--   { 0, wait_ms } denied, wait_ms until enough tokens accrue
--   {-1, 0 }       bucket unconfigured (missing capacity/refill fields)
local key = KEYS[1]
local weight = tonumber(ARGV[1])
local time = redis.call('TIME')
local now_ms = time[1] * 1000 + math.floor(time[2] / 1000)

local cap = tonumber(redis.call('HGET', key, 'capacity_milli'))
local refill = tonumber(redis.call('HGET', key, 'refill_per_sec_milli'))
if not cap or not refill or refill <= 0 then
  return {-1, 0}
end

local tokens = tonumber(redis.call('HGET', key, 'tokens_milli'))
local last = tonumber(redis.call('HGET', key, 'last_refill_ms'))
if not tokens or not last then
  tokens = cap
  last = now_ms
end

local elapsed = now_ms - last
if elapsed < 0 then elapsed = 0 end
tokens = tokens + math.floor(elapsed * refill / 1000)
if tokens > cap then tokens = cap end

local granted = 0
local wait_ms = 0
if tokens >= weight then
  tokens = tokens - weight
  granted = 1
else
  wait_ms = math.ceil((weight - tokens) * 1000 / refill)
end

redis.call('HSET', key, 'tokens_milli', tokens, 'last_refill_ms', now_ms)
return {granted, wait_ms}
