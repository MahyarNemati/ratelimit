-- KEYS[1]: hash {w = current window index, c = current count, p = previous count}
-- ARGV: now_us, window_us, limit, n
local key = KEYS[1]
local now = now_us()
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local n = tonumber(ARGV[4])

local idx = math.floor(now / window)
local state = redis.call('HMGET', key, 'w', 'c', 'p')
local w = tonumber(state[1])
local curr = tonumber(state[2]) or 0
local prev = tonumber(state[3]) or 0
if w == nil then
  w = idx
end
if idx == w + 1 then
  prev = curr
  curr = 0
elseif idx > w + 1 then
  prev = 0
  curr = 0
end
if idx > w then
  w = idx
end

local start = w * window
local elapsed = math.max(now - start, 0)
local weighted = prev * (window - elapsed) + curr * window
local need = limit - n

local allowed = 0
local retry = 0
if weighted <= need * window then
  curr = curr + n
  weighted = weighted + n * window
  allowed = 1
elseif curr <= need then
  local e = window - math.floor((need - curr) * window / prev)
  retry = start + e - now
else
  local e = window - math.floor(need * window / curr)
  retry = start + window + e - now
end

local remaining = math.max(math.floor((limit * window - weighted) / window), 0)
local reset = 0
if curr > 0 then
  reset = start + 2 * window - now
elseif prev > 0 then
  reset = start + window - now
end

redis.call('HSET', key, 'w', int(w), 'c', int(curr), 'p', int(prev))
redis.call('PEXPIRE', key, expire_ms(math.max(reset, 1)))

return {allowed, remaining, retry, reset, 0}
