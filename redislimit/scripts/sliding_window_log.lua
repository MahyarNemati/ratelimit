-- KEYS[1]: sorted set of admitted requests scored by time in microseconds
-- ARGV: now_us, window_us, limit, n, member_prefix
local key = KEYS[1]
local now = now_us()
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local n = tonumber(ARGV[4])
local prefix = ARGV[5]

redis.call('ZREMRANGEBYSCORE', key, '-inf', int(now - window))
local count = redis.call('ZCARD', key)

if count + n <= limit then
  local score = int(now)
  for i = 1, n do
    redis.call('ZADD', key, score, prefix .. ':' .. i)
  end
  redis.call('PEXPIRE', key, expire_ms(window))
  return {1, limit - count - n, 0, window, 0}
end

local idx = count + n - limit - 1
local oldest = redis.call('ZRANGE', key, idx, idx, 'WITHSCORES')
local newest = redis.call('ZRANGE', key, -1, -1, 'WITHSCORES')
local retry = tonumber(oldest[2]) + window - now
local reset = tonumber(newest[2]) + window - now
return {0, math.max(limit - count, 0), retry, reset, 0}
