-- Shared helpers. ARGV[1] is the caller's clock in microseconds, or -1 to
-- use the Redis server clock, which keeps every app instance on one timeline.
local function now_us()
  local t = tonumber(ARGV[1])
  if t >= 0 then
    return t
  end
  local tm = redis.call('TIME')
  return tonumber(tm[1]) * 1000000 + tonumber(tm[2])
end

local function int(x)
  return string.format('%.0f', x)
end

local function expire_ms(us)
  return math.max(math.ceil(us / 1000), 1)
end
