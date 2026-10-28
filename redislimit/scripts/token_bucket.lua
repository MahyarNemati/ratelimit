-- KEYS[1]: bucket hash {t = tokens, u = last refill}
-- ARGV: now_us, rate, period_us, burst, n, reserve (0|1), max_wait_us
local key = KEYS[1]
local now = now_us()
local rate = tonumber(ARGV[2])
local period = tonumber(ARGV[3])
local burst = tonumber(ARGV[4])
local n = tonumber(ARGV[5])
local reserve = ARGV[6] == '1'
local max_wait = tonumber(ARGV[7])

local state = redis.call('HMGET', key, 't', 'u')
local tokens = tonumber(state[1])
local updated = tonumber(state[2])
if tokens == nil or updated == nil then
  tokens = burst
  updated = now
end
if now > updated then
  tokens = math.min(burst, tokens + (now - updated) * rate / period)
  updated = now
end

local allowed = 0
local delay = 0
if tokens >= n then
  tokens = tokens - n
  allowed = 1
elseif reserve then
  local d = math.ceil((n - tokens) * period / rate)
  if d <= max_wait then
    tokens = tokens - n
    allowed = 1
    delay = d
  end
end

local retry = 0
if allowed == 0 then
  retry = math.ceil((n - tokens) * period / rate)
end
local reset = math.ceil((burst - tokens) * period / rate)

redis.call('HSET', key, 't', string.format('%.17g', tokens), 'u', int(updated))
redis.call('PEXPIRE', key, expire_ms(reset))

return {allowed, math.floor(math.max(tokens, 0)), retry, reset, delay}
