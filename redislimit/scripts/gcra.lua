-- KEYS[1]: theoretical arrival time in microseconds
-- ARGV: now_us, interval_us, burst, n, reserve (0|1), max_wait_us
local key = KEYS[1]
local now = now_us()
local interval = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local n = tonumber(ARGV[4])
local reserve = ARGV[5] == '1'
local max_wait = tonumber(ARGV[6])

local tau = interval * burst
local tat = tonumber(redis.call('GET', key))
if tat == nil or tat < now then
  tat = now
end
local new_tat = tat + interval * n
local diff = new_tat - tau - now

local allowed = 0
local retry = 0
local delay = 0
if diff <= 0 then
  allowed = 1
elseif reserve and diff <= max_wait then
  allowed = 1
  delay = diff
else
  retry = diff
end

if allowed == 1 then
  tat = new_tat
  redis.call('SET', key, int(tat), 'PX', expire_ms(tat - now))
end

local remaining = math.max(math.floor((tau - (tat - now)) / interval), 0)
return {allowed, remaining, retry, tat - now, delay}
