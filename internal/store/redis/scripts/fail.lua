-- fail: record a failed attempt, fenced by owner and lease token.
-- Retryable failures with attempts left go back to ready after an
-- exponential backoff with equal jitter: delay = d/2 + jitter * d/2 where
-- d = min(max, base * 2^(attempts-1)). Exhausted retryable failures go to
-- DEAD; non-retryable failures go to FAILED.
-- ARGV: job_id, worker_id, lease_token, error, retryable(0|1), base_ms, max_ms, jitter[0,1)
-- Returns {'RETRY', delay_ms} | {'FAILED'} | {'DEAD'} | {'ALREADY', status}
--       | {'NOT_FOUND'} | {'STALE', status, owner, token}
local id, wid, token, err, retryable = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5] == '1'
local base, maxd, jitter = tonumber(ARGV[6]), tonumber(ARGV[7]), tonumber(ARGV[8])
local jk = job_key(id)
local f = redis.call('HMGET', jk, 'status', 'lease_owner', 'lease_token', 'reported_token',
  'attempts', 'max_attempts')
local status, owner, cur = f[1], f[2], f[3]

if not status then
  return {'NOT_FOUND'}
end
if owner ~= wid or cur ~= token then
  return {'STALE', status, owner, cur}
end
if status ~= 'LEASED' then
  if f[4] == token then
    return {'ALREADY', status}
  end
  return {'STALE', status, owner, cur}
end

local now = now_ms()
local attempts, max_attempts = tonumber(f[5]), tonumber(f[6])
redis.call('ZREM', K_LEASES, id)
redis.call('SREM', worker_jobs_key(wid), id)

if retryable and attempts < max_attempts then
  local d = math.min(maxd, base * 2 ^ (attempts - 1))
  local delay = math.floor(d / 2 + jitter * d / 2)
  redis.call('HSET', jk, 'status', 'PENDING', 'last_error', err, 'lease_expires_at', 0,
    'enqueued_at', now + delay, 'reported_token', token)
  redis.call('ZADD', K_READY, now + delay, id)
  event(id, now, 'RETRY', wid, token, err)
  return {'RETRY', delay}
end

local final = 'FAILED'
if retryable then
  final = 'DEAD'
  redis.call('ZADD', K_DEAD, now, id)
end
redis.call('HSET', jk, 'status', final, 'last_error', err, 'finished_at', now,
  'lease_expires_at', 0, 'reported_token', token)
event(id, now, final, wid, token, err)
return {final}
