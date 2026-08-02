-- heartbeat: record worker liveness and renew the leases it reports holding.
-- Only reported leases are renewed. A lease the worker never learned about
-- (its AcquireJob response was lost) is not renewed, so it expires and the
-- reaper requeues it.
-- ARGV: worker_id, lease_ms, then (job_id, lease_token) pairs
-- Returns {'OK', {lost job_id, lease_token pairs}} or {'NO_WORKER'}
local wid, lease_ms = ARGV[1], tonumber(ARGV[2])
local now = now_ms()
local wk = worker_key(wid)

if redis.call('HGET', wk, 'state') ~= 'ALIVE' then
  return {'NO_WORKER'}
end
redis.call('ZADD', K_WORKERS, now, wid)
redis.call('HSET', wk, 'last_heartbeat', now)

local lost = {}
local expires = now + lease_ms
for i = 3, #ARGV, 2 do
  local id, token = ARGV[i], ARGV[i + 1]
  local f = redis.call('HMGET', job_key(id), 'status', 'lease_owner', 'lease_token')
  if f[1] == 'LEASED' and f[2] == wid and f[3] == token then
    redis.call('HSET', job_key(id), 'lease_expires_at', expires)
    redis.call('ZADD', K_LEASES, expires, id)
  else
    table.insert(lost, id)
    table.insert(lost, token)
  end
end
return {'OK', lost}
