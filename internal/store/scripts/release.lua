-- release: hand back a lease whose AcquireJob response could not be
-- delivered (the caller had already gone away). The attempt is undone and
-- the job returns to the front of the queue at its previous eligible time.
-- ARGV: job_id, worker_id, lease_token
-- Returns {'OK'} | {'STALE'}
local id, wid, token = ARGV[1], ARGV[2], ARGV[3]
local jk = job_key(id)
local f = redis.call('HMGET', jk, 'status', 'lease_owner', 'lease_token', 'enqueued_at')
if f[1] ~= 'LEASED' or f[2] ~= wid or f[3] ~= token then
  return {'STALE'}
end
local now = now_ms()
redis.call('HINCRBY', jk, 'attempts', -1)
redis.call('HSET', jk, 'status', 'PENDING', 'lease_expires_at', 0)
redis.call('ZREM', K_LEASES, id)
redis.call('SREM', worker_jobs_key(wid), id)
redis.call('ZADD', K_READY, tonumber(f[4]), id)
event(id, now, 'RELEASED', wid, token, 'acquire response undeliverable')
return {'OK'}
