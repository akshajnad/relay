-- complete: record success, fenced by owner and lease token.
-- ARGV: job_id, worker_id, lease_token, result
-- Returns {'OK'} | {'ALREADY'} | {'NOT_FOUND'} | {'STALE', status, owner, token}
local id, wid, token, result = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local jk = job_key(id)
local f = redis.call('HMGET', jk, 'status', 'lease_owner', 'lease_token', 'reported_token')
local status, owner, cur = f[1], f[2], f[3]

if not status then
  return {'NOT_FOUND'}
end
-- Without FENCING (relay_nofence test builds only), any report on a leased
-- job is accepted, whoever holds the lease.
if not FENCING and status == 'LEASED' then
  owner, cur = wid, token
end
if owner == wid and cur == token then
  if status == 'LEASED' then
    local now = now_ms()
    redis.call('HSET', jk, 'status', 'SUCCEEDED', 'result', result, 'finished_at', now,
      'lease_expires_at', 0, 'reported_token', token)
    redis.call('ZREM', K_LEASES, id)
    redis.call('SREM', worker_jobs_key(wid), id)
    event(id, now, 'SUCCEEDED', wid, token)
    return {'OK'}
  end
  if status == 'SUCCEEDED' and f[4] == token then
    -- This lease already completed the job: a retried RPC. Don't rewrite.
    return {'ALREADY'}
  end
end
return {'STALE', status, owner, cur}
