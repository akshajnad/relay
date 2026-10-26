-- reap: the backstop for failure detection. Requeue (or kill) every job
-- whose lease expired without being renewed.
-- ARGV: batch
-- Returns {requeued, dead, scanned}
local batch = tonumber(ARGV[1])
local now = now_ms()
local ids = redis.call('ZRANGE', K_LEASES, '-inf', now, 'BYSCORE', 'LIMIT', 0, batch)
local requeued, dead = 0, 0
for _, id in ipairs(ids) do
  if redis.call('HGET', job_key(id), 'status') == 'LEASED' then
    if revoke(id, now, 'lease expired') == 'dead' then
      dead = dead + 1
    else
      requeued = requeued + 1
    end
  else
    redis.call('ZREM', K_LEASES, id)
  end
end
return {requeued, dead, #ids}
