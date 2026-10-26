-- expire_workers: declare dead every worker whose last heartbeat is older
-- than dead_after_ms, and immediately revoke every lease it held. The
-- heartbeat age is checked inside the script, so a heartbeat that lands
-- concurrently either happens before (worker survives) or after (worker is
-- already dead and must re-register).
-- ARGV: dead_after_ms, batch
-- Returns {requeued, dead_jobs, {worker_ids}}
local dead_after, batch = tonumber(ARGV[1]), tonumber(ARGV[2])
local now = now_ms()
local wids = redis.call('ZRANGE', K_WORKERS, '-inf', '(' .. (now - dead_after), 'BYSCORE',
  'LIMIT', 0, batch)
local requeued, dead = 0, 0
for _, wid in ipairs(wids) do
  redis.call('ZREM', K_WORKERS, wid)
  redis.call('HSET', worker_key(wid), 'state', 'DEAD', 'died_at', now)
  local wjk = worker_jobs_key(wid)
  for _, id in ipairs(redis.call('SMEMBERS', wjk)) do
    local f = redis.call('HMGET', job_key(id), 'status', 'lease_owner')
    if f[1] == 'LEASED' and f[2] == wid then
      if revoke(id, now, 'worker ' .. wid .. ' declared dead') == 'dead' then
        dead = dead + 1
      else
        requeued = requeued + 1
      end
    end
  end
  redis.call('DEL', wjk)
end
return {requeued, dead, wids}
