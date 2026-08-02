-- register: add a live worker.
-- ARGV: worker_id, hostname, concurrency
local wid, host, conc = ARGV[1], ARGV[2], ARGV[3]
local now = now_ms()
redis.call('HSET', worker_key(wid), 'id', wid, 'hostname', host, 'concurrency', conc,
  'state', 'ALIVE', 'registered_at', now, 'last_heartbeat', now)
redis.call('ZADD', K_WORKERS, now, wid)
redis.call('SADD', K_WORKERS_ALL, wid)
return now
