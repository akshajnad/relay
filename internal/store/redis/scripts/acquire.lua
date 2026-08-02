-- acquire: lease the oldest eligible job to a live worker.
-- ARGV: worker_id, lease_ms
-- Returns {'OK', now, eligible_at, HGETALL(job), more_eligible(0|1)}
--      or {'EMPTY', now, next_eligible_in_ms (-1 if the queue is empty)}
--      or {'NO_WORKER'}
local wid, lease_ms = ARGV[1], tonumber(ARGV[2])
local now = now_ms()

if redis.call('HGET', worker_key(wid), 'state') ~= 'ALIVE' then
  return {'NO_WORKER'}
end

local head = redis.call('ZRANGE', K_READY, '-inf', now, 'BYSCORE', 'LIMIT', 0, 1, 'WITHSCORES')
if #head == 0 then
  local nxt = redis.call('ZRANGE', K_READY, 0, 0, 'WITHSCORES')
  if #nxt == 0 then
    return {'EMPTY', now, -1}
  end
  return {'EMPTY', now, tonumber(nxt[2]) - now}
end

local id, eligible = head[1], tonumber(head[2])
local jk = job_key(id)
redis.call('ZREM', K_READY, id)
redis.call('HINCRBY', jk, 'attempts', 1)
local token = redis.call('HINCRBY', jk, 'lease_token', 1)
local expires = now + lease_ms
redis.call('HSET', jk, 'status', 'LEASED', 'lease_owner', wid, 'lease_expires_at', expires,
  'leased_at', now)
redis.call('ZADD', K_LEASES, expires, id)
redis.call('SADD', worker_jobs_key(wid), id)
event(id, now, 'LEASED', wid, token)
-- Tell the coordinator whether another job is already eligible, so it can
-- pass the wake-up on to the next waiting worker.
local more = 0
if #redis.call('ZRANGE', K_READY, '-inf', now, 'BYSCORE', 'LIMIT', 0, 1) > 0 then
  more = 1
end
return {'OK', now, eligible, redis.call('HGETALL', jk), more}
