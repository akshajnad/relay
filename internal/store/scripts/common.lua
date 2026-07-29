-- Shared prelude, prepended to every script at load time.
--
-- Every timestamp is Redis server time in milliseconds (TIME), so lease and
-- backoff math never depends on the coordinator's clock. Job and worker keys
-- are derived inside the scripts (acquire and reap only learn job ids at run
-- time), which is fine on a standalone Redis but not Redis Cluster safe.

local PREFIX = 'relay:'
local K_READY = PREFIX .. 'ready'
local K_LEASES = PREFIX .. 'leases'
local K_DEAD = PREFIX .. 'dead'
local K_WORKERS = PREFIX .. 'workers'
local K_WORKERS_ALL = PREFIX .. 'workers:all'

local function job_key(id) return PREFIX .. 'job:' .. id end
local function events_key(id) return PREFIX .. 'job:' .. id .. ':events' end
local function idem_key(k) return PREFIX .. 'idem:' .. k end
local function worker_key(id) return PREFIX .. 'worker:' .. id end
local function worker_jobs_key(id) return PREFIX .. 'worker:' .. id .. ':jobs' end

local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- event appends one transition to the job's history list.
local function event(id, at, kind, worker, token, detail)
  redis.call('RPUSH', events_key(id), cjson.encode({
    at = at, kind = kind, worker = worker or '', token = tonumber(token) or 0, detail = detail or '',
  }))
end

-- revoke takes a lease away from its holder (worker death or lease expiry)
-- and either requeues the job immediately or, if it has used every attempt,
-- moves it to the dead-letter set. lease_token is left alone: the next
-- acquire increments it, which is what fences out the old holder.
-- Returns 'ready' or 'dead'.
local function revoke(id, now, reason)
  local jk = job_key(id)
  local f = redis.call('HMGET', jk, 'lease_owner', 'lease_token', 'attempts', 'max_attempts')
  local owner, token = f[1], f[2]
  redis.call('ZREM', K_LEASES, id)
  if owner and owner ~= '' then
    redis.call('SREM', worker_jobs_key(owner), id)
  end
  if tonumber(f[3]) >= tonumber(f[4]) then
    redis.call('HSET', jk, 'status', 'DEAD', 'lease_expires_at', 0, 'finished_at', now,
      'last_error', reason .. '; attempts exhausted')
    redis.call('ZADD', K_DEAD, now, id)
    event(id, now, 'DEAD', owner, token, reason)
    return 'dead'
  end
  redis.call('HSET', jk, 'status', 'PENDING', 'lease_expires_at', 0, 'enqueued_at', now,
    'last_error', reason)
  redis.call('ZADD', K_READY, now, id)
  event(id, now, 'REQUEUED', owner, token, reason)
  return 'ready'
end
