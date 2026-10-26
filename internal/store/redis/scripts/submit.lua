-- submit: create a job and make it eligible now, unless the idempotency key
-- already maps to a job, in which case return that job's id untouched.
-- ARGV: id, type, payload, idempotency_key, max_attempts
-- Returns {job_id, duplicate(0|1)}
local id, typ, payload, idem, max_attempts = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local now = now_ms()

if idem ~= '' then
  if not redis.call('SET', idem_key(idem), id, 'NX') then
    return {redis.call('GET', idem_key(idem)), 1}
  end
end

redis.call('HSET', job_key(id),
  'id', id, 'type', typ, 'payload', payload, 'idempotency_key', idem,
  'status', 'PENDING', 'attempts', 0, 'max_attempts', max_attempts,
  'lease_owner', '', 'lease_expires_at', 0, 'lease_token', 0, 'reported_token', 0,
  'created_at', now, 'enqueued_at', now, 'leased_at', 0, 'finished_at', 0,
  'last_error', '', 'result', '')
redis.call('ZADD', K_READY, now, id)
event(id, now, 'SUBMITTED')
return {id, 0}
