# Relay design

Status: living document. The RFC sections are completed in milestone 6. The
decision log below records each significant decision as it was made, with
the alternatives that were rejected.

## Decision log

### D1. One `Store` interface covering every state transition (M1)

**Decision.** `internal/store` defines `Store`, and the coordinator depends
only on it. Its methods are Submit, RegisterWorker, Acquire, Renew,
Complete, Fail, Release, ReapExpired, ExpireWorkers, Get, ListWorkers and
QueueDepth. Backends live in `internal/store/redis` (package `redisstore`)
and `internal/store/raft` (package `raftstore`).

**Liveness is inside the interface.** Renew, ReapExpired and ExpireWorkers
are store methods, not coordinator logic. Redis keeps heartbeats and lease
deadlines in Redis. Raft keeps them as soft state on the leader (D-raft
below). The coordinator's liveness loop calls the same two methods either
way, so lease bookkeeping can live where each backend needs it.

**Alternatives.**
- *A narrower key-value interface (CAS on job records) with transition logic
  in the coordinator.* Rejected. On Redis, each transition would become a
  read-modify-write over several round trips. The atomicity that one Lua
  script gives today would have to be rebuilt with WATCH/MULTI.
- *Separate `LeaseManager` and `JobStore` interfaces.* Rejected. A worker's
  death and the requeue of its leases must be atomic: in Redis one script
  does both, and in Raft one FSM command does both. Splitting the
  interfaces would separate those two writes.

**NOT_LEADER.** A replicated store returns `*store.NotLeaderError`. The
coordinator maps it to gRPC `UNAVAILABLE` with an `ErrorInfo{reason:
NOT_LEADER, metadata: leader_id, leader_addr}` detail. It is not
`FAILED_PRECONDITION`, because workers already treat that code as "you
were declared dead, re-register". The detail means *the call did not
execute*, so the client library may redirect it blindly. A plain
`UNAVAILABLE` means the outcome is unknown.

**Conformance.** `internal/store/storetest` uses only the interface. It
checks that fencing tokens increase and never checks their exact values,
because Redis tokens count per job and Raft tokens are log indices. It
checks structure through `QueueDepth` and `ListWorkers` rather than
backend keys. Redis-only layout tests stay in `store/redis`.

### D2. Chaos histories: one clock, unbuffered writes, three outcome types (M2)

**Decision.** Every client-visible RPC is recorded by a gRPC client
interceptor (`internal/history`) as an `invoke` record and then an `ok`,
`fail` or `info` record, Jepsen style. Records are JSONL, written with
one `write(2)` each to an unbuffered file. A `docker kill` therefore loses
nothing already recorded, and an invoke with no completion is read back as
indeterminate.

- `ok` covers success and *definite* answers about job state (ABORTED for
  a stale lease, NOT_FOUND, FAILED_PRECONDITION for a dead worker). The op
  ran, and its code is an observation that the model has to explain.
- `fail` means the op certainly did not execute: NOT_LEADER, or an invalid
  argument.
- `info` covers everything else (UNAVAILABLE, deadline, cancel). The op
  may or may not have applied.

**One clock.** Porcupine orders ops by real time, so every recorded
process must read the same clock. Workers and the load generator all run
as containers in the same Docker VM and share its `CLOCK_REALTIME`. The
host only injects faults, and its fault timeline is never used in checks.
*Rejected:* running the load generator on the host. macOS and the Linux
VM can drift by milliseconds, which is enough to report a false
linearizability violation.

### D3. The checker: invariants plus a nondeterministic per-job model (M2)

The checker runs after quiescing. It combines the history with every
job's final state and its server-side event log, read through the normal
recorded client. Every invariant is checked independently from both the
client's and the server's point of view, where both views exist:

| Invariant | Client view | Server view |
|---|---|---|
| no lost acks | every acked id is readable | — |
| all jobs terminal | — | status is terminal after quiescing |
| terminal at most once | ≤1 lease had its terminal report accepted | ≤1 terminal event, and it is the last event |
| fencing | no accepted report was *sent* after a newer token had been handed out | every holder transition carries the token and worker of the latest LEASED event, with no REQUEUED/RELEASED in between |
| idempotency | all acks of a key name one job; ≤1 reported "created" | the job stores that key; no other job carries it |
| attempts | — | attempts ≤ max; DEAD ⇒ attempts = max; LEASED − RELEASED = attempts |

**Linearizability.** Porcupine checks each job's history, partitioned by
job id, against a `NondeterministicModel`. Clients never observe some
transitions: lease expiry, worker death, a lease whose acquire reply was
lost, and the coordinator releasing such a lease. Before each observed op
the model takes the closure over those *internal* transitions. A lease
nobody observed is tracked as `Hidden`: its token is known only to be
greater than the last observed one. The model therefore needs tokens to
increase, never to increase by exactly one, which is what lets the same
checker accept Raft log-index tokens. Heartbeat responses are *not* in
the model. They are advisory, because the authority on a lease is the
fencing token and not the renewal reply.

**Freeze before the final read.** The first deliberate-bug runs
overloaded the cluster, so quiescing timed out with jobs still running,
and a worker completed a job *after* the load generator's "final" read.
The checker correctly reported a contradiction, but the bug was in the
harness. The fix is a hand-off: once the load generator is done
quiescing it writes `quiesced`, the orchestrator SIGKILLs every worker
and writes `frozen`, and only then does the load take the final
snapshot. After that no recorded op can follow the snapshot. The
alternative, comparing the final state only against ops that finished
before each job's final read, was rejected because it silently shrinks
what gets checked.

### D4. Proving the checker catches bugs (M2)

**Decision.** The `relay_nofence` build tag sets `store.FencingEnabled =
false`. In Redis, `complete.lua` and `fail.lua` then accept any report on
a LEASED job, whoever holds the lease. The tag is only ever compiled into
the separate `relay:nofence` image. A plain runtime flag was rejected,
because a production binary should not be able to switch fencing off.

**Workload matters.** In the first nofence run, a heavy load (40
submits/s of 2–6s jobs on 20 slots) produced 19 zombie reports and
*none* were accepted, even with fencing disabled. Requeued jobs waited
minutes in the backlog, so each zombie report arrived while its job was
PENDING, and Lua rejects those even without the token check. The test
therefore uses 3 submits/s of 3–6s jobs, so a requeued job is re-leased
at once and the zombie reports while the new lease is live. With that
workload the checker flags 14 fencing violations and 9 non-linearizable
jobs, and every other invariant stays clean. That makes it a precise
detector, not a noisy one. `test/chaos/nofence_test.go` runs this
automatically.

### D5. History timestamps use CLOCK_MONOTONIC (M2, found in M3)

**Finding.** A 5-minute Redis chaos run (seed 2) reported one job as
non-linearizable. Its history was trivially linearizable (submit →
acquire → complete → reads that all said SUCCEEDED), but one read
*returned 3.4ms before it was invoked*. The Docker VM's wall clock had
stepped backwards, most likely a time-sync correction against the host.
Porcupine cannot place an op whose interval is inverted, so it rejected
a correct history.

**Fix.** Records are ordered by `CLOCK_MONOTONIC`. On Linux it is one
system-wide clock, so it is comparable across every container on the VM
kernel, and it never steps backwards. Records keep the wall clock in a
separate field, for correlating with Docker events in M5. The reader now
*rejects* any op whose completion precedes its invocation, so a clock
problem fails loudly as a harness error and never looks like a Relay
bug. Re-checking the saved run produces exactly that error, naming the
single inverted op.

### D6. Raft integration (M3)

**Library.** `hashicorp/raft` with `raft-boltdb/v2` (bbolt) for the log
and stable store, and file snapshots. Writing Raft from scratch was
rejected: the risk sits in exactly the corner cases (log matching after
leader changes, joint configuration changes, snapshot install) that a
battle-tested library already handles. Relay's value is in what it
builds on top.

**Replicated FSM.** Every mutation of job or worker state is one
command: submit, register, acquire, complete, fail, release,
expire_leases, expire_workers, set_node_addr. The FSM ports the Lua
scripts transition for transition, including the exact backoff
arithmetic, and the same conformance suite checks both backends.
Determinism rules:
- `Apply` never reads the clock or a random source. The leader stamps
  `now`, ids, and the backoff jitter into the command when it proposes.
- `Apply` iterates maps only in sorted order. Worker death revokes leases
  in job-id order, so event logs are identical on every replica.
- Backoff base and max travel in each command, so nodes with different
  flags cannot diverge.
- `TestReplicasAreIdentical` checks the result: all replicas, and a fresh
  FSM replaying the leader's log, serialize to the same bytes.

Commands are JSON, not protobuf as first planned. Go's `encoding/json`
output for structs and maps is deterministic, which is all the FSM
needs, and JSON avoids a second codegen path. Its cost (base64 payloads,
roughly 2× larger entries) shows up in the M5 numbers.

**Fencing token = log index of the granting entry.** Raft log indices
are unique and strictly increasing across the whole log, including
across terms and leader changes. A new leader's entries always come
after every committed entry of earlier terms (Leader Completeness), and
an uncommitted entry that gets overwritten never reached `Apply`, so its
index was never handed out as a token. A lease granted later therefore
always has a larger token, without a per-job counter. Two leases can
never share a token, because each acquire is its own entry.

**Soft-state leases.** The FSM stores a lease's owner and token, never
its deadline. Deadlines (`leases`) and worker heartbeats (`beats`) live
only in the leader's memory, on its monotonic clock:
- A heartbeat (`Renew`) checks ownership against the FSM and moves the
  deadline. It costs no log write. With N workers heartbeating every
  500ms, this keeps 2N entries per second out of the log.
- The reaper proposes `expire_leases(job, token)`. The FSM revokes the
  lease only if `token` is still current, so a proposal that raced a
  completion or a newer lease does nothing.
- On election the new leader calls `Barrier()`, so its FSM holds every
  committed entry. It then sets every lease deadline to now + lease and
  every live worker's heartbeat to now, and only then serves.

*Why this is safe.* Losing or resetting soft state can only make a lease
last *longer* than it would have. What stops a double completion is
fencing, not the deadline. A worker whose lease expired late still
cannot complete once the job has been re-leased, because its token is no
longer current. A worker that resumes after a failover keeps its lease
and can complete, which is correct, since nobody else holds it. The
worst case is delayed recovery: up to one extra lease period plus one
election timeout before a dead worker's jobs are requeued.

*Worker death is replicated, heartbeats are not.* `expire_workers` is a
command, so "a dead worker must re-register" holds on every leader.
Re-registration is rare and worth a log entry. Heartbeat timestamps are
the high-rate part and stay soft.

*A lease can be revoked slightly early.* A heartbeat can land after the
reaper has decided to expire that lease but before the command commits.
Redis avoids this because its heartbeat and reap scripts are atomic with
each other. Here the cost is one unneeded reassignment, never a
correctness problem.

**Leader-only serving.**
- Writes and `AcquireJob` go to the leader. A follower returns
  `NotLeaderError` with the leader's id and its gRPC address, which is
  replicated in the FSM by `set_node_addr`.
- `Get` and `ListWorkers` call `VerifyLeader()` first. A deposed leader
  that has not noticed yet cannot serve a stale read.
  `TestMinorityLeaderStepsDown` checks this: an isolated old leader
  refuses both reads and writes.
- When leadership is lost, the coordinator wakes every pending long-poll.
  Each one retries the store, gets NOT_LEADER, and returns it.
  `TestLeadershipLossFailsPendingLongPolls` checks this happens within 1s
  for five blocked pollers.
- An empty `AcquireJob` costs no log entry. The leader checks its own FSM
  first, which holds every acknowledged submit and requeue.
- A duplicate idempotency key is answered from the FSM. A committed entry
  is never undone, so even a deposed leader answers that correctly.

**Proposal outcomes.** Apply returns `raft.ErrNotLeader` before anything
is appended, so it maps to NOT_LEADER, which is safe to redirect.
`ErrLeadershipLost` and timeouts *after* proposing have unknown outcomes.
They map to plain UNAVAILABLE, the client library does not replay them,
and the caller's own retry logic runs instead (idempotency keys for
submits, ALREADY for reports).

**Client library.** `rpc.DialCluster` keeps one connection per endpoint
and routes each call to the believed leader:
- NOT_LEADER → follow the hint (by node id, so it works through
  Toxiproxy or published host ports), otherwise try the next node; retry
  with jittered backoff until the call's deadline.
- Any other UNAVAILABLE or deadline → return the error unretried, but
  send the next call to a different node.
- `WaitForReady(false)` → a dead node fails fast instead of absorbing a
  30s long-poll deadline.

Workers, relayctl, relay-bench and relay-chaos all use it.

**Membership.** Every node can bootstrap from an identical peer list
(`--bootstrap --peers`); hashicorp/raft treats that as idempotent, and a
node with existing state skips it. Nodes are added and removed through
`ClusterService.AddVoter/RemoveServer` (`relayctl cluster add|remove`).
A new node catches up by snapshot install when the log has been
compacted past it (`TestAddAndRemoveVoter`).

**Snapshots.** `Snapshot()` serializes the whole FSM to JSON on the FSM
goroutine, which holds up Apply for O(state). It is simple and correct.
Its cost grows because terminal jobs are never deleted (see limitations).
`Restore` rebuilds the derived indexes (ready queue, leased and held
sets) from the jobs. `TestSnapshotRestoreAndCatchUp` compacts the log
past a stopped follower and checks that it installs the snapshot and
converges. `TestFullClusterRestart` restarts all three nodes from
snapshot plus log.

### D7. Observability (M4)

**One trace per job across an asynchronous queue.** otelgrpc propagates
trace context in gRPC metadata, which covers every RPC hop. The gap from
"enqueued" to "picked up by a worker" is not an RPC, so the submit's W3C
`traceparent` is stored with the job: a `trace_parent` hash field in
Redis, a command field in Raft. When the job is assigned, the coordinator
creates a `relay.dispatch` span *in the job's trace*. The span is backdated
to the acquire call, carries the measured dispatch latency and queue wait
as attributes, and links to the worker's AcquireJob RPC span (a different
trace). The dispatch span's traceparent goes back in the AcquireJob
response, so the worker's `relay.execute` span is its child, and the
Complete/Fail RPC is a child of the execution. A retried job therefore
shows each attempt as its own dispatch → execute → report branch under one
enqueue. `TestJobTraceSpansSubmitToComplete` asserts the whole parent
chain in-process. *Rejected:* one trace per AcquireJob. That is easier,
but it loses the ability to see a job's whole life, including retries
and reassignments, in one place.

**Prometheus.** `/metrics` serves the Prometheus text format, and the
HdrHistogram JSON snapshot that relay-bench reads moved to
`/metrics.json`. The definitions are unchanged. Counters read the
existing atomics at scrape time (`CounterFunc`) instead of being counted
twice. The latency histograms feed Prometheus and HdrHistogram from the
same `Record` call. Prometheus buckets are coarse, so the benchmark
numbers keep coming from HdrHistogram. `relay_queue_depth` and
`relay_leases_active` are gauges read from the store at scrape time. Every
Raft node reports its own replica's depth, so the dashboard shows the
max.

**Startup bug found by running it.** With tracing enabled, the
coordinator exited at startup with "conflicting Schema URL". The SDK's
default resource and the pinned semconv version disagreed. Unit tests
never set an endpoint, so they missed it. The fix is a schemaless
service-name resource, and `TestSetupWithEndpoint` now exercises the real
exporter path.
