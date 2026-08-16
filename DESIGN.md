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
