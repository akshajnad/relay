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
