# Relay

Relay is a distributed task engine in Go. Clients submit jobs over gRPC, a
pool of workers long-polls for them, and the coordinators keep every job's
state transitions **exactly-once**, through worker crashes, stalls,
network partitions, and coordinator failover. State lives either in Redis
(one atomic Lua script per transition) or in a **Raft-replicated state
machine** spread across 3 or 5 coordinators, with no external dependency.

The claims are checked, not just asserted. A seeded chaos harness kills,
pauses and partitions real containers while recording Jepsen-style
histories. A checker then verifies every invariant plus per-job
linearizability (porcupine), and it is shown to catch a deliberately
injected fencing bug. The benchmarks compare Relay against
[asynq](https://github.com/hibiken/asynq) on the same workloads and
report the results as measured, including where Relay loses.

**[DESIGN.md](DESIGN.md)** is the full RFC: architecture, data model,
lease and fencing design, delivery semantics, the soft-state lease
argument for Raft, a failure-mode table, alternatives, limitations, and a
decision log.

- [30-second tour](#30-second-tour)
- [Architecture](#architecture)
- [Quickstart](#quickstart)
- [Correctness](#correctness)
- [Benchmarks](#benchmarks)
- [Observability](#observability)
- [Repository map](#repository-map)

## 30-second tour

- **Leases with fencing tokens.** A worker reports a result with
  `(worker_id, lease_token)`. The store applies it only if that token is
  still current, so a zombie worker's late result is rejected
  (`ABORTED`), never written. Tokens are a per-job counter on Redis and
  the Raft log index of the granting entry on Raft.
- **At-least-once execution, exactly-once transitions, idempotent
  submission.** These are stated precisely in
  [DESIGN §5](DESIGN.md#5-delivery-semantics).
- **Raft without heartbeat writes.** Lease deadlines and worker liveness
  are soft state on the leader. Only `ExpireLease(job, token)` is
  replicated, and a new leader grants everyone a full grace period. The
  argument for why that is safe is in
  [DESIGN §7](DESIGN.md#7-raft-integration-and-the-soft-state-lease-argument).
- **Leader-following clients.** Followers answer `NOT_LEADER` with the
  leader's address. Workers, `relayctl` and the benchmarks follow it, and
  they never blindly replay a call whose outcome is unknown.
- **Pluggable store.** One `Store` interface, two backends, and one
  conformance suite that both pass.

## Architecture

```mermaid
flowchart LR
    subgraph clients
        CLI[relayctl]
        BENCH[relay-bench]
    end
    subgraph workers
        W1[worker] & W2[worker] & W3[worker]
    end
    subgraph raft["coordinators (Raft)"]
        L[leader<br/>JobService · WorkerService<br/>FSM + soft leases]
        F1[follower<br/>FSM replica]
        F2[follower<br/>FSM replica]
        L -- replicate log --> F1 & F2
    end
    subgraph obs[observability]
        J[Jaeger]
        P[Prometheus] --> G[Grafana]
    end
    CLI & BENCH -- SubmitJob / GetJob --> L
    W1 & W2 & W3 -- Register · Heartbeat · AcquireJob long-poll · Complete/Fail --> L
    F1 -. NOT_LEADER + leader address .-> W1
    L -. OTLP traces .-> J
    P -. scrape /metrics .-> L & F1 & F2
```

| Binary | Role |
|---|---|
| `coordinator` | gRPC server (`JobService`, `WorkerService`, `ClusterService`), long-poll dispatch, failure detector, lease reaper. `--store redis` or `--store raft` |
| `worker` | registers, heartbeats, runs `--concurrency` acquire→run→report loops (`noop`, `sleep`, `fail-n-times`), drains on SIGTERM |
| `relayctl` | `submit [--wait]`, `get`, `workers`, `health`, `cluster status\|add\|remove` |
| `relay-chaos` | seeded chaos runs plus the history checker |
| `relay-bench` | comparative benchmarks (`compare`) and charts (`plot`) |
| `asynq-worker` | the asynq baseline, with the same handlers and the same timing hooks |

## Quickstart

You need Go 1.26+ and a Docker-compatible runtime (Docker Desktop,
OrbStack, colima).

```sh
make up                     # 3 Raft coordinators + 3 workers + Jaeger, Prometheus, Grafana
go run ./cmd/relayctl --coordinator n1=localhost:50051,n2=localhost:50052,n3=localhost:50053 cluster status
go run ./cmd/relayctl --coordinator localhost:50051 submit --type fail-n-times --payload '{"n":1}' --max-attempts 3 --wait
open http://localhost:3000  # Grafana "Relay" dashboard; traces at http://localhost:16686
make down

make test                   # unit tests, Store conformance on both backends (-race)
make integration            # container failure tests, Raft failover, redirect, minority leader, membership, chaos self-tests
make chaos SEED=1           # 5-minute seeded chaos run on Raft (BACKEND=redis for Redis)
make chaos-sweep            # 20 seeds x 5 min on a pinned image, summarized into bench/results/chaos-sweep-$BACKEND.json (~2h)
make bench                  # Relay-Redis vs Relay-Raft vs asynq (~2.5h; see BENCH_ARGS in the Makefile)
```

`make up BACKEND=redis` runs the Redis backend instead. Every flag can
also be set through an environment variable: `RELAY_` plus the flag name
in upper case (`--lease-duration` → `RELAY_LEASE_DURATION`).

## Correctness

`relay-chaos run --seed N --duration 5m --faults all` brings up a cluster,
runs concurrent submitters with about 10% duplicate idempotency keys
against `sleep` and `fail-n-times` workers, and injects faults on a
schedule derived only from the seed:
- worker kill, pause and partition
- coordinator kill, pause, restart, client isolation and backend partition
- on Raft: leader kill, leader pause, leader partition (the old leader
  stays reachable by clients but is cut off from its peers), and
  full-cluster restart
- Toxiproxy latency and jitter between workers and coordinators

It heals everything, waits for quiescence, freezes the workers, and checks:

| Invariant | Checked against |
|---|---|
| no acknowledged submission lost | client acks vs final reads |
| every job terminal after quiescing | final state |
| at most one terminal transition; result written once | server event log, and accepted reports per lease token |
| every accepted report carried the current token | server log (latest LEASED) and real-time client order |
| duplicate keys map to one job | client acks and the stored key |
| attempts ≤ max; DEAD only at max | final state and event log |
| per-job linearizability | porcupine, with a nondeterministic model of transitions clients never see |

A failing run saves the history and a Porcupine visualization, and the
same seed reproduces the same fault schedule.

**Results.** Every fault kind, 20 seeds × 5 minutes per backend, on one
pinned image at commit `c302e0f`: **40 of 40 seed runs pass, with 0
violations.** On Raft that is 158,495 jobs and 1,155,170 recorded
operations under 1,100 injected faults, including 59 leader kills, 66
minority-side leader partitions and 52 full-cluster restarts. There were
396 leader elections, and 2,395 zombie reports were fenced out. On Redis
(the same harness without the leader faults, since Redis itself is never faulted,
[DESIGN §8](DESIGN.md#8-failure-modes)) it is 103,758 jobs and 979,836
operations under 1,181 faults, with 4,347 zombie reports fenced out.
Per-seed numbers are in [DESIGN §9](DESIGN.md#9-verification),
[`bench/results/chaos-sweep-raft.json`](bench/results/chaos-sweep-raft.json)
and [`bench/results/chaos-sweep-redis.json`](bench/results/chaos-sweep-redis.json).

**The checker catches real bugs.** Building with `-tags relay_nofence`
removes the token check from both backends. `make integration` runs that
build under worker pauses and partitions, and asserts the checker reports
fencing violations and non-linearizable jobs, with visualizations, while
every other invariant stays clean. The recorded run
([`bench/results/verification-c302e0f.txt`](bench/results/verification-c302e0f.txt))
flagged 10 fencing violations and 5 non-linearizable jobs on Redis, and
10 and 7 on Raft; the exact counts vary with timing from run to run.
Building the harness found
three real problems, all in the harness itself, each fixed at the root
and recorded in [DESIGN §12](DESIGN.md#12-decision-log):
- a final-snapshot race (fixed with a freeze hand-off)
- a backwards step of the VM's wall clock (histories now use
  `CLOCK_MONOTONIC`)
- a scheduler bug in overlapping fault windows

## Benchmarks

All numbers are from one run of `make bench`
([`bench/results/compare-20261027T033746Z.json`](bench/results/compare-20261027T033746Z.json),
charts alongside), on image `relay:dev` built at commit `547dd3a`. The
results file records the image's revision. The setup:
- **Hardware:** Apple M4 Pro (14 cores, 24 GB), macOS 26.6.2, go1.26.1.
- **Container runtime:** Docker Desktop 29.9.2, with
  14 CPUs and 7.7 GB for its VM.
- **Shared host:** every container (store, coordinators, workers, load
  driver) runs on that one machine, so there are no real network hops.
- **Configuration:** each backend's failure-detection settings are
  recorded in the results file.
- **One disturbance:** the host slept for about a minute during
  asynq-tuned's recovery phase (`pmset` log, 05:48–05:49 UTC). That hit
  trial 21, which reads 150.43s, the phase's max; its other 29 trials
  ran 87–127s. Every other phase ran with the host awake.

| backend | throughput at saturation | dispatch latency p50 / p95 / p99 (ms) | end-to-end queue wait p50 / p95 / p99 (ms) | submit RTT p50 / p95 / p99 (ms) | worker-failure recovery p50 / p90 / max | leader failover p50 / p90 / max |
|---|---|---|---|---|---|---|
| **relay-redis** | 16,388 | 0.34 / 0.48 / 0.66 | 0.34 / 0.48 / 0.66 | 0.22 / 0.34 / 0.48 | 2.83s / 3.75s / 3.81s (n=30) | — |
| **relay-raft** | 4,511 | 3.12 / 7.43 / 13.76 | 3.12 / 7.43 / 13.76 | 1.78 / 4.84 / 9.28 | 2.85s / 3.79s / 3.82s (n=30) | 2.29s / 2.69s / 3.21s (n=30) |
| **asynq** | 16,781 | 0.90 / 156.91 / 294.75 | 77.52 / 279.71 / 369.34 | 0.19 / 0.34 / 0.94 | 111.02s / 126.26s / 154.47s (n=30) | — |
| **asynq-tuned** | 16,793 | 0.95 / 2.97 / 4.24 | 0.95 / 2.97 / 4.24 | 0.10 / 0.20 / 0.30 | 111.80s / 126.77s / 150.43s (n=30) | — |

![throughput](bench/results/compare-20261027T033746Z/throughput.png)
![dispatch latency](bench/results/compare-20261027T033746Z/dispatch_latency.png)
![queue wait](bench/results/compare-20261027T033746Z/queue_wait.png)
![worker recovery](bench/results/compare-20261027T033746Z/worker_recovery.png)
![leader failover](bench/results/compare-20261027T033746Z/leader_failover.png)

**Correctness under faults.** Each backend ran the same 2-minute
workload: 20 sleep jobs/s on 4 workers × 4 slots, while seeded faults
(seed 1) killed, paused or partitioned workers for 2–10s at a time. The
test then waited for every acknowledged job to finish.

| backend | faults | acked jobs | lost | unfinished | duplicate executions | duplicate terminal transitions |
|---|---|---|---|---|---|---|
| relay-redis | 18 | 2,456 | 0 | 0 | 47 | 0 |
| relay-raft | 18 | 2,456 | 0 | 0 | 48 | 0 |
| asynq | 18 | 2,456 | 0 | 0 | 8 | not observable |
| asynq-tuned | 18 | 2,456 | 0 | 0 | 11 | not observable |

### Workloads and definitions

- **Workers.** Every backend gets the same worker containers (10 × 10
  slots), the same handlers (`noop` and `sleep`) and the same Redis
  configuration (AOF, `everysec`). asynq runs with its default
  configuration (**asynq**) and with `TaskCheckInterval=10ms`
  (**asynq-tuned**), so its default polling interval is not the only
  thing on show. asynq is driven through its own client, server, mux,
  `TaskID` and `MaxRetry` APIs.
- **Throughput at saturation.** 20,000 `noop` jobs are preloaded with no
  workers running, then all 10 workers start. Throughput is jobs ÷ (last
  handler end − first handler start). This measures scheduling cost per
  job, because there is no work in the jobs.
- **Dispatch latency, one definition for every backend.** This is the
  project's existing definition, "assignment time minus max(job eligible
  time, worker request time)", observed from outside so asynq can be
  measured too:
  - *assignment* is the handler start;
  - *eligible* is when the driver invoked the submit;
  - *worker request* is when a slot on that worker became free, simulated
    FIFO from that worker's handler start/end events.

  Every timestamp comes from `CLOCK_MONOTONIC` inside the one Docker VM.
  The load is 30,000 `sleep(20ms)` jobs at a steady 500/s, about 10% of
  capacity.
- **End-to-end queue wait.** Handler start minus submit invoked.
- **Worker-failure recovery.** Trials use 4 workers × 2 slots, 4 jobs of
  8s each, so there is spare capacity. In each trial a worker holding at
  least one job is killed with `docker kill`. Recovery runs from the
  container's death time (Docker's own clock) to the last of its jobs
  starting on another worker (the worker's wall clock, the same VM).
  There are 30 trials per backend.
- **Leader failover** (Raft only). Under a steady trickle of 50 jobs/s,
  the leader is killed with `docker kill`. Failover runs to the first
  `AcquireJob` that was *sent after the kill* and returned a job, which
  only a new leader can grant. The node is restarted and the cluster
  rejoins between trials. There are 30 trials.

### What the numbers say, including where Relay loses

- **Throughput: asynq wins, narrowly against Relay-Redis and clearly
  against Relay-Raft.** asynq dequeues straight from Redis inside its
  worker process. Relay adds a gRPC hop through the coordinator per
  acquire and per report, and still lands within 3%
  (16,388 vs 16,781 jobs/s; asynq-tuned 16,793).
  **Relay-Raft is about 3.6× slower**
  (4,511 jobs/s). Every
  acquire and every completion is a Raft log entry, replicated to a
  majority and fsynced by bbolt before it is applied. That is the price
  of having no single point of failure, and it is the main
  [known limitation](DESIGN.md#11-known-limitations) of the replicated
  design.
- **Dispatch latency: Relay-Redis is fastest; asynq's default is
  dominated by its poll interval.** With an empty queue, an asynq server
  sleeps for `TaskCheckInterval` (1s by default) between dequeue
  attempts, so jobs pile up during a sleep and then drain in a burst.
  Jobs that waited through a sleep, while slots sat idle, show up in the
  p95/p99 (157 / 295 ms). Jobs drained in the burst waited only for
  slots, which the definition excludes, which is why the p50 is 1 ms. The
  end-to-end queue wait (p50 78 ms) shows the full cost. With a 10 ms
  interval, asynq-tuned is close to Relay-Redis: 0.95 vs 0.34 ms p50,
  and 4.2 vs 0.66 ms p99. Relay's long-poll plus FIFO notifier hands a
  job to an already-waiting worker without polling. Relay-Raft's ~3 ms
  p50 is the log commit on submit and on acquire (see submit RTT: ~2 ms).
- **Recovery from a dead worker: Relay is about 40× faster.** Relay
  declares a worker dead after 3 missed 1s heartbeats and requeues its
  jobs in the same atomic step. That takes 2.8–3.8s; the two modes come
  from the detector's 1s tick. asynq waits for its fixed 30s lease to
  expire, then *another* 30s (its clock-skew allowance), then for its
  recoverer, which runs every 60s. The reclaimed task then waits a
  sidekiq-style retry delay of 15–44s. That gives the measured
  87–154s. `TaskCheckInterval` does not touch that path, so asynq-tuned
  recovers no faster than asynq's defaults (p50 111.8s vs 111.0s).
- **Leader failover (Relay-Raft): 2.3s median, 3.2s worst.** That is
  Raft's 1s heartbeat timeout plus a randomized election, the new
  leader's `Barrier`, and clients following `NOT_LEADER` to it. Raft's
  timeouts can be lowered at the cost of spurious elections on a loaded
  host.
- **Faults: no backend lost a job, and Relay records zero duplicate
  terminal transitions.**
  - Relay re-ran more jobs (47–48 vs 8–11). Detecting failure in 3s means a
    worker paused or partitioned for 2–10s gets its jobs reassigned while
    it is still running them. That trade, faster recovery for more
    duplicate executions, is inherent to at-least-once execution, and
    fencing is what keeps it from becoming a duplicate *completion*.
  - asynq waits out short pauses because of its 30s lease. For the
    duplicates it does have, it records no transition history, so
    whether a zombie's completion was also accepted cannot be checked.
    Its worker checks its lease against its *own* clock before writing,
    and Redis checks no token
    ([DESIGN §10](DESIGN.md#10-alternatives-considered)).

**A note on Relay's internal metric.** The coordinator's own dispatch
latency, the project's original metric
([details](#relays-coordinator-measured-latency)), reports Relay-Redis at
p50 0.09 / p95 1.09 / p99 1.13 ms.
That is lower at the median than the external number, because it starts
when the job is eligible inside Redis rather than at submit, but *higher*
at p95. Its `redis_now − eligible_at` term uses Redis `TIME` truncated to
whole milliseconds, so a 0.1 ms wait can count as 1 ms. The internal
queue wait shows the quantization directly: p50 0.00 ms,
p95 1.00 ms. The definition is unchanged; this is a
resolution limit, documented in
[DESIGN §11](DESIGN.md#11-known-limitations). On Raft the internal metric
is p50 2.75 / p99 12.41 ms.

### Relay's coordinator-measured latency

`make bench-relay BENCH_ARGS="--modes burst,steady,preload"` runs the
original single-backend benchmark against Relay-Redis, with latency
measured by the coordinator. Without `BENCH_ARGS` it runs only burst and
steady. Its last result is
[`bench/results/20261027T063007Z.json`](bench/results/20261027T063007Z.json),
image revision `32aa118`. p95 scheduling latency was 1.07 ms in burst
mode, 0.39 ms in steady mode and 0.55 ms in preload mode, against a
target of 25 ms. Sub-millisecond p95s here move with the 1 ms resolution
noted above. A two-mode run a minute earlier measured steady p95 at
1.16 ms, and the previous result
([`20261026T053700Z.json`](bench/results/20261026T053700Z.json)) had
1.69 / 0.18 / 1.27 ms.

## Observability

- **Tracing.** Each job is one trace:
  `SubmitJob → relay.enqueue → relay.dispatch → relay.execute (worker) → CompleteJob`.
  Each retry shows up as its own dispatch/execute branch. The submit's W3C
  traceparent is stored with the job to bridge the queue, and otelgrpc
  carries context across the RPCs.
- **Prometheus at `/metrics`.** It exposes dispatch latency, queue wait,
  queue depth, active leases, lease expirations, reassignments,
  stale-completion rejections, Raft commit latency, leader changes and
  leadership.
- **Grafana.** The "Relay" dashboard in `deploy/grafana` is provisioned
  automatically by `make up`.

## Repository map

| Path | |
|---|---|
| `internal/store` | `Store` interface; `redis/` (Lua), `raft/` (FSM, soft leases, membership), `storetest/` (conformance) |
| `internal/coordinator` | gRPC services, long-poll notifier, liveness loops, leadership handling |
| `internal/rpc` | `DialCluster`: NOT_LEADER-following client |
| `internal/history`, `internal/check` | Jepsen-style history recording and the checker |
| `cmd/relay-chaos` | chaos orchestrator, workload, and fault schedule |
| `cmd/relay-bench` | benchmarks (`compare`, `drive`, `plot`) |
| `deploy/` | compose files (Redis, Raft, chaos, bench, observability), Grafana and Prometheus config |
| `bench/results/` | committed results JSON and charts |
