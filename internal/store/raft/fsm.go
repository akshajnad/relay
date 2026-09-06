package raftstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/akshajnad/relay/internal/store"
	"github.com/google/btree"
	"github.com/hashicorp/raft"
)

// Command ops. Every mutation of job or worker state is one of these,
// proposed by the leader and applied by every replica.
const (
	opSubmit       = "submit"
	opRegister     = "register"
	opAcquire      = "acquire"
	opComplete     = "complete"
	opFail         = "fail"
	opRelease      = "release"
	opExpireLeases = "expire_leases"
	opExpireWorker = "expire_workers"
	opSetNodeAddr  = "set_node_addr"
)

// command is one log entry. The leader stamps NowMs, ids and jitter when it
// proposes, so Apply never reads the clock or a random source and every
// replica computes the same state.
type command struct {
	Op    string `json:"op"`
	NowMs int64  `json:"now"`

	// submit
	ID          string `json:"id,omitempty"`
	Type        string `json:"type,omitempty"`
	Payload     []byte `json:"payload,omitempty"`
	Key         string `json:"key,omitempty"`
	MaxAttempts int    `json:"max,omitempty"`
	TraceParent string `json:"tp,omitempty"`

	// register
	Worker      string `json:"worker,omitempty"`
	Hostname    string `json:"host,omitempty"`
	Concurrency int    `json:"conc,omitempty"`

	// complete, fail, release
	Job       string  `json:"job,omitempty"`
	Token     int64   `json:"token,omitempty"`
	Result    []byte  `json:"result,omitempty"`
	Err       string  `json:"err,omitempty"`
	Retryable bool    `json:"retryable,omitempty"`
	Jitter    float64 `json:"jitter,omitempty"`
	BaseMs    int64   `json:"base,omitempty"`
	MaxMs     int64   `json:"maxd,omitempty"`

	// expire_leases: (job, token) pairs; expire_workers: worker ids
	Leases  []store.Lease `json:"leases,omitempty"`
	Workers []string      `json:"workers,omitempty"`

	// set_node_addr
	NodeID   string `json:"node,omitempty"`
	NodeAddr string `json:"addr,omitempty"`
}

// job is one job's replicated state. Lease deadlines are deliberately not
// here: they are leader-local soft state (see soft.go).
type job struct {
	ID             string        `json:"id"`
	Type           string        `json:"type"`
	Payload        []byte        `json:"payload"`
	IdempotencyKey string        `json:"key"`
	Status         string        `json:"status"`
	Attempts       int           `json:"attempts"`
	MaxAttempts    int           `json:"max"`
	LeaseOwner     string        `json:"owner"`
	LeaseToken     int64         `json:"token"`
	ReportedToken  int64         `json:"reported"`
	CreatedAt      int64         `json:"created"`
	EnqueuedAt     int64         `json:"enqueued"`
	LeasedAt       int64         `json:"leased"`
	FinishedAt     int64         `json:"finished"`
	LastError      string        `json:"last_error"`
	Result         []byte        `json:"result"`
	TraceParent    string        `json:"tp,omitempty"`
	Events         []store.Event `json:"events"`
}

type worker struct {
	ID           string `json:"id"`
	Hostname     string `json:"host"`
	Concurrency  int    `json:"conc"`
	State        string `json:"state"`
	RegisteredAt int64  `json:"registered"`
	DiedAt       int64  `json:"died,omitempty"`
}

// readyItem orders the ready queue like Redis' ZSET: by eligible time,
// then by id (UUIDv7 ids sort by creation).
type readyItem struct {
	at int64
	id string
}

func readyLess(a, b readyItem) bool {
	if a.at != b.at {
		return a.at < b.at
	}
	return a.id < b.id
}

// fsm is the replicated state machine. Apply runs on Raft's FSM goroutine;
// reads take mu for reading.
type fsm struct {
	mu      sync.RWMutex
	jobs    map[string]*job
	ready   *btree.BTreeG[readyItem]
	idem    map[string]string
	workers map[string]*worker
	// held maps worker -> job ids it holds leases on.
	held map[string]map[string]bool
	// leased is the set of LEASED job ids.
	leased map[string]bool
	// nodeAddrs maps Raft server id -> gRPC address, for leader hints.
	nodeAddrs map[string]string

	// onApply, if set, observes every applied command's result on this
	// replica (the leader uses it to maintain soft lease state).
	onApply func(cmd *command, res any)
}

func newFSM() *fsm {
	f := &fsm{}
	f.reset()
	return f
}

func (f *fsm) reset() {
	f.jobs = map[string]*job{}
	f.ready = btree.NewG(32, readyLess)
	f.idem = map[string]string{}
	f.workers = map[string]*worker{}
	f.held = map[string]map[string]bool{}
	f.leased = map[string]bool{}
	f.nodeAddrs = map[string]string{}
}

// Results of applying commands, returned through ApplyFuture.Response.
type (
	submitResult struct {
		ID        string
		Duplicate bool
	}
	acquireResult struct {
		NoWorker       bool
		Job            *store.Job
		EligibleAt     int64
		More           bool
		NextEligibleIn int64 // ms; -1 when the ready queue is empty
	}
	reportResult struct {
		Code      string // "OK", "ALREADY", "NOT_FOUND", "STALE", "RETRY", "FAILED", "DEAD"
		Status    string
		Owner     string
		Token     int64
		RetryInMs int64
	}
	revokeResult struct {
		Requeued, Dead int
		Workers        []string
	}
)

func (f *fsm) Apply(l *raft.Log) any {
	if l.Type != raft.LogCommand {
		return nil
	}
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		// A command this replica cannot decode would make replicas diverge;
		// failing loudly is the only safe option.
		panic(fmt.Sprintf("raftstore: undecodable log entry %d: %v", l.Index, err))
	}
	f.mu.Lock()
	res := f.apply(&c, l.Index)
	f.mu.Unlock()
	if f.onApply != nil {
		f.onApply(&c, res)
	}
	return res
}

func (f *fsm) apply(c *command, index uint64) any {
	switch c.Op {
	case opSubmit:
		return f.submit(c)
	case opRegister:
		f.workers[c.Worker] = &worker{ID: c.Worker, Hostname: c.Hostname, Concurrency: c.Concurrency,
			State: store.WorkerAlive, RegisteredAt: c.NowMs}
		return nil
	case opAcquire:
		return f.acquire(c, int64(index))
	case opComplete:
		return f.complete(c)
	case opFail:
		return f.fail(c)
	case opRelease:
		return f.release(c)
	case opExpireLeases:
		return f.expireLeases(c)
	case opExpireWorker:
		return f.expireWorkers(c)
	case opSetNodeAddr:
		f.nodeAddrs[c.NodeID] = c.NodeAddr
		return nil
	}
	panic(fmt.Sprintf("raftstore: unknown op %q at index %d", c.Op, index))
}

func (f *fsm) event(j *job, at int64, kind, worker string, token int64, detail string) {
	j.Events = append(j.Events, store.Event{AtMs: at, Kind: kind, Worker: worker, Token: token, Detail: detail})
}

func (f *fsm) submit(c *command) submitResult {
	if c.Key != "" {
		if id, ok := f.idem[c.Key]; ok {
			return submitResult{ID: id, Duplicate: true}
		}
		f.idem[c.Key] = c.ID
	}
	j := &job{ID: c.ID, Type: c.Type, Payload: c.Payload, IdempotencyKey: c.Key, Status: store.StatusPending,
		MaxAttempts: c.MaxAttempts, CreatedAt: c.NowMs, EnqueuedAt: c.NowMs, TraceParent: c.TraceParent}
	f.event(j, c.NowMs, "SUBMITTED", "", 0, "")
	f.jobs[j.ID] = j
	f.ready.ReplaceOrInsert(readyItem{c.NowMs, j.ID})
	return submitResult{ID: j.ID}
}

func (f *fsm) aliveWorker(id string) bool {
	w := f.workers[id]
	return w != nil && w.State == store.WorkerAlive
}

// firstReady returns the head of the ready queue.
func (f *fsm) firstReady() (readyItem, bool) {
	return f.ready.Min()
}

func (f *fsm) acquire(c *command, token int64) acquireResult {
	if !f.aliveWorker(c.Worker) {
		return acquireResult{NoWorker: true}
	}
	head, ok := f.firstReady()
	if !ok {
		return acquireResult{NextEligibleIn: -1}
	}
	if head.at > c.NowMs {
		return acquireResult{NextEligibleIn: head.at - c.NowMs}
	}
	f.ready.Delete(head)
	j := f.jobs[head.id]
	j.Attempts++
	j.LeaseToken = token
	j.Status = store.StatusLeased
	j.LeaseOwner = c.Worker
	j.LeasedAt = c.NowMs
	f.leased[j.ID] = true
	f.hold(c.Worker, j.ID)
	f.event(j, c.NowMs, "LEASED", c.Worker, token, "")
	more := false
	if next, ok := f.firstReady(); ok && next.at <= c.NowMs {
		more = true
	}
	return acquireResult{Job: j.public(), EligibleAt: head.at, More: more}
}

func (f *fsm) hold(w, id string) {
	if f.held[w] == nil {
		f.held[w] = map[string]bool{}
	}
	f.held[w][id] = true
}

func (f *fsm) unhold(w, id string) {
	delete(f.held[w], id)
	if len(f.held[w]) == 0 {
		delete(f.held, w)
	}
}

// endLease clears a job's lease bookkeeping (but not its owner and token,
// which stay as the record of who held it last, as in Redis).
func (f *fsm) endLease(j *job) {
	delete(f.leased, j.ID)
	f.unhold(j.LeaseOwner, j.ID)
}

func (f *fsm) complete(c *command) reportResult {
	j := f.jobs[c.Job]
	if j == nil {
		return reportResult{Code: "NOT_FOUND"}
	}
	owner, cur := j.LeaseOwner, j.LeaseToken
	// Without fencing (relay_nofence test builds only), any report on a
	// leased job is accepted, whoever holds the lease.
	if !store.FencingEnabled && j.Status == store.StatusLeased {
		owner, cur = c.Worker, c.Token
	}
	if owner == c.Worker && cur == c.Token {
		if j.Status == store.StatusLeased {
			f.endLease(j)
			j.Status, j.Result, j.FinishedAt, j.ReportedToken = store.StatusSucceeded, c.Result, c.NowMs, c.Token
			f.event(j, c.NowMs, "SUCCEEDED", c.Worker, c.Token, "")
			return reportResult{Code: "OK"}
		}
		if j.Status == store.StatusSucceeded && j.ReportedToken == c.Token {
			return reportResult{Code: "ALREADY"}
		}
	}
	return reportResult{Code: "STALE", Status: j.Status, Owner: j.LeaseOwner, Token: j.LeaseToken}
}

func (f *fsm) fail(c *command) reportResult {
	j := f.jobs[c.Job]
	if j == nil {
		return reportResult{Code: "NOT_FOUND"}
	}
	owner, cur := j.LeaseOwner, j.LeaseToken
	if !store.FencingEnabled && j.Status == store.StatusLeased {
		owner, cur = c.Worker, c.Token
	}
	stale := reportResult{Code: "STALE", Status: j.Status, Owner: j.LeaseOwner, Token: j.LeaseToken}
	if owner != c.Worker || cur != c.Token {
		return stale
	}
	if j.Status != store.StatusLeased {
		if j.ReportedToken == c.Token {
			return reportResult{Code: "ALREADY", Status: j.Status}
		}
		return stale
	}
	f.endLease(j)
	j.ReportedToken = c.Token
	j.LastError = c.Err
	if c.Retryable && j.Attempts < j.MaxAttempts {
		// Same arithmetic as fail.lua: d = min(max, base * 2^(attempts-1)),
		// delay = floor(d/2 + jitter*d/2).
		d := math.Min(float64(c.MaxMs), float64(c.BaseMs)*math.Pow(2, float64(j.Attempts-1)))
		delay := int64(math.Floor(d/2 + c.Jitter*d/2))
		j.Status, j.EnqueuedAt = store.StatusPending, c.NowMs+delay
		f.ready.ReplaceOrInsert(readyItem{j.EnqueuedAt, j.ID})
		f.event(j, c.NowMs, "RETRY", c.Worker, c.Token, c.Err)
		return reportResult{Code: "RETRY", RetryInMs: delay}
	}
	final := store.StatusFailed
	if c.Retryable {
		final = store.StatusDead
	}
	j.Status, j.FinishedAt = final, c.NowMs
	f.event(j, c.NowMs, final, c.Worker, c.Token, c.Err)
	return reportResult{Code: final}
}

func (f *fsm) release(c *command) reportResult {
	j := f.jobs[c.Job]
	if j == nil || j.Status != store.StatusLeased || j.LeaseOwner != c.Worker || j.LeaseToken != c.Token {
		return reportResult{Code: "STALE"}
	}
	f.endLease(j)
	j.Attempts--
	j.Status = store.StatusPending
	f.ready.ReplaceOrInsert(readyItem{j.EnqueuedAt, j.ID})
	f.event(j, c.NowMs, "RELEASED", c.Worker, c.Token, "acquire response undeliverable")
	return reportResult{Code: "OK"}
}

// revoke takes a lease away (expiry or worker death) and requeues the job
// or, with no attempts left, dead-letters it. Returns true if dead.
func (f *fsm) revoke(j *job, now int64, reason string) bool {
	f.endLease(j)
	if j.Attempts >= j.MaxAttempts {
		j.Status, j.FinishedAt, j.LastError = store.StatusDead, now, reason+"; attempts exhausted"
		f.event(j, now, "DEAD", j.LeaseOwner, j.LeaseToken, reason)
		return true
	}
	j.Status, j.EnqueuedAt, j.LastError = store.StatusPending, now, reason
	f.ready.ReplaceOrInsert(readyItem{now, j.ID})
	f.event(j, now, "REQUEUED", j.LeaseOwner, j.LeaseToken, reason)
	return false
}

// expireLeases revokes each (job, token) lease that is still current. The
// leader proposes these from its soft deadlines; the token check makes a
// proposal that raced with a completion or a newer lease a no-op.
func (f *fsm) expireLeases(c *command) revokeResult {
	var r revokeResult
	for _, l := range c.Leases {
		j := f.jobs[l.JobID]
		if j == nil || j.Status != store.StatusLeased || j.LeaseToken != l.Token {
			continue
		}
		if f.revoke(j, c.NowMs, "lease expired") {
			r.Dead++
		} else {
			r.Requeued++
		}
	}
	return r
}

// expireWorkers declares workers dead and revokes every lease they hold,
// in job-id order so every replica appends events identically.
func (f *fsm) expireWorkers(c *command) revokeResult {
	var r revokeResult
	for _, wid := range c.Workers {
		w := f.workers[wid]
		if w == nil || w.State != store.WorkerAlive {
			continue
		}
		w.State, w.DiedAt = store.WorkerDead, c.NowMs
		r.Workers = append(r.Workers, wid)
		for _, id := range sortedSet(f.held[wid]) {
			j := f.jobs[id]
			if j.Status == store.StatusLeased && j.LeaseOwner == wid {
				if f.revoke(j, c.NowMs, "worker "+wid+" declared dead") {
					r.Dead++
				} else {
					r.Requeued++
				}
			}
		}
		delete(f.held, wid)
	}
	return r
}

func sortedSet(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// public converts to the store type. LeaseExpiresAt is filled in by the
// caller from soft state.
func (j *job) public() *store.Job {
	return &store.Job{
		ID: j.ID, Type: j.Type, Payload: append([]byte(nil), j.Payload...), IdempotencyKey: j.IdempotencyKey,
		Status: j.Status, Attempts: j.Attempts, MaxAttempts: j.MaxAttempts,
		LeaseOwner: j.LeaseOwner, LeaseToken: j.LeaseToken,
		CreatedAt: j.CreatedAt, EnqueuedAt: j.EnqueuedAt, LeasedAt: j.LeasedAt, FinishedAt: j.FinishedAt,
		LastError: j.LastError, Result: append([]byte(nil), j.Result...), TraceParent: j.TraceParent,
	}
}

// snapshotState is the serialized FSM. Maps are emitted by encoding/json
// with sorted keys, so equal states serialize to equal bytes.
type snapshotState struct {
	Jobs      map[string]*job    `json:"jobs"`
	Idem      map[string]string  `json:"idem"`
	Workers   map[string]*worker `json:"workers"`
	NodeAddrs map[string]string  `json:"node_addrs"`
}

// Snapshot serializes the state immediately (on the FSM goroutine, where
// no Apply can run concurrently) so Persist can write it while Apply
// continues. The cost is one O(state) encode per snapshot.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	b, err := f.marshal()
	if err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: b}, nil
}

func (f *fsm) marshal() ([]byte, error) {
	return json.Marshal(snapshotState{Jobs: f.jobs, Idem: f.idem, Workers: f.workers, NodeAddrs: f.nodeAddrs})
}

// Restore replaces all state and rebuilds the derived indexes (ready
// queue, held and leased sets) from the jobs.
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var st snapshotState
	if err := json.NewDecoder(rc).Decode(&st); err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reset()
	if st.Jobs != nil {
		f.jobs = st.Jobs
	}
	if st.Idem != nil {
		f.idem = st.Idem
	}
	if st.Workers != nil {
		f.workers = st.Workers
	}
	if st.NodeAddrs != nil {
		f.nodeAddrs = st.NodeAddrs
	}
	for _, j := range f.jobs {
		switch j.Status {
		case store.StatusPending:
			f.ready.ReplaceOrInsert(readyItem{j.EnqueuedAt, j.ID})
		case store.StatusLeased:
			f.leased[j.ID] = true
			f.hold(j.LeaseOwner, j.ID)
		}
	}
	return nil
}

type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, bytes.NewReader(s.data)); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// describe is a short summary used in logs.
func (f *fsm) describe() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var b strings.Builder
	fmt.Fprintf(&b, "jobs=%d ready=%d leased=%d workers=%d", len(f.jobs), f.ready.Len(), len(f.leased), len(f.workers))
	return b.String()
}
