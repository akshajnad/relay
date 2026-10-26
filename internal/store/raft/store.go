// Package raftstore is a store.Store replicated with Raft
// (github.com/hashicorp/raft, raft-boltdb for the log and stable store).
//
// All job and worker state lives in the FSM, and every mutation is a Raft
// command whose timestamps the leader stamps when it proposes. Fencing
// tokens are the Raft log index of the entry that granted the lease.
//
// Lease deadlines and worker heartbeats are deliberately NOT replicated:
// they are soft state on the leader, so a heartbeat costs no log write.
// The leader's reaper proposes ExpireLease(job, token), which the FSM
// applies only if the token is still current. A new leader resets every
// deadline and heartbeat to now, giving all workers a full grace period.
// This is safe because fencing, not the deadline, is what prevents a
// double completion: losing soft state can only delay recovery.
package raftstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/akshajnad/relay/internal/store"
	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// Peer is one cluster member.
type Peer struct {
	ID       string
	RaftAddr string
	// GRPCAddr is the address clients reach the member's coordinator on,
	// returned in NOT_LEADER hints.
	GRPCAddr string
}

type Config struct {
	Store store.Config

	NodeID string
	// RaftBind is the address the Raft TCP transport listens on, and
	// RaftAdvertise the address peers dial (defaults to RaftBind).
	RaftBind      string
	RaftAdvertise string
	GRPCAdvertise string
	// DataDir holds raft.db and snapshots. Empty means in-memory (tests).
	DataDir string
	// Bootstrap makes this node bootstrap the cluster from Peers if it has
	// no existing state. Every bootstrapping node must use identical Peers.
	Bootstrap bool
	Peers     []Peer

	ApplyTimeout       time.Duration
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	SnapshotThreshold  uint64
	SnapshotInterval   time.Duration
	TrailingLogs       uint64

	// Transport overrides the TCP transport (tests use raft's in-memory
	// transport). Stores override the log, stable and snapshot stores.
	Transport raft.Transport
	Stores    *Stores

	// RaftLog receives hashicorp/raft's own log output.
	RaftLog io.Writer
}

// Stores bundles a node's persistent stores, so a test can restart a node
// on the same state.
type Stores struct {
	Log      raft.LogStore
	Stable   raft.StableStore
	Snapshot raft.SnapshotStore
}

// NewInmemStores returns fresh in-memory stores.
func NewInmemStores() *Stores {
	m := raft.NewInmemStore()
	return &Stores{Log: m, Stable: m, Snapshot: raft.NewInmemSnapshotStore()}
}

func DefaultConfig() Config {
	return Config{
		Store:              store.DefaultConfig(),
		ApplyTimeout:       5 * time.Second,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		SnapshotThreshold:  8192,
		SnapshotInterval:   30 * time.Second,
		TrailingLogs:       10240,
	}
}

var _ store.Store = (*Store)(nil)
var _ store.Leadership = (*Store)(nil)

// Store is the Raft implementation of store.Store. Only the leader serves
// calls; every other node returns *store.NotLeaderError.
type Store struct {
	cfg     Config
	raft    *raft.Raft
	fsm     *fsm
	trans   raft.Transport
	closers []io.Closer
	log     *slog.Logger
	jitter  func() float64

	softMu sync.Mutex
	// serving is true once this node is leader and has caught up (Barrier)
	// and reset its soft state.
	serving bool
	gen     uint64
	leases  map[string]softLease // job id -> deadline of its current lease
	beats   map[string]softBeat  // worker id -> last heartbeat

	changes chan bool
	done    chan struct{}
	wg      sync.WaitGroup

	// observe receives Raft commit latencies (Apply until the future
	// resolves) when set.
	observe func(time.Duration)
}

type softLease struct {
	token    int64
	deadline time.Time // monotonic
	wallMs   int64     // the same deadline on the wall clock, for display
}

type softBeat struct {
	at     time.Time
	wallMs int64
}

// Open starts this node's Raft instance.
func Open(cfg Config, log *slog.Logger) (*Store, error) {
	if cfg.RaftAdvertise == "" {
		cfg.RaftAdvertise = cfg.RaftBind
	}
	s := &Store{
		cfg: cfg, fsm: newFSM(), log: log, jitter: rand.Float64,
		leases: map[string]softLease{}, beats: map[string]softBeat{},
		changes: make(chan bool, 64), done: make(chan struct{}),
	}
	s.fsm.onApply = s.onApply

	notify := make(chan bool, 16)
	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)
	rc.NotifyCh = notify
	rc.HeartbeatTimeout = cfg.HeartbeatTimeout
	rc.ElectionTimeout = cfg.ElectionTimeout
	rc.LeaderLeaseTimeout = cfg.LeaderLeaseTimeout
	rc.SnapshotThreshold = cfg.SnapshotThreshold
	rc.SnapshotInterval = cfg.SnapshotInterval
	rc.TrailingLogs = cfg.TrailingLogs
	rc.BatchApplyCh = true
	out := cfg.RaftLog
	if out == nil {
		out = os.Stderr
	}
	rc.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Warn, Output: out, JSONFormat: true})

	stores := cfg.Stores
	if stores == nil {
		if cfg.DataDir == "" {
			stores = NewInmemStores()
		} else {
			if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
				return nil, err
			}
			bolt, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(cfg.DataDir, "raft.db")})
			if err != nil {
				return nil, fmt.Errorf("open raft.db: %w", err)
			}
			s.closers = append(s.closers, bolt)
			snaps, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, out)
			if err != nil {
				return nil, err
			}
			stores = &Stores{Log: bolt, Stable: bolt, Snapshot: snaps}
		}
	}

	trans := cfg.Transport
	if trans == nil {
		adv, err := net.ResolveTCPAddr("tcp", cfg.RaftAdvertise)
		if err != nil {
			return nil, fmt.Errorf("raft advertise address: %w", err)
		}
		t, err := raft.NewTCPTransport(cfg.RaftBind, adv, 3, 10*time.Second, out)
		if err != nil {
			return nil, fmt.Errorf("raft transport: %w", err)
		}
		trans = t
		s.closers = append(s.closers, t)
	}
	s.trans = trans

	r, err := raft.NewRaft(rc, s.fsm, stores.Log, stores.Stable, stores.Snapshot, trans)
	if err != nil {
		return nil, err
	}
	s.raft = r

	if cfg.Bootstrap {
		var servers []raft.Server
		for _, p := range cfg.Peers {
			servers = append(servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.RaftAddr), Suffrage: raft.Voter})
		}
		err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error()
		if err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}

	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.watchLeadership(notify) }()
	return s, nil
}

// Close shuts this node down.
func (s *Store) Close() error {
	close(s.done)
	err := s.raft.Shutdown().Error()
	s.wg.Wait()
	for _, c := range s.closers {
		if cerr := c.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

func (s *Store) Config() store.Config { return s.cfg.Store }

// Raft exposes the underlying node (tests, metrics).
func (s *Store) Raft() *raft.Raft { return s.raft }

// SetCommitObserver registers a callback for Raft commit latencies.
func (s *Store) SetCommitObserver(f func(time.Duration)) { s.observe = f }

func (s *Store) LeaderChanges() <-chan bool { return s.changes }

func (s *Store) IsLeader() bool {
	s.softMu.Lock()
	defer s.softMu.Unlock()
	return s.serving
}

// watchLeadership follows Raft leadership notifications. Becoming leader is
// asynchronous: the node first waits for a Barrier, so its FSM has applied
// every entry from earlier terms, then resets soft state, and only then
// serves.
func (s *Store) watchLeadership(notify <-chan bool) {
	for {
		select {
		case <-s.done:
			return
		case isLeader := <-notify:
			s.softMu.Lock()
			s.gen++
			gen := s.gen
			wasServing := s.serving
			s.serving = false
			s.leases, s.beats = map[string]softLease{}, map[string]softBeat{}
			s.softMu.Unlock()
			if wasServing {
				s.emit(false)
			}
			if isLeader {
				s.wg.Add(1)
				go func() { defer s.wg.Done(); s.becomeLeader(gen) }()
			}
		}
	}
}

func (s *Store) becomeLeader(gen uint64) {
	if err := s.raft.Barrier(s.cfg.ApplyTimeout).Error(); err != nil {
		s.log.Warn("raft: barrier after election failed; not serving", "err", err)
		return
	}
	now := time.Now()
	s.fsm.mu.RLock()
	leased := make(map[string]int64, len(s.fsm.leased))
	for id := range s.fsm.leased {
		leased[id] = s.fsm.jobs[id].LeaseToken
	}
	alive := s.fsm.aliveWorkerIDs()
	addrs := make(map[string]string, len(s.fsm.nodeAddrs))
	for k, v := range s.fsm.nodeAddrs {
		addrs[k] = v
	}
	s.fsm.mu.RUnlock()

	s.softMu.Lock()
	if s.gen != gen || s.raft.State() != raft.Leader {
		s.softMu.Unlock()
		return
	}
	// Every lease and every worker gets a full period from now: the old
	// leader's deadlines are gone, and nobody can be blamed for heartbeats
	// sent to it.
	for id, tok := range leased {
		s.leases[id] = s.newLease(tok, now)
	}
	for _, w := range alive {
		s.beats[w] = softBeat{at: now, wallMs: now.UnixMilli()}
	}
	s.serving = true
	s.softMu.Unlock()
	s.log.Info("raft: serving as leader", "node", s.cfg.NodeID, "leases_reset", len(leased), "workers", len(alive), "state", s.fsm.describe())
	s.emit(true)

	// Record client addresses for leader hints.
	for _, p := range append([]Peer{{ID: s.cfg.NodeID, GRPCAddr: s.cfg.GRPCAdvertise}}, s.cfg.Peers...) {
		if p.GRPCAddr != "" && addrs[p.ID] != p.GRPCAddr {
			addrs[p.ID] = p.GRPCAddr
			if _, err := s.propose(&command{Op: opSetNodeAddr, NowMs: now.UnixMilli(), NodeID: p.ID, NodeAddr: p.GRPCAddr}); err != nil {
				s.log.Warn("raft: recording node address failed", "node", p.ID, "err", err)
			}
		}
	}
}

func (s *Store) emit(v bool) {
	select {
	case s.changes <- v:
	case <-s.done:
	}
}

func (s *Store) newLease(token int64, now time.Time) softLease {
	d := s.cfg.Store.LeaseDuration
	return softLease{token: token, deadline: now.Add(d), wallMs: now.UnixMilli() + d.Milliseconds()}
}

// onApply keeps soft state in step with the FSM on the serving leader.
func (s *Store) onApply(c *command, res any) {
	s.softMu.Lock()
	defer s.softMu.Unlock()
	if !s.serving {
		return
	}
	switch c.Op {
	case opRegister:
		now := time.Now()
		s.beats[c.Worker] = softBeat{at: now, wallMs: now.UnixMilli()}
	case opAcquire:
		if r, ok := res.(acquireResult); ok && r.Job != nil {
			l := s.newLease(r.Job.LeaseToken, time.Now())
			// Report the deadline relative to the proposal's timestamp, as
			// the Redis backend does relative to the script's.
			l.wallMs = c.NowMs + s.cfg.Store.LeaseDuration.Milliseconds()
			s.leases[r.Job.ID] = l
		}
	case opComplete, opFail, opRelease:
		if l, ok := s.leases[c.Job]; ok && l.token == c.Token {
			delete(s.leases, c.Job)
		}
	case opExpireLeases:
		for _, l := range c.Leases {
			if cur, ok := s.leases[l.JobID]; ok && cur.token == l.Token {
				delete(s.leases, l.JobID)
			}
		}
	case opExpireWorker:
		for _, w := range c.Workers {
			delete(s.beats, w)
		}
	}
}

// notLeader builds the redirect error, naming the leader's client address
// when it is known.
func (s *Store) notLeader() error {
	_, id := s.raft.LeaderWithID()
	e := &store.NotLeaderError{LeaderID: string(id)}
	if id != "" {
		s.fsm.mu.RLock()
		e.LeaderAddr = s.fsm.nodeAddrs[string(id)]
		s.fsm.mu.RUnlock()
		if e.LeaderAddr == "" {
			for _, p := range s.cfg.Peers {
				if p.ID == string(id) {
					e.LeaderAddr = p.GRPCAddr
				}
			}
		}
	}
	return e
}

// propose replicates one command and returns the FSM's result. A
// NotLeaderError means the command was not appended; any other error means
// its outcome is unknown (it may yet commit).
func (s *Store) propose(c *command) (any, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	f := s.raft.Apply(b, s.cfg.ApplyTimeout)
	if err := f.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return nil, s.notLeader()
		}
		return nil, fmt.Errorf("raft apply %s: %w", c.Op, err)
	}
	if s.observe != nil {
		s.observe(time.Since(start))
	}
	return f.Response(), nil
}

// leaderPropose proposes only while serving as leader.
func (s *Store) leaderPropose(c *command) (any, error) {
	if !s.IsLeader() {
		return nil, s.notLeader()
	}
	return s.propose(c)
}

// verifyLeader confirms leadership with a quorum before a read, so a
// deposed leader that has not noticed yet cannot serve a stale read.
func (s *Store) verifyLeader() error {
	if !s.IsLeader() {
		return s.notLeader()
	}
	if err := s.raft.VerifyLeader().Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return s.notLeader()
		}
		return fmt.Errorf("verify leader: %w", err)
	}
	return nil
}

func (s *Store) Submit(_ context.Context, p store.SubmitParams) (string, bool, error) {
	if !s.IsLeader() {
		return "", false, s.notLeader()
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = s.cfg.Store.DefaultMaxAttempts
	}
	// A key already in the FSM is a duplicate: committed entries are never
	// undone, so this needs no log entry.
	if p.IdempotencyKey != "" {
		s.fsm.mu.RLock()
		id, ok := s.fsm.idem[p.IdempotencyKey]
		s.fsm.mu.RUnlock()
		if ok {
			return id, true, nil
		}
	}
	newID, err := uuid.NewV7()
	if err != nil {
		return "", false, err
	}
	res, err := s.propose(&command{Op: opSubmit, NowMs: time.Now().UnixMilli(), ID: newID.String(),
		Type: p.Type, Payload: p.Payload, Key: p.IdempotencyKey, MaxAttempts: p.MaxAttempts})
	if err != nil {
		return "", false, err
	}
	r := res.(submitResult)
	return r.ID, r.Duplicate, nil
}

func (s *Store) RegisterWorker(_ context.Context, hostname string, concurrency int) (string, error) {
	id := "w-" + hostname + "-" + uuid.NewString()[:8]
	_, err := s.leaderPropose(&command{Op: opRegister, NowMs: time.Now().UnixMilli(), Worker: id,
		Hostname: hostname, Concurrency: concurrency})
	return id, err
}

func (s *Store) Acquire(_ context.Context, workerID string) (store.Acquired, error) {
	if !s.IsLeader() {
		return store.Acquired{}, s.notLeader()
	}
	nowMs := time.Now().UnixMilli()
	// Look before proposing: an empty poll costs no log entry. Every
	// acknowledged submit and requeue has been applied here, so this view
	// only misses work that is concurrently arriving, and the notifier
	// wakes the poller for that.
	s.fsm.mu.RLock()
	alive := s.fsm.aliveWorker(workerID)
	head, ok := s.fsm.firstReady()
	s.fsm.mu.RUnlock()
	if !alive {
		return store.Acquired{}, store.ErrWorkerNotAlive
	}
	if !ok {
		return store.Acquired{StoreNowMs: nowMs, NextEligibleIn: -1}, nil
	}
	if head.at > nowMs {
		return store.Acquired{StoreNowMs: nowMs, NextEligibleIn: time.Duration(head.at-nowMs) * time.Millisecond}, nil
	}
	res, err := s.propose(&command{Op: opAcquire, NowMs: nowMs, Worker: workerID})
	if err != nil {
		return store.Acquired{}, err
	}
	r := res.(acquireResult)
	switch {
	case r.NoWorker:
		return store.Acquired{}, store.ErrWorkerNotAlive
	case r.Job == nil:
		in := time.Duration(-1)
		if r.NextEligibleIn >= 0 {
			in = time.Duration(r.NextEligibleIn) * time.Millisecond
		}
		return store.Acquired{StoreNowMs: nowMs, NextEligibleIn: in}, nil
	}
	r.Job.LeaseExpiresAt = nowMs + s.cfg.Store.LeaseDuration.Milliseconds()
	return store.Acquired{Job: r.Job, StoreNowMs: nowMs, EligibleAtMs: r.EligibleAt, More: r.More}, nil
}

// Renew is leader-local: it checks ownership against the FSM and moves
// soft deadlines, without writing to the log.
func (s *Store) Renew(_ context.Context, workerID string, held []store.Lease) ([]store.Lease, error) {
	if !s.IsLeader() {
		return nil, s.notLeader()
	}
	s.fsm.mu.RLock()
	alive := s.fsm.aliveWorker(workerID)
	var ok, lost []store.Lease
	for _, l := range held {
		j := s.fsm.jobs[l.JobID]
		if j != nil && j.Status == store.StatusLeased && j.LeaseOwner == workerID && j.LeaseToken == l.Token {
			ok = append(ok, l)
		} else {
			lost = append(lost, l)
		}
	}
	s.fsm.mu.RUnlock()
	if !alive {
		return nil, store.ErrWorkerNotAlive
	}
	now := time.Now()
	s.softMu.Lock()
	defer s.softMu.Unlock()
	if !s.serving {
		return nil, s.notLeader()
	}
	s.beats[workerID] = softBeat{at: now, wallMs: now.UnixMilli()}
	for _, l := range ok {
		s.leases[l.JobID] = s.newLease(l.Token, now)
	}
	if lost == nil {
		lost = []store.Lease{}
	}
	return lost, nil
}

func (s *Store) Complete(_ context.Context, jobID, workerID string, token int64, result []byte) (bool, error) {
	res, err := s.leaderPropose(&command{Op: opComplete, NowMs: time.Now().UnixMilli(), Job: jobID,
		Worker: workerID, Token: token, Result: result})
	if err != nil {
		return false, err
	}
	r := res.(reportResult)
	switch r.Code {
	case "OK":
		return false, nil
	case "ALREADY":
		return true, nil
	case "NOT_FOUND":
		return false, store.ErrNotFound
	}
	return false, &store.StaleLeaseError{JobID: jobID, Status: r.Status, CurrentOwner: r.Owner, CurrentToken: r.Token}
}

func (s *Store) Fail(_ context.Context, jobID, workerID string, token int64, errMsg string, retryable bool) (store.FailOutcome, error) {
	c := s.cfg.Store
	res, err := s.leaderPropose(&command{Op: opFail, NowMs: time.Now().UnixMilli(), Job: jobID, Worker: workerID,
		Token: token, Err: errMsg, Retryable: retryable, Jitter: s.jitter(),
		BaseMs: c.BackoffBase.Milliseconds(), MaxMs: c.BackoffMax.Milliseconds()})
	if err != nil {
		return store.FailOutcome{}, err
	}
	r := res.(reportResult)
	switch r.Code {
	case "RETRY":
		return store.FailOutcome{Status: store.StatusPending, RetryIn: time.Duration(r.RetryInMs) * time.Millisecond}, nil
	case store.StatusFailed, store.StatusDead:
		return store.FailOutcome{Status: r.Code}, nil
	case "ALREADY":
		return store.FailOutcome{Status: r.Status, Already: true}, nil
	case "NOT_FOUND":
		return store.FailOutcome{}, store.ErrNotFound
	}
	return store.FailOutcome{}, &store.StaleLeaseError{JobID: jobID, Status: r.Status, CurrentOwner: r.Owner, CurrentToken: r.Token}
}

func (s *Store) Release(_ context.Context, jobID, workerID string, token int64) error {
	res, err := s.leaderPropose(&command{Op: opRelease, NowMs: time.Now().UnixMilli(), Job: jobID, Worker: workerID, Token: token})
	if err != nil {
		return err
	}
	if res.(reportResult).Code != "OK" {
		return &store.StaleLeaseError{JobID: jobID}
	}
	return nil
}

// ReapExpired proposes ExpireLease for every lease whose soft deadline has
// passed. It first reconciles soft state with the FSM, so a lease the
// leader somehow has no deadline for still gets one.
func (s *Store) ReapExpired(_ context.Context) (store.RevokeResult, error) {
	if !s.IsLeader() {
		return store.RevokeResult{}, s.notLeader()
	}
	now := time.Now()
	s.fsm.mu.RLock()
	leased := make(map[string]int64, len(s.fsm.leased))
	for id := range s.fsm.leased {
		leased[id] = s.fsm.jobs[id].LeaseToken
	}
	s.fsm.mu.RUnlock()

	s.softMu.Lock()
	var expired []store.Lease
	for id, l := range s.leases {
		if tok, ok := leased[id]; !ok || tok != l.token {
			delete(s.leases, id) // the lease ended or was replaced
		}
	}
	for id, tok := range leased {
		l, ok := s.leases[id]
		if !ok {
			s.leases[id] = s.newLease(tok, now)
			continue
		}
		if now.After(l.deadline) {
			expired = append(expired, store.Lease{JobID: id, Token: l.token})
		}
	}
	s.softMu.Unlock()
	sort.Slice(expired, func(i, j int) bool { return expired[i].JobID < expired[j].JobID })

	var total store.RevokeResult
	for len(expired) > 0 {
		n := min(len(expired), s.cfg.Store.ScanBatch)
		res, err := s.propose(&command{Op: opExpireLeases, NowMs: now.UnixMilli(), Leases: expired[:n]})
		if err != nil {
			return total, err
		}
		r := res.(revokeResult)
		total.Requeued += r.Requeued
		total.Dead += r.Dead
		expired = expired[n:]
	}
	return total, nil
}

// ExpireWorkers declares dead every live worker whose last heartbeat to
// this leader is older than deadAfter. A worker this leader has never
// heard from starts its grace period now.
func (s *Store) ExpireWorkers(_ context.Context, deadAfter time.Duration) (store.RevokeResult, []string, error) {
	if !s.IsLeader() {
		return store.RevokeResult{}, nil, s.notLeader()
	}
	now := time.Now()
	s.fsm.mu.RLock()
	alive := s.fsm.aliveWorkerIDs()
	s.fsm.mu.RUnlock()
	var dead []string
	s.softMu.Lock()
	for _, w := range alive {
		b, ok := s.beats[w]
		if !ok {
			s.beats[w] = softBeat{at: now, wallMs: now.UnixMilli()}
			continue
		}
		if now.Sub(b.at) > deadAfter {
			dead = append(dead, w)
		}
	}
	s.softMu.Unlock()

	var total store.RevokeResult
	var workers []string
	for len(dead) > 0 {
		n := min(len(dead), s.cfg.Store.ScanBatch)
		res, err := s.propose(&command{Op: opExpireWorker, NowMs: now.UnixMilli(), Workers: dead[:n]})
		if err != nil {
			return total, workers, err
		}
		r := res.(revokeResult)
		total.Requeued += r.Requeued
		total.Dead += r.Dead
		workers = append(workers, r.Workers...)
		dead = dead[n:]
	}
	return total, workers, nil
}

func (s *Store) Get(_ context.Context, id string) (*store.Job, []store.Event, error) {
	if err := s.verifyLeader(); err != nil {
		return nil, nil, err
	}
	s.fsm.mu.RLock()
	j := s.fsm.jobs[id]
	var pub *store.Job
	var events []store.Event
	if j != nil {
		pub = j.public()
		events = append([]store.Event(nil), j.Events...)
	}
	s.fsm.mu.RUnlock()
	if pub == nil {
		return nil, nil, store.ErrNotFound
	}
	if pub.Status == store.StatusLeased {
		s.softMu.Lock()
		if l, ok := s.leases[id]; ok && l.token == pub.LeaseToken {
			pub.LeaseExpiresAt = l.wallMs
		}
		s.softMu.Unlock()
	}
	return pub, events, nil
}

func (s *Store) ListWorkers(_ context.Context) ([]store.Worker, error) {
	if err := s.verifyLeader(); err != nil {
		return nil, err
	}
	s.fsm.mu.RLock()
	out := make([]store.Worker, 0, len(s.fsm.workers))
	for _, w := range s.fsm.workers {
		out = append(out, store.Worker{ID: w.ID, Hostname: w.Hostname, Concurrency: w.Concurrency, State: w.State,
			RegisteredAt: w.RegisteredAt, LastHeartbeat: w.RegisteredAt, ActiveJobs: len(s.fsm.held[w.ID])})
	}
	s.fsm.mu.RUnlock()
	s.softMu.Lock()
	for i := range out {
		if b, ok := s.beats[out[i].ID]; ok {
			out[i].LastHeartbeat = b.wallMs
		}
	}
	s.softMu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// QueueDepth reads this node's FSM without a leadership check; it feeds
// metrics, which every node may report.
func (s *Store) QueueDepth(_ context.Context) (store.Depth, error) {
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	return store.Depth{Pending: int64(s.fsm.ready.Len()), Leased: int64(len(s.fsm.leased))}, nil
}

func (f *fsm) aliveWorkerIDs() []string {
	var ids []string
	for id, w := range f.workers {
		if w.State == store.WorkerAlive {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
