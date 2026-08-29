package raftstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/akshajnad/relay/internal/store"
	"github.com/akshajnad/relay/internal/store/storetest"
	"github.com/hashicorp/raft"
)

// testCluster is an in-memory Raft cluster. Nodes can be stopped and
// restarted on the same stores, and partitioned at the transport.
type testCluster struct {
	t      *testing.T
	scfg   store.Config
	mut    func(*Config)
	nodes  []*Store
	trans  []*raft.InmemTransport
	stores []*Stores
}

func nodeID(i int) string   { return "n" + strconv.Itoa(i) }
func nodeAddr(i int) string { return "raft-" + strconv.Itoa(i) }
func grpcAddr(i int) string { return "coord-" + strconv.Itoa(i) + ":50051" }

func newCluster(t *testing.T, n int, scfg store.Config, mut func(*Config)) *testCluster {
	t.Helper()
	c := &testCluster{t: t, scfg: scfg, mut: mut, nodes: make([]*Store, n), trans: make([]*raft.InmemTransport, n), stores: make([]*Stores, n)}
	for i := 0; i < n; i++ {
		c.stores[i] = NewInmemStores()
		c.start(i, i == 0)
	}
	t.Cleanup(func() {
		for i, s := range c.nodes {
			if s != nil {
				_ = s.Close()
				c.nodes[i] = nil
			}
		}
	})
	return c
}

func (c *testCluster) peers() []Peer {
	ps := make([]Peer, len(c.nodes))
	for i := range ps {
		ps[i] = Peer{ID: nodeID(i), RaftAddr: nodeAddr(i), GRPCAddr: grpcAddr(i)}
	}
	return ps
}

// start (re)starts node i on its existing stores with a fresh transport.
func (c *testCluster) start(i int, bootstrap bool) {
	c.t.Helper()
	_, tr := raft.NewInmemTransport(raft.ServerAddress(nodeAddr(i)))
	c.trans[i] = tr
	for j, other := range c.trans {
		if j != i && other != nil {
			tr.Connect(raft.ServerAddress(nodeAddr(j)), other)
			other.Connect(raft.ServerAddress(nodeAddr(i)), tr)
		}
	}
	cfg := DefaultConfig()
	cfg.Store = c.scfg
	cfg.NodeID, cfg.RaftBind, cfg.GRPCAdvertise = nodeID(i), nodeAddr(i), grpcAddr(i)
	cfg.Bootstrap, cfg.Peers = bootstrap, c.peers()
	cfg.HeartbeatTimeout, cfg.ElectionTimeout, cfg.LeaderLeaseTimeout = 50*time.Millisecond, 50*time.Millisecond, 50*time.Millisecond
	cfg.ApplyTimeout = 2 * time.Second
	cfg.Transport, cfg.Stores, cfg.RaftLog = tr, c.stores[i], io.Discard
	if c.mut != nil {
		c.mut(&cfg)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(testWriter{c.t}, nil))
	}
	s, err := Open(cfg, log)
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[i] = s
}

func (c *testCluster) stop(i int) {
	c.t.Helper()
	if err := c.nodes[i].Close(); err != nil {
		c.t.Fatal(err)
	}
	c.nodes[i] = nil
	for j, other := range c.trans {
		if j != i && other != nil {
			other.Disconnect(raft.ServerAddress(nodeAddr(i)))
		}
	}
	c.trans[i] = nil
}

// isolate cuts node i off from every peer (both directions).
func (c *testCluster) isolate(i int) {
	c.trans[i].DisconnectAll()
	for j, other := range c.trans {
		if j != i && other != nil {
			other.Disconnect(raft.ServerAddress(nodeAddr(i)))
		}
	}
}

func (c *testCluster) heal(i int) {
	for j, other := range c.trans {
		if j != i && other != nil {
			c.trans[i].Connect(raft.ServerAddress(nodeAddr(j)), other)
			other.Connect(raft.ServerAddress(nodeAddr(i)), c.trans[i])
		}
	}
}

// leader waits for a serving leader, optionally excluding one node.
func (c *testCluster) leader(exclude ...int) (int, *Store) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
	nodes:
		for i, s := range c.nodes {
			for _, x := range exclude {
				if i == x {
					continue nodes
				}
			}
			if s != nil && s.IsLeader() {
				return i, s
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return -1, nil
}

// waitApplied waits until every running node has applied the leader's
// last index.
func (c *testCluster) waitApplied() {
	c.t.Helper()
	_, l := c.leader()
	want := l.raft.LastIndex()
	deadline := time.Now().Add(10 * time.Second)
	for {
		done := true
		for _, s := range c.nodes {
			if s != nil && s.raft.AppliedIndex() < want {
				done = false
			}
		}
		if done {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("replicas did not apply index %d", want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// waitConverged waits until node i's FSM serializes to want. Raft's
// AppliedIndex advances when a batch is handed to the FSM goroutine, not
// when Apply returns, so replicas are compared by polling.
func (c *testCluster) waitConverged(i int, want []byte) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !bytes.Equal(marshalFSM(c.t, c.nodes[i]), want) {
		if time.Now().After(deadline) {
			c.t.Fatalf("node %d did not converge to the expected state", i)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(bytes.TrimRight(p, "\n")))
	return len(p), nil
}

func TestConformanceSingleNode(t *testing.T) {
	storetest.Run(t, func(t *testing.T, cfg store.Config) store.Store {
		_, s := newCluster(t, 1, cfg, nil).leader()
		return s
	})
}

func TestConformanceThreeNodes(t *testing.T) {
	storetest.Run(t, func(t *testing.T, cfg store.Config) store.Store {
		_, s := newCluster(t, 3, cfg, nil).leader()
		return s
	})
}

// workload drives a mix of every transition through the leader.
func workload(t *testing.T, s *Store, jobs int) {
	t.Helper()
	ctx := context.Background()
	w1, err := s.RegisterWorker(ctx, "a", 4)
	if err != nil {
		t.Fatal(err)
	}
	w2, err := s.RegisterWorker(ctx, "b", 4)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < jobs; i++ {
		if _, _, err := s.Submit(ctx, store.SubmitParams{Type: "noop", IdempotencyKey: fmt.Sprintf("k%d", i%(jobs-3)), MaxAttempts: 3}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; ; i++ {
		w := w1
		if i%2 == 1 {
			w = w2
		}
		a, err := s.Acquire(ctx, w)
		if err != nil {
			t.Fatal(err)
		}
		if a.Job == nil {
			break
		}
		switch i % 4 {
		case 0:
			_, err = s.Complete(ctx, a.Job.ID, w, a.Job.LeaseToken, []byte("ok"))
		case 1:
			_, err = s.Fail(ctx, a.Job.ID, w, a.Job.LeaseToken, "nope", false)
		case 2:
			err = s.Release(ctx, a.Job.ID, w, a.Job.LeaseToken)
			if err == nil {
				// leave it for a later acquire
				continue
			}
		case 3:
			// leave it leased
		}
		if err != nil {
			t.Fatal(err)
		}
		if i > 4*jobs {
			break
		}
	}
	if _, err := s.propose(&command{Op: opExpireWorker, NowMs: time.Now().UnixMilli(), Workers: []string{w2}}); err != nil {
		t.Fatal(err)
	}
}

func marshalFSM(t *testing.T, s *Store) []byte {
	t.Helper()
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	b, err := s.fsm.marshal()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestReplicasAreIdentical checks the FSM is deterministic: every replica,
// and a fresh FSM replaying the leader's log, ends in byte-identical state.
func TestReplicasAreIdentical(t *testing.T) {
	c := newCluster(t, 3, store.DefaultConfig(), nil)
	_, leader := c.leader()
	workload(t, leader, 40)
	c.waitApplied()
	want := marshalFSM(t, leader)
	for i := range c.nodes {
		c.waitConverged(i, want)
	}

	li, _ := c.leader()
	replay := newFSM()
	logs := c.stores[li].Log
	first, _ := logs.FirstIndex()
	last, _ := logs.LastIndex()
	for i := first; i <= last; i++ {
		var l raft.Log
		if err := logs.GetLog(i, &l); err != nil {
			t.Fatal(err)
		}
		replay.Apply(&l)
	}
	got, err := replay.marshal()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("replaying the log produced a different state")
	}
}

// TestSnapshotRestoreAndCatchUp stops a follower, writes past a snapshot
// that compacts the log beyond the follower's position, and restarts it:
// it must install the snapshot, catch up, and match the leader.
func TestSnapshotRestoreAndCatchUp(t *testing.T) {
	c := newCluster(t, 3, store.DefaultConfig(), func(cfg *Config) {
		cfg.SnapshotThreshold, cfg.TrailingLogs, cfg.SnapshotInterval = 16, 4, 50*time.Millisecond
	})
	li, leader := c.leader()
	follower := (li + 1) % 3
	workload(t, leader, 10)
	c.waitApplied()
	stoppedAt := c.nodes[follower].raft.LastIndex()
	c.stop(follower)

	li, leader = c.leader()
	workload(t, leader, 60)
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	first, _ := c.stores[li].Log.FirstIndex()
	if first <= stoppedAt {
		t.Fatalf("log not compacted past the follower: first index %d, follower at %d", first, stoppedAt)
	}

	c.start(follower, false)
	c.waitApplied()
	c.waitConverged(follower, marshalFSM(t, leader))
	snaps, err := c.stores[follower].Snapshot.List()
	if err != nil || len(snaps) == 0 {
		t.Fatalf("follower has no installed snapshot: %v %v", snaps, err)
	}
	if snaps[0].Index <= stoppedAt {
		t.Fatalf("follower's snapshot %d does not cover the gap after %d", snaps[0].Index, stoppedAt)
	}
}

// TestFullClusterRestart stops every node and restarts them on their
// stores (snapshot + log replay); no state is lost.
func TestFullClusterRestart(t *testing.T) {
	c := newCluster(t, 3, store.DefaultConfig(), func(cfg *Config) {
		cfg.SnapshotThreshold, cfg.TrailingLogs = 16, 4
	})
	_, leader := c.leader()
	workload(t, leader, 30)
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	workload(t, leader, 5) // entries after the snapshot are replayed from the log
	c.waitApplied()
	want := marshalFSM(t, leader)
	for i := range c.nodes {
		c.stop(i)
	}
	for i := range c.nodes {
		c.start(i, true) // bootstrap is a no-op on existing state
	}
	c.leader()
	c.waitApplied()
	for i := range c.nodes {
		c.waitConverged(i, want)
	}
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	c := newCluster(t, 3, store.DefaultConfig(), nil)
	li, _ := c.leader()
	f := c.nodes[(li+1)%3]
	ctx := context.Background()
	calls := map[string]error{
		"submit":  func() error { _, _, err := f.Submit(ctx, store.SubmitParams{Type: "noop"}); return err }(),
		"acquire": func() error { _, err := f.Acquire(ctx, "w"); return err }(),
		"renew":   func() error { _, err := f.Renew(ctx, "w", nil); return err }(),
		"get":     func() error { _, _, err := f.Get(ctx, "x"); return err }(),
		"reap":    func() error { _, err := f.ReapExpired(ctx); return err }(),
	}
	for name, err := range calls {
		var nl *store.NotLeaderError
		if !errors.As(err, &nl) {
			t.Fatalf("%s on follower = %v, want NotLeaderError", name, err)
		}
		if nl.LeaderID != nodeID(li) || nl.LeaderAddr != grpcAddr(li) {
			t.Fatalf("%s hint = %+v, want %s at %s", name, nl, nodeID(li), grpcAddr(li))
		}
	}
}

// TestFailoverKeepsFencing: a lease granted by one leader is revoked by the
// next one (after a full grace period), the job is re-leased under a higher
// token, and the first holder's late completion is rejected.
func TestFailoverKeepsFencing(t *testing.T) {
	scfg := store.DefaultConfig()
	scfg.LeaseDuration = 300 * time.Millisecond
	c := newCluster(t, 3, scfg, nil)
	ctx := context.Background()
	li, leader := c.leader()
	w1, _ := leader.RegisterWorker(ctx, "a", 1)
	w2, _ := leader.RegisterWorker(ctx, "b", 1)
	id, _, err := leader.Submit(ctx, store.SubmitParams{Type: "noop", MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	a, err := leader.Acquire(ctx, w1)
	if err != nil || a.Job == nil {
		t.Fatalf("acquire = %+v, %v", a, err)
	}
	c.waitApplied()
	c.stop(li)

	_, next := c.leader(li)
	start := time.Now()
	var requeued bool
	for !requeued && time.Since(start) < 5*time.Second {
		r, err := next.ReapExpired(ctx)
		if err != nil {
			t.Fatal(err)
		}
		requeued = r.Requeued == 1
		// w2 keeps heartbeating; w1 is gone with the old leader.
		if _, err := next.Renew(ctx, w2, nil); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !requeued {
		t.Fatal("new leader never expired the old leader's lease")
	}
	if waited := time.Since(start); waited < scfg.LeaseDuration {
		t.Fatalf("lease expired after %v, before a full lease period on the new leader", waited)
	}
	b, err := next.Acquire(ctx, w2)
	if err != nil || b.Job == nil || b.Job.ID != id {
		t.Fatalf("re-acquire = %+v, %v", b, err)
	}
	if b.Job.LeaseToken <= a.Job.LeaseToken {
		t.Fatalf("token %d after failover does not exceed %d", b.Job.LeaseToken, a.Job.LeaseToken)
	}
	var stale *store.StaleLeaseError
	if _, err := next.Complete(ctx, id, w1, a.Job.LeaseToken, []byte("zombie")); !errors.As(err, &stale) {
		t.Fatalf("zombie completion = %v", err)
	}
	if _, err := next.Complete(ctx, id, w2, b.Job.LeaseToken, []byte("ok")); err != nil {
		t.Fatal(err)
	}
}

// TestMinorityLeaderStepsDown partitions the leader away: it must stop
// serving, announce the loss, and never acknowledge a write, while the
// majority elects a new leader that keeps all acknowledged state.
func TestMinorityLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 3, store.DefaultConfig(), nil)
	ctx := context.Background()
	li, old := c.leader()
	<-old.LeaderChanges() // true
	acked, _, err := old.Submit(ctx, store.SubmitParams{Type: "noop", IdempotencyKey: "before"})
	if err != nil {
		t.Fatal(err)
	}
	c.waitApplied()

	c.isolate(li)
	select {
	case v := <-old.LeaderChanges():
		if v {
			t.Fatal("got leadership again instead of losing it")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("isolated leader never announced losing leadership")
	}
	if _, _, err := old.Submit(ctx, store.SubmitParams{Type: "noop", IdempotencyKey: "during"}); err == nil {
		t.Fatal("isolated old leader acknowledged a write")
	}
	if _, _, err := old.Get(ctx, acked); err == nil {
		t.Fatal("isolated old leader served a read")
	}

	_, next := c.leader(li)
	if j, _, err := next.Get(ctx, acked); err != nil || j.IdempotencyKey != "before" {
		t.Fatalf("acknowledged job after failover = %+v, %v", j, err)
	}
	c.heal(li)
	c.waitApplied()
	c.waitConverged(li, marshalFSM(t, next))
}

// TestAddAndRemoveVoter grows a 1-node cluster to 3 (the new nodes catch
// up from a snapshot) and shrinks it again.
func TestAddAndRemoveVoter(t *testing.T) {
	c := newCluster(t, 1, store.DefaultConfig(), func(cfg *Config) {
		cfg.SnapshotThreshold, cfg.TrailingLogs = 16, 4
	})
	_, leader := c.leader()
	workload(t, leader, 30)
	if err := leader.raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	// Two more nodes join an existing cluster: no bootstrap.
	c.nodes = append(c.nodes, nil, nil)
	c.trans = append(c.trans, nil, nil)
	c.stores = append(c.stores, NewInmemStores(), NewInmemStores())
	ctx := context.Background()
	for i := 1; i <= 2; i++ {
		c.start(i, false)
		if err := leader.AddVoter(ctx, nodeID(i), nodeAddr(i), grpcAddr(i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitApplied()
	want := marshalFSM(t, leader)
	for i := 1; i <= 2; i++ {
		c.waitConverged(i, want)
	}
	st, err := c.nodes[2].ClusterStatus()
	if err != nil || len(st.Servers) != 3 || st.LeaderID != nodeID(0) || st.LeaderAddr != grpcAddr(0) {
		t.Fatalf("status = %+v, %v", st, err)
	}
	// A follower cannot change membership.
	var nl *store.NotLeaderError
	if err := c.nodes[1].RemoveServer(ctx, nodeID(2)); !errors.As(err, &nl) {
		t.Fatalf("remove on follower = %v", err)
	}
	if err := leader.RemoveServer(ctx, nodeID(2)); err != nil {
		t.Fatal(err)
	}
	st, _ = leader.ClusterStatus()
	if len(st.Servers) != 2 {
		t.Fatalf("servers after remove = %+v", st.Servers)
	}
}
