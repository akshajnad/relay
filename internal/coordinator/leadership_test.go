package coordinator

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/redistest"
	"github.com/akshajnad/relay/internal/rpc"
	"github.com/akshajnad/relay/internal/store"
	redisstore "github.com/akshajnad/relay/internal/store/redis"
)

// switchableLeader wraps a store with a leadership flag the test controls:
// while not leader, Acquire answers NOT_LEADER like a Raft follower.
type switchableLeader struct {
	store.Store
	leader  atomic.Bool
	changes chan bool
}

func (s *switchableLeader) LeaderChanges() <-chan bool { return s.changes }
func (s *switchableLeader) IsLeader() bool             { return s.leader.Load() }

func (s *switchableLeader) Acquire(ctx context.Context, w string) (store.Acquired, error) {
	if !s.leader.Load() {
		return store.Acquired{}, &store.NotLeaderError{LeaderID: "n2", LeaderAddr: "coord-2:50051"}
	}
	return s.Store.Acquire(ctx, w)
}

// TestLeadershipLossFailsPendingLongPolls: pollers blocked on an empty
// queue must get NOT_LEADER as soon as leadership is lost, not after the
// poll timeout.
func TestLeadershipLossFailsPendingLongPolls(t *testing.T) {
	inner := redisstore.New(redistest.Client(t), store.DefaultConfig())
	if err := inner.LoadScripts(context.Background()); err != nil {
		t.Fatal(err)
	}
	st := &switchableLeader{Store: inner, changes: make(chan bool, 1)}
	st.leader.Store(true)
	cfg := DefaultConfig()
	cfg.PollTimeout = 30 * time.Second
	cfg.StartupGrace = 0
	h := serve(t, st, cfg)
	w := h.register(t)

	const pollers = 5
	errs := make(chan error, pollers)
	for i := 0; i < pollers; i++ {
		go func() {
			_, err := h.workers.AcquireJob(context.Background(), &relayv1.AcquireJobRequest{WorkerId: w})
			errs <- err
		}()
	}
	eventually(t, 2*time.Second, "pollers to block", func() bool { return h.c.notifier.Waiting() == pollers })

	lost := time.Now()
	st.leader.Store(false)
	st.changes <- false
	for i := 0; i < pollers; i++ {
		err := <-errs
		id, addr, ok := rpc.ParseNotLeader(err)
		if !ok || id != "n2" || addr != "coord-2:50051" {
			t.Fatalf("poller got %v, want NOT_LEADER naming n2", err)
		}
	}
	if d := time.Since(lost); d > time.Second {
		t.Fatalf("pollers took %v to fail after leadership loss", d)
	}
	if got := h.metrics.LeaderChanges.Load(); got != 1 {
		t.Fatalf("leader changes = %d", got)
	}
}
