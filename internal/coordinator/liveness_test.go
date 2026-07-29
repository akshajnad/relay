package coordinator

import (
	"context"
	"testing"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/store"
	"google.golang.org/grpc/codes"
)

func fastLiveness(c *Config) {
	c.HeartbeatInterval = 50 * time.Millisecond
	c.DeadAfter = 150 * time.Millisecond
	c.ReapInterval = 50 * time.Millisecond
	c.StartupGrace = 0
}

func runLiveness(t *testing.T, h *harness) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.c.RunLiveness(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

// A worker that stops heartbeating is declared dead, its job is handed to a
// live worker that is already long-polling, and its late result is rejected.
func TestDeadWorkerJobsRequeued(t *testing.T) {
	h := start(t, fastLiveness, nil) // 10s leases: the detector, not the reaper, must act
	ctx := context.Background()
	runLiveness(t, h)
	a, b := h.register(t), h.register(t)
	id := h.submit(t, &relayv1.SubmitJobRequest{MaxAttempts: 3})
	ja := h.acquire(t, a)

	// b heartbeats in the background; a has "crashed" and sends nothing.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
				h.workers.Heartbeat(ctx, &relayv1.HeartbeatRequest{WorkerId: b})
			}
		}
	}()

	started := time.Now()
	jb := h.acquire(t, b) // long-polls until the detector requeues a's job
	if jb.Id != id || jb.Attempts != 2 || jb.LeaseToken != ja.LeaseToken+1 {
		t.Fatalf("reassigned job = %v", jb)
	}
	if took := time.Since(started); took > 3*time.Second {
		t.Fatalf("failover took %v", took)
	}

	_, err := h.workers.Heartbeat(ctx, &relayv1.HeartbeatRequest{WorkerId: a})
	wantCode(t, err, codes.FailedPrecondition)
	_, err = h.workers.CompleteJob(ctx, &relayv1.CompleteJobRequest{JobId: id, WorkerId: a, LeaseToken: ja.LeaseToken})
	wantCode(t, err, codes.Aborted)
	if h.metrics.Snapshot().Counters.WorkersExpired != 1 {
		t.Fatal("worker expiry not counted")
	}
}

// The reaper requeues a lease that expires while its worker stays alive,
// e.g. because the worker never learned it held the lease.
func TestExpiredLeaseReapedWhileWorkerAlive(t *testing.T) {
	h := start(t, fastLiveness, func(s *store.Config) { s.LeaseDuration = 100 * time.Millisecond })
	ctx := context.Background()
	runLiveness(t, h)
	w := h.register(t)
	id := h.submit(t, &relayv1.SubmitJobRequest{})
	h.acquire(t, w)

	// w heartbeats but does not report the lease, so it is not renewed.
	eventually(t, 3*time.Second, "lease reaped", func() bool {
		if _, err := h.workers.Heartbeat(ctx, &relayv1.HeartbeatRequest{WorkerId: w}); err != nil {
			t.Fatal(err)
		}
		return h.get(t, id).Job.Status == relayv1.JobStatus_JOB_STATUS_PENDING
	})
	if h.metrics.Snapshot().Counters.WorkersExpired != 0 {
		t.Fatal("live worker was declared dead")
	}
}

// Renewed leases survive far past their nominal duration.
func TestHeartbeatKeepsLeaseAlive(t *testing.T) {
	h := start(t, fastLiveness, func(s *store.Config) { s.LeaseDuration = 100 * time.Millisecond })
	ctx := context.Background()
	runLiveness(t, h)
	w := h.register(t)
	id := h.submit(t, &relayv1.SubmitJobRequest{})
	j := h.acquire(t, w)
	held := []*relayv1.HeldLease{{JobId: id, LeaseToken: j.LeaseToken}}
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		resp, err := h.workers.Heartbeat(ctx, &relayv1.HeartbeatRequest{WorkerId: w, Held: held})
		if err != nil || len(resp.Lost) != 0 {
			t.Fatalf("heartbeat = %v, %v", resp, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := h.get(t, id).Job; got.Status != relayv1.JobStatus_JOB_STATUS_LEASED || got.LeaseToken != j.LeaseToken {
		t.Fatalf("lease lost despite heartbeats: %v", got)
	}
}
