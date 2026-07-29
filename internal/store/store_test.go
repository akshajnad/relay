package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/akshajnad/relay/internal/redistest"
	"github.com/redis/go-redis/v9"
)

func TestMain(m *testing.M) { redistest.Main(m) }

func newStore(t *testing.T, mut func(*Config)) (*Store, *redis.Client) {
	t.Helper()
	rdb := redistest.Client(t)
	cfg := DefaultConfig()
	if mut != nil {
		mut(&cfg)
	}
	s := New(rdb, cfg)
	if err := s.LoadScripts(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, rdb
}

func mustSubmit(t *testing.T, s *Store, p SubmitParams) string {
	t.Helper()
	if p.Type == "" {
		p.Type = "noop"
	}
	id, dup, err := s.Submit(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if dup {
		t.Fatalf("unexpected duplicate for %+v", p)
	}
	return id
}

func mustRegister(t *testing.T, s *Store, host string) string {
	t.Helper()
	id, err := s.RegisterWorker(context.Background(), host, 1)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustAcquire(t *testing.T, s *Store, worker string) *Job {
	t.Helper()
	a, err := s.Acquire(context.Background(), worker)
	if err != nil {
		t.Fatal(err)
	}
	if a.Job == nil {
		t.Fatal("expected a job, queue was empty")
	}
	return a.Job
}

func mustGet(t *testing.T, s *Store, id string) (*Job, []Event) {
	t.Helper()
	j, ev, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return j, ev
}

// eventually polls cond until it returns true or the timeout elapses.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func kinds(ev []Event) []string {
	out := make([]string, len(ev))
	for i, e := range ev {
		out[i] = e.Kind
	}
	return out
}

func countKind(ev []Event, kind string) int {
	n := 0
	for _, e := range ev {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

func asStale(t *testing.T, err error) *StaleLeaseError {
	t.Helper()
	var se *StaleLeaseError
	if !errors.As(err, &se) {
		t.Fatalf("expected StaleLeaseError, got %v", err)
	}
	return se
}

func TestSubmitAndGet(t *testing.T) {
	s, _ := newStore(t, nil)
	id := mustSubmit(t, s, SubmitParams{Type: "sleep", Payload: []byte{0, 1, 2, 255}, MaxAttempts: 4})
	j, ev := mustGet(t, s, id)
	if j.Status != StatusPending || j.Type != "sleep" || j.MaxAttempts != 4 || j.Attempts != 0 {
		t.Fatalf("unexpected job %+v", j)
	}
	if string(j.Payload) != string([]byte{0, 1, 2, 255}) {
		t.Fatalf("payload not binary safe: %v", j.Payload)
	}
	if j.CreatedAt == 0 || j.EnqueuedAt != j.CreatedAt {
		t.Fatalf("timestamps not set: %+v", j)
	}
	if len(ev) != 1 || ev[0].Kind != "SUBMITTED" {
		t.Fatalf("history = %v", kinds(ev))
	}
	if _, _, err := s.Get(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrNotFound", err)
	}

	id2 := mustSubmit(t, s, SubmitParams{})
	if j2, _ := mustGet(t, s, id2); j2.MaxAttempts != DefaultConfig().DefaultMaxAttempts {
		t.Fatalf("default max attempts not applied: %d", j2.MaxAttempts)
	}
}

func TestSubmitIdempotent(t *testing.T) {
	s, rdb := newStore(t, nil)
	ctx := context.Background()
	id := mustSubmit(t, s, SubmitParams{IdempotencyKey: "k1"})
	again, dup, err := s.Submit(ctx, SubmitParams{Type: "noop", IdempotencyKey: "k1"})
	if err != nil || !dup || again != id {
		t.Fatalf("resubmit = (%s, %v, %v), want (%s, true, nil)", again, dup, err, id)
	}
	if other := mustSubmit(t, s, SubmitParams{IdempotencyKey: "k2"}); other == id {
		t.Fatal("different keys produced the same job")
	}

	// Many concurrent submits with one key create exactly one job.
	const n = 50
	ids := make([]string, n)
	dups := make([]bool, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var err error
			ids[i], dups[i], err = s.Submit(ctx, SubmitParams{Type: "noop", IdempotencyKey: "race"})
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	created := 0
	for i := range ids {
		if ids[i] != ids[0] {
			t.Fatalf("concurrent submits returned different ids: %s vs %s", ids[i], ids[0])
		}
		if !dups[i] {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("%d submits reported creating the job, want 1", created)
	}
	if n := rdb.ZCard(ctx, Prefix+"ready").Val(); n != 3 {
		t.Fatalf("ready has %d jobs, want 3", n)
	}
}

func TestAcquire(t *testing.T) {
	s, rdb := newStore(t, nil)
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")

	a, err := s.Acquire(ctx, w)
	if err != nil || a.Job != nil || a.NextEligibleIn != -1 {
		t.Fatalf("empty acquire = %+v, %v", a, err)
	}

	ids := []string{mustSubmit(t, s, SubmitParams{}), mustSubmit(t, s, SubmitParams{}), mustSubmit(t, s, SubmitParams{})}
	for i, want := range ids {
		a, err := s.Acquire(ctx, w)
		if err != nil {
			t.Fatal(err)
		}
		j := a.Job
		if j == nil || j.ID != want {
			t.Fatalf("acquire %d got %+v, want job %s (FIFO)", i, j, want)
		}
		if j.Status != StatusLeased || j.LeaseOwner != w || j.LeaseToken != 1 || j.Attempts != 1 {
			t.Fatalf("lease fields wrong: %+v", j)
		}
		if j.LeasedAt != a.RedisNowMs || j.LeaseExpiresAt != a.RedisNowMs+s.cfg.LeaseDuration.Milliseconds() {
			t.Fatalf("lease times wrong: %+v now=%d", j, a.RedisNowMs)
		}
		if a.EligibleAtMs != j.EnqueuedAt {
			t.Fatalf("eligible %d != enqueued %d", a.EligibleAtMs, j.EnqueuedAt)
		}
		if score := rdb.ZScore(ctx, Prefix+"leases", j.ID).Val(); int64(score) != j.LeaseExpiresAt {
			t.Fatalf("leases score %v != expiry %d", score, j.LeaseExpiresAt)
		}
		if !rdb.SIsMember(ctx, Prefix+"worker:"+w+":jobs", j.ID).Val() {
			t.Fatal("job missing from worker's job set")
		}
	}
	if a, _ := s.Acquire(ctx, w); a.Job != nil {
		t.Fatal("acquired from an empty queue")
	}
	if n := rdb.ZCard(ctx, Prefix+"ready").Val(); n != 0 {
		t.Fatalf("ready still has %d", n)
	}
}

func TestAcquireUnknownWorker(t *testing.T) {
	s, _ := newStore(t, nil)
	mustSubmit(t, s, SubmitParams{})
	if _, err := s.Acquire(context.Background(), "w-nobody"); !errors.Is(err, ErrWorkerNotAlive) {
		t.Fatalf("acquire by unknown worker = %v", err)
	}
}

func TestHeartbeatRenewsOnlyReportedLeases(t *testing.T) {
	s, _ := newStore(t, nil)
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")
	id1, id2 := mustSubmit(t, s, SubmitParams{}), mustSubmit(t, s, SubmitParams{})
	j1, j2 := mustAcquire(t, s, w), mustAcquire(t, s, w)

	eventually(t, 2*time.Second, "lease renewal", func() bool {
		lost, err := s.Heartbeat(ctx, w, []Lease{{JobID: id1, Token: j1.LeaseToken}})
		if err != nil || len(lost) != 0 {
			t.Fatalf("heartbeat = %v, %v", lost, err)
		}
		cur, _ := mustGet(t, s, id1)
		return cur.LeaseExpiresAt > j1.LeaseExpiresAt
	})
	if cur, _ := mustGet(t, s, id2); cur.LeaseExpiresAt != j2.LeaseExpiresAt {
		t.Fatal("unreported lease was renewed")
	}

	lost, err := s.Heartbeat(ctx, w, []Lease{{JobID: id2, Token: 99}, {JobID: "missing", Token: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 2 || lost[0] != (Lease{JobID: id2, Token: 99}) || lost[1].JobID != "missing" {
		t.Fatalf("lost = %+v", lost)
	}
	if _, err := s.Heartbeat(ctx, "w-nobody", nil); !errors.Is(err, ErrWorkerNotAlive) {
		t.Fatalf("heartbeat by unknown worker = %v", err)
	}
}

func TestCompleteFencing(t *testing.T) {
	s, rdb := newStore(t, nil)
	ctx := context.Background()
	a, b := mustRegister(t, s, "host-a"), mustRegister(t, s, "host-b")
	id := mustSubmit(t, s, SubmitParams{})
	j := mustAcquire(t, s, a)

	asStale(t, func() error { _, err := s.Complete(ctx, id, a, j.LeaseToken+1, []byte("x")); return err }())
	asStale(t, func() error { _, err := s.Complete(ctx, id, b, j.LeaseToken, []byte("x")); return err }())
	if _, err := s.Complete(ctx, "missing", a, 1, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("complete missing = %v", err)
	}

	already, err := s.Complete(ctx, id, a, j.LeaseToken, []byte("first"))
	if err != nil || already {
		t.Fatalf("complete = %v, %v", already, err)
	}
	already, err = s.Complete(ctx, id, a, j.LeaseToken, []byte("second"))
	if err != nil || !already {
		t.Fatalf("retried complete = %v, %v; want already", already, err)
	}
	got, ev := mustGet(t, s, id)
	if got.Status != StatusSucceeded || string(got.Result) != "first" || got.FinishedAt == 0 {
		t.Fatalf("job after complete = %+v", got)
	}
	if countKind(ev, "SUCCEEDED") != 1 {
		t.Fatalf("history = %v", kinds(ev))
	}
	if rdb.ZScore(ctx, Prefix+"leases", id).Err() != redis.Nil {
		t.Fatal("completed job still in leases")
	}
	if rdb.SIsMember(ctx, Prefix+"worker:"+a+":jobs", id).Val() {
		t.Fatal("completed job still in worker's job set")
	}
	// A failure report on a completed lease is not a new transition.
	if out, err := s.Fail(ctx, id, a, j.LeaseToken, "late", true); err != nil || !out.Already || out.Status != StatusSucceeded {
		t.Fatalf("fail after complete = %+v, %v", out, err)
	}
}

// TestZombieCompletionRejected walks the zombie scenario at the store level:
// A's lease expires and is reaped, B gets the job, and A's late result must
// not land, before or after B completes.
func TestZombieCompletionRejected(t *testing.T) {
	s, _ := newStore(t, func(c *Config) { c.LeaseDuration = 50 * time.Millisecond })
	ctx := context.Background()
	a, b := mustRegister(t, s, "host-a"), mustRegister(t, s, "host-b")
	id := mustSubmit(t, s, SubmitParams{MaxAttempts: 3})
	ja := mustAcquire(t, s, a)

	eventually(t, 2*time.Second, "lease to be reaped", func() bool {
		r, err := s.ReapExpired(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return r.Requeued == 1
	})
	jb := mustAcquire(t, s, b)
	if jb.ID != id || jb.LeaseToken != ja.LeaseToken+1 || jb.Attempts != 2 {
		t.Fatalf("reassigned job = %+v", jb)
	}

	se := asStale(t, func() error { _, err := s.Complete(ctx, id, a, ja.LeaseToken, []byte("zombie")); return err }())
	if se.CurrentOwner != b || se.CurrentToken != jb.LeaseToken || se.Status != StatusLeased {
		t.Fatalf("stale error = %+v", se)
	}
	if _, err := s.Complete(ctx, id, b, jb.LeaseToken, []byte("b")); err != nil {
		t.Fatal(err)
	}
	asStale(t, func() error { _, err := s.Complete(ctx, id, a, ja.LeaseToken, []byte("zombie")); return err }())
	asStale(t, func() error { _, err := s.Fail(ctx, id, a, ja.LeaseToken, "zombie", true); return err }())

	got, ev := mustGet(t, s, id)
	if got.Status != StatusSucceeded || string(got.Result) != "b" || got.LeaseOwner != b {
		t.Fatalf("job = %+v", got)
	}
	want := []string{"SUBMITTED", "LEASED", "REQUEUED", "LEASED", "SUCCEEDED"}
	if fmt.Sprint(kinds(ev)) != fmt.Sprint(want) {
		t.Fatalf("history = %v, want %v", kinds(ev), want)
	}
}

func TestFailRetryWithBackoff(t *testing.T) {
	s, rdb := newStore(t, func(c *Config) { c.BackoffBase = 200 * time.Millisecond })
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")
	id := mustSubmit(t, s, SubmitParams{MaxAttempts: 3})

	for attempt := 1; attempt <= 2; attempt++ {
		j := mustAcquire(t, s, w)
		if j.Attempts != attempt {
			t.Fatalf("attempts = %d, want %d", j.Attempts, attempt)
		}
		out, err := s.Fail(ctx, id, w, j.LeaseToken, fmt.Sprintf("boom %d", attempt), true)
		if err != nil {
			t.Fatal(err)
		}
		// Equal jitter: delay is in [d/2, d] for d = base * 2^(attempt-1).
		d := s.cfg.BackoffBase << (attempt - 1)
		if out.Status != StatusPending || out.RetryIn < d/2 || out.RetryIn > d {
			t.Fatalf("attempt %d: outcome %+v, want retry in [%v, %v]", attempt, out, d/2, d)
		}
		failed, _ := mustGet(t, s, id)
		if failed.LastError != fmt.Sprintf("boom %d", attempt) || failed.Status != StatusPending {
			t.Fatalf("after fail: %+v", failed)
		}
		if score := rdb.ZScore(ctx, Prefix+"ready", id).Val(); int64(score) != failed.EnqueuedAt {
			t.Fatalf("ready score %v != enqueued_at %d", score, failed.EnqueuedAt)
		}

		// Not eligible yet: acquire reports how long until it is.
		a, err := s.Acquire(ctx, w)
		if err != nil || a.Job != nil || a.NextEligibleIn <= 0 || a.NextEligibleIn > d {
			t.Fatalf("acquire during backoff = %+v, %v", a, err)
		}
		var next *Job
		eventually(t, 3*time.Second, "job to become eligible", func() bool {
			a, err := s.Acquire(ctx, w)
			if err != nil {
				t.Fatal(err)
			}
			next = a.Job
			return next != nil
		})
		if next.LeasedAt < failed.EnqueuedAt {
			t.Fatalf("leased at %d before eligible at %d", next.LeasedAt, failed.EnqueuedAt)
		}
		// Hand the lease back so the next loop iteration re-acquires it.
		if err := s.Release(ctx, id, w, next.LeaseToken); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFailExhaustedGoesDead(t *testing.T) {
	s, rdb := newStore(t, func(c *Config) { c.BackoffBase = time.Millisecond })
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")
	id := mustSubmit(t, s, SubmitParams{MaxAttempts: 2})

	j := mustAcquire(t, s, w)
	if out, _ := s.Fail(ctx, id, w, j.LeaseToken, "e1", true); out.Status != StatusPending {
		t.Fatalf("first failure: %+v", out)
	}
	var j2 *Job
	eventually(t, 2*time.Second, "retry", func() bool {
		a, _ := s.Acquire(ctx, w)
		j2 = a.Job
		return j2 != nil
	})
	out, err := s.Fail(ctx, id, w, j2.LeaseToken, "e2", true)
	if err != nil || out.Status != StatusDead {
		t.Fatalf("second failure = %+v, %v", out, err)
	}
	// A retried report of the same failure is recognized, not re-applied.
	if out, err := s.Fail(ctx, id, w, j2.LeaseToken, "e2", true); err != nil || !out.Already || out.Status != StatusDead {
		t.Fatalf("retried fail = %+v, %v", out, err)
	}
	got, ev := mustGet(t, s, id)
	if got.Status != StatusDead || got.Attempts != 2 || got.LastError != "e2" || got.FinishedAt == 0 {
		t.Fatalf("job = %+v", got)
	}
	if rdb.ZScore(ctx, Prefix+"dead", id).Err() != nil {
		t.Fatal("job missing from dead-letter set")
	}
	want := []string{"SUBMITTED", "LEASED", "RETRY", "LEASED", "DEAD"}
	if fmt.Sprint(kinds(ev)) != fmt.Sprint(want) {
		t.Fatalf("history = %v, want %v", kinds(ev), want)
	}
}

func TestFailNonRetryable(t *testing.T) {
	s, rdb := newStore(t, nil)
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")
	id := mustSubmit(t, s, SubmitParams{MaxAttempts: 5})
	j := mustAcquire(t, s, w)
	out, err := s.Fail(ctx, id, w, j.LeaseToken, "bad input", false)
	if err != nil || out.Status != StatusFailed {
		t.Fatalf("fail = %+v, %v", out, err)
	}
	got, _ := mustGet(t, s, id)
	if got.Status != StatusFailed || got.LastError != "bad input" {
		t.Fatalf("job = %+v", got)
	}
	if rdb.ZScore(ctx, Prefix+"dead", id).Err() != redis.Nil {
		t.Fatal("non-retryable failure should not be dead-lettered")
	}
	if rdb.ZCard(ctx, Prefix+"ready").Val() != 0 {
		t.Fatal("non-retryable failure was requeued")
	}
}

func TestReapExpiredLeases(t *testing.T) {
	s, rdb := newStore(t, func(c *Config) { c.LeaseDuration = 50 * time.Millisecond; c.ScanBatch = 2 })
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")
	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, mustSubmit(t, s, SubmitParams{MaxAttempts: 2}))
		mustAcquire(t, s, w)
	}
	// The last job is on its final attempt, so expiry kills it.
	if err := rdb.HSet(ctx, Prefix+"job:"+ids[4], "max_attempts", 1).Err(); err != nil {
		t.Fatal(err)
	}

	var total RevokeResult
	eventually(t, 2*time.Second, "all leases reaped", func() bool {
		r, err := s.ReapExpired(ctx)
		if err != nil {
			t.Fatal(err)
		}
		total.Requeued += r.Requeued
		total.Dead += r.Dead
		return total.Requeued+total.Dead == 5
	})
	if total.Requeued != 4 || total.Dead != 1 {
		t.Fatalf("reaped %+v, want 4 requeued and 1 dead", total)
	}
	for _, id := range ids[:4] {
		j, ev := mustGet(t, s, id)
		if j.Status != StatusPending || j.Attempts != 1 || ev[len(ev)-1].Kind != "REQUEUED" {
			t.Fatalf("job %s = %+v %v", id, j, kinds(ev))
		}
	}
	if j, _ := mustGet(t, s, ids[4]); j.Status != StatusDead {
		t.Fatalf("exhausted job = %+v", j)
	}
	if rdb.ZCard(ctx, Prefix+"leases").Val() != 0 || rdb.SCard(ctx, Prefix+"worker:"+w+":jobs").Val() != 0 {
		t.Fatal("reaped leases left behind in leases or worker set")
	}
	if rdb.ZCard(ctx, Prefix+"ready").Val() != 4 {
		t.Fatal("requeued jobs missing from ready")
	}
}

func TestExpireWorkers(t *testing.T) {
	s, rdb := newStore(t, nil)
	ctx := context.Background()
	a, b := mustRegister(t, s, "host-a"), mustRegister(t, s, "host-b")
	mustSubmit(t, s, SubmitParams{})
	mustSubmit(t, s, SubmitParams{})
	mustSubmit(t, s, SubmitParams{})
	held := []*Job{mustAcquire(t, s, a), mustAcquire(t, s, a)}
	jb := mustAcquire(t, s, b)

	const deadAfter = 100 * time.Millisecond
	var expired []string
	eventually(t, 3*time.Second, "worker a to be declared dead", func() bool {
		// b keeps heartbeating; a has stopped.
		if _, err := s.Heartbeat(ctx, b, []Lease{{JobID: jb.ID, Token: jb.LeaseToken}}); err != nil {
			t.Fatal(err)
		}
		r, ws, err := s.ExpireWorkers(ctx, deadAfter)
		if err != nil {
			t.Fatal(err)
		}
		if len(ws) > 0 {
			if r.Requeued != 2 {
				t.Fatalf("requeued %d jobs, want 2", r.Requeued)
			}
			expired = ws
			return true
		}
		return false
	})
	if len(expired) != 1 || expired[0] != a {
		t.Fatalf("expired workers = %v, want [%s]", expired, a)
	}
	for _, j := range held {
		got, ev := mustGet(t, s, j.ID)
		if got.Status != StatusPending || ev[len(ev)-1].Kind != "REQUEUED" {
			t.Fatalf("held job after worker death = %+v %v", got, kinds(ev))
		}
	}
	if got, _ := mustGet(t, s, jb.ID); got.Status != StatusLeased || got.LeaseOwner != b {
		t.Fatalf("live worker's job disturbed: %+v", got)
	}
	if _, err := s.Acquire(ctx, a); !errors.Is(err, ErrWorkerNotAlive) {
		t.Fatalf("dead worker acquire = %v", err)
	}
	if _, err := s.Heartbeat(ctx, a, nil); !errors.Is(err, ErrWorkerNotAlive) {
		t.Fatalf("dead worker heartbeat = %v", err)
	}
	if rdb.Exists(ctx, Prefix+"worker:"+a+":jobs").Val() != 0 {
		t.Fatal("dead worker's job set not cleared")
	}

	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]string{}
	for _, w := range workers {
		states[w.ID] = w.State
		if w.ID == b && (w.ActiveJobs != 1 || w.Hostname != "host-b") {
			t.Fatalf("worker b = %+v", w)
		}
	}
	if states[a] != "DEAD" || states[b] != "ALIVE" {
		t.Fatalf("worker states = %v", states)
	}
}

func TestReleaseUndoesAttempt(t *testing.T) {
	s, rdb := newStore(t, nil)
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")
	first := mustSubmit(t, s, SubmitParams{})
	mustSubmit(t, s, SubmitParams{})
	j := mustAcquire(t, s, w)

	if err := s.Release(ctx, first, w, j.LeaseToken+1); err == nil {
		t.Fatal("release with wrong token succeeded")
	}
	if err := s.Release(ctx, first, w, j.LeaseToken); err != nil {
		t.Fatal(err)
	}
	got, ev := mustGet(t, s, first)
	if got.Status != StatusPending || got.Attempts != 0 || ev[len(ev)-1].Kind != "RELEASED" {
		t.Fatalf("released job = %+v %v", got, kinds(ev))
	}
	if rdb.ZCard(ctx, Prefix+"leases").Val() != 0 {
		t.Fatal("released lease still tracked")
	}
	// It keeps its place at the head of the queue, under a new token.
	again := mustAcquire(t, s, w)
	if again.ID != first || again.LeaseToken != j.LeaseToken+1 || again.Attempts != 1 {
		t.Fatalf("re-acquired %+v", again)
	}
	asStale(t, func() error { _, err := s.Complete(ctx, first, w, j.LeaseToken, nil); return err }())
}
