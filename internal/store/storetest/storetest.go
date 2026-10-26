// Package storetest is the conformance suite every store.Store backend
// must pass. It uses only the Store interface, so it asserts behaviour, not
// layout: fencing tokens are checked to increase, never for exact values,
// because the Redis backend counts per job and the Raft backend uses log
// indices.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/akshajnad/relay/internal/store"
)

// Factory returns a new, empty store with cfg. It registers its own cleanup.
type Factory func(t *testing.T, cfg store.Config) store.Store

// Run runs the whole suite against stores made by newStore. Subtests do not
// run in parallel, so a factory may reuse one backend and wipe it per call.
func Run(t *testing.T, newStore Factory) {
	tests := []struct {
		name string
		fn   func(*testing.T, Factory)
	}{
		{"SubmitAndGet", testSubmitAndGet},
		{"SubmitIdempotent", testSubmitIdempotent},
		{"Acquire", testAcquire},
		{"AcquireUnknownWorker", testAcquireUnknownWorker},
		{"ConcurrentAcquireLeasesEachJobOnce", testConcurrentAcquire},
		{"RenewOnlyReportedLeases", testRenewOnlyReported},
		{"CompleteFencing", testCompleteFencing},
		{"ZombieCompletionRejected", testZombieCompletionRejected},
		{"FailRetryWithBackoff", testFailRetryWithBackoff},
		{"FailExhaustedGoesDead", testFailExhaustedGoesDead},
		{"FailNonRetryable", testFailNonRetryable},
		{"ReapExpiredLeases", testReapExpiredLeases},
		{"ExpireWorkers", testExpireWorkers},
		{"ReleaseUndoesAttempt", testReleaseUndoesAttempt},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newStore) })
	}
}

func newWith(t *testing.T, f Factory, mut func(*store.Config)) store.Store {
	t.Helper()
	cfg := store.DefaultConfig()
	if mut != nil {
		mut(&cfg)
	}
	return f(t, cfg)
}

func mustSubmit(t *testing.T, s store.Store, p store.SubmitParams) string {
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

func mustRegister(t *testing.T, s store.Store, host string) string {
	t.Helper()
	id, err := s.RegisterWorker(context.Background(), host, 1)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func mustAcquire(t *testing.T, s store.Store, worker string) *store.Job {
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

func mustGet(t *testing.T, s store.Store, id string) (*store.Job, []store.Event) {
	t.Helper()
	j, ev, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return j, ev
}

func mustDepth(t *testing.T, s store.Store) store.Depth {
	t.Helper()
	d, err := s.QueueDepth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func workerByID(t *testing.T, s store.Store, id string) store.Worker {
	t.Helper()
	ws, err := s.ListWorkers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range ws {
		if w.ID == id {
			return w
		}
	}
	t.Fatalf("worker %s not listed", id)
	return store.Worker{}
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

func kinds(ev []store.Event) []string {
	out := make([]string, len(ev))
	for i, e := range ev {
		out[i] = e.Kind
	}
	return out
}

func countKind(ev []store.Event, kind string) int {
	n := 0
	for _, e := range ev {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

func asStale(t *testing.T, err error) *store.StaleLeaseError {
	t.Helper()
	var se *store.StaleLeaseError
	if !errors.As(err, &se) {
		t.Fatalf("expected StaleLeaseError, got %v", err)
	}
	return se
}

func testSubmitAndGet(t *testing.T, f Factory) {
	s := newWith(t, f, nil)
	id := mustSubmit(t, s, store.SubmitParams{Type: "sleep", Payload: []byte{0, 1, 2, 255}, MaxAttempts: 4})
	j, ev := mustGet(t, s, id)
	if j.ID != id || j.Status != store.StatusPending || j.Type != "sleep" || j.MaxAttempts != 4 || j.Attempts != 0 {
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
	if _, _, err := s.Get(context.Background(), "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrNotFound", err)
	}

	id2 := mustSubmit(t, s, store.SubmitParams{})
	if j2, _ := mustGet(t, s, id2); j2.MaxAttempts != s.Config().DefaultMaxAttempts {
		t.Fatalf("default max attempts not applied: %d", j2.MaxAttempts)
	}
	if d := mustDepth(t, s); d.Pending != 2 || d.Leased != 0 {
		t.Fatalf("depth = %+v", d)
	}
}

func testSubmitIdempotent(t *testing.T, f Factory) {
	s := newWith(t, f, nil)
	ctx := context.Background()
	id := mustSubmit(t, s, store.SubmitParams{IdempotencyKey: "k1"})
	again, dup, err := s.Submit(ctx, store.SubmitParams{Type: "noop", IdempotencyKey: "k1"})
	if err != nil || !dup || again != id {
		t.Fatalf("resubmit = (%s, %v, %v), want (%s, true, nil)", again, dup, err, id)
	}
	if other := mustSubmit(t, s, store.SubmitParams{IdempotencyKey: "k2"}); other == id {
		t.Fatal("different keys produced the same job")
	}
	if j, _ := mustGet(t, s, id); j.IdempotencyKey != "k1" {
		t.Fatalf("idempotency key not stored: %+v", j)
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
			ids[i], dups[i], err = s.Submit(ctx, store.SubmitParams{Type: "noop", IdempotencyKey: "race"})
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
	if d := mustDepth(t, s); d.Pending != 3 {
		t.Fatalf("%d pending jobs, want 3", d.Pending)
	}
}

func testAcquire(t *testing.T, f Factory) {
	s := newWith(t, f, nil)
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")

	a, err := s.Acquire(ctx, w)
	if err != nil || a.Job != nil || a.NextEligibleIn != -1 {
		t.Fatalf("empty acquire = %+v, %v", a, err)
	}

	ids := []string{mustSubmit(t, s, store.SubmitParams{}), mustSubmit(t, s, store.SubmitParams{}), mustSubmit(t, s, store.SubmitParams{})}
	for i, want := range ids {
		a, err := s.Acquire(ctx, w)
		if err != nil {
			t.Fatal(err)
		}
		j := a.Job
		if j == nil || j.ID != want {
			t.Fatalf("acquire %d got %+v, want job %s (FIFO)", i, j, want)
		}
		if j.Status != store.StatusLeased || j.LeaseOwner != w || j.LeaseToken <= 0 || j.Attempts != 1 {
			t.Fatalf("lease fields wrong: %+v", j)
		}
		if j.LeasedAt != a.StoreNowMs || j.LeaseExpiresAt != a.StoreNowMs+s.Config().LeaseDuration.Milliseconds() {
			t.Fatalf("lease times wrong: %+v now=%d", j, a.StoreNowMs)
		}
		if a.EligibleAtMs != j.EnqueuedAt {
			t.Fatalf("eligible %d != enqueued %d", a.EligibleAtMs, j.EnqueuedAt)
		}
		if a.More != (i < len(ids)-1) {
			t.Fatalf("acquire %d: more = %v", i, a.More)
		}
		got, ev := mustGet(t, s, j.ID)
		if got.Status != store.StatusLeased || got.LeaseToken != j.LeaseToken || ev[len(ev)-1].Kind != "LEASED" ||
			ev[len(ev)-1].Token != j.LeaseToken || ev[len(ev)-1].Worker != w {
			t.Fatalf("stored lease %+v %+v", got, ev)
		}
	}
	if a, _ := s.Acquire(ctx, w); a.Job != nil {
		t.Fatal("acquired from an empty queue")
	}
	if d := mustDepth(t, s); d.Pending != 0 || d.Leased != 3 {
		t.Fatalf("depth = %+v", d)
	}
	if wk := workerByID(t, s, w); wk.ActiveJobs != 3 || wk.State != store.WorkerAlive || wk.Hostname != "host-a" {
		t.Fatalf("worker = %+v", wk)
	}
}

func testAcquireUnknownWorker(t *testing.T, f Factory) {
	s := newWith(t, f, nil)
	mustSubmit(t, s, store.SubmitParams{})
	if _, err := s.Acquire(context.Background(), "w-nobody"); !errors.Is(err, store.ErrWorkerNotAlive) {
		t.Fatalf("acquire by unknown worker = %v", err)
	}
	if _, err := s.Renew(context.Background(), "w-nobody", nil); !errors.Is(err, store.ErrWorkerNotAlive) {
		t.Fatalf("renew by unknown worker = %v", err)
	}
}

// testConcurrentAcquire races many acquirers over a fixed set of jobs. Each
// job must be leased exactly once.
func testConcurrentAcquire(t *testing.T, f Factory) {
	s := newWith(t, f, nil)
	ctx := context.Background()
	const jobs, acquirers = 40, 8
	for i := 0; i < jobs; i++ {
		mustSubmit(t, s, store.SubmitParams{})
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for i := 0; i < acquirers; i++ {
		w := mustRegister(t, s, fmt.Sprintf("host-%d", i))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				a, err := s.Acquire(ctx, w)
				if err != nil {
					t.Error(err)
					return
				}
				if a.Job == nil {
					return
				}
				mu.Lock()
				seen[a.Job.ID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != jobs {
		t.Fatalf("leased %d distinct jobs, want %d", len(seen), jobs)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s leased %d times", id, n)
		}
	}
}

func testRenewOnlyReported(t *testing.T, f Factory) {
	s := newWith(t, f, nil)
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")
	id1, id2 := mustSubmit(t, s, store.SubmitParams{}), mustSubmit(t, s, store.SubmitParams{})
	j1, j2 := mustAcquire(t, s, w), mustAcquire(t, s, w)

	eventually(t, 2*time.Second, "lease renewal", func() bool {
		lost, err := s.Renew(ctx, w, []store.Lease{{JobID: id1, Token: j1.LeaseToken}})
		if err != nil || len(lost) != 0 {
			t.Fatalf("renew = %v, %v", lost, err)
		}
		cur, _ := mustGet(t, s, id1)
		return cur.LeaseExpiresAt > j1.LeaseExpiresAt
	})
	if cur, _ := mustGet(t, s, id2); cur.LeaseExpiresAt != j2.LeaseExpiresAt {
		t.Fatal("unreported lease was renewed")
	}

	lost, err := s.Renew(ctx, w, []store.Lease{{JobID: id2, Token: j2.LeaseToken + 99}, {JobID: "missing", Token: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(lost) != 2 || lost[0] != (store.Lease{JobID: id2, Token: j2.LeaseToken + 99}) || lost[1].JobID != "missing" {
		t.Fatalf("lost = %+v", lost)
	}
}

func testCompleteFencing(t *testing.T, f Factory) {
	s := newWith(t, f, nil)
	ctx := context.Background()
	a, b := mustRegister(t, s, "host-a"), mustRegister(t, s, "host-b")
	id := mustSubmit(t, s, store.SubmitParams{})
	j := mustAcquire(t, s, a)

	asStale(t, func() error { _, err := s.Complete(ctx, id, a, j.LeaseToken+1, []byte("x")); return err }())
	asStale(t, func() error { _, err := s.Complete(ctx, id, b, j.LeaseToken, []byte("x")); return err }())
	if _, err := s.Complete(ctx, "missing", a, 1, nil); !errors.Is(err, store.ErrNotFound) {
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
	if got.Status != store.StatusSucceeded || string(got.Result) != "first" || got.FinishedAt == 0 {
		t.Fatalf("job after complete = %+v", got)
	}
	if countKind(ev, "SUCCEEDED") != 1 || ev[len(ev)-1].Token != j.LeaseToken {
		t.Fatalf("history = %+v", ev)
	}
	if d := mustDepth(t, s); d.Leased != 0 || d.Pending != 0 {
		t.Fatalf("depth after complete = %+v", d)
	}
	if wk := workerByID(t, s, a); wk.ActiveJobs != 0 {
		t.Fatalf("completed job still held by worker: %+v", wk)
	}
	// A failure report on a completed lease is not a new transition.
	if out, err := s.Fail(ctx, id, a, j.LeaseToken, "late", true); err != nil || !out.Already || out.Status != store.StatusSucceeded {
		t.Fatalf("fail after complete = %+v, %v", out, err)
	}
}

// testZombieCompletionRejected walks the zombie scenario: A's lease expires
// and is reaped, B gets the job, and A's late result must not land, before
// or after B completes.
func testZombieCompletionRejected(t *testing.T, f Factory) {
	s := newWith(t, f, func(c *store.Config) { c.LeaseDuration = 50 * time.Millisecond })
	ctx := context.Background()
	a, b := mustRegister(t, s, "host-a"), mustRegister(t, s, "host-b")
	id := mustSubmit(t, s, store.SubmitParams{MaxAttempts: 3})
	ja := mustAcquire(t, s, a)

	eventually(t, 2*time.Second, "lease to be reaped", func() bool {
		r, err := s.ReapExpired(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return r.Requeued == 1
	})
	jb := mustAcquire(t, s, b)
	if jb.ID != id || jb.LeaseToken <= ja.LeaseToken || jb.Attempts != 2 {
		t.Fatalf("reassigned job = %+v (first token %d)", jb, ja.LeaseToken)
	}

	se := asStale(t, func() error { _, err := s.Complete(ctx, id, a, ja.LeaseToken, []byte("zombie")); return err }())
	if se.CurrentOwner != b || se.CurrentToken != jb.LeaseToken || se.Status != store.StatusLeased {
		t.Fatalf("stale error = %+v", se)
	}
	if _, err := s.Complete(ctx, id, b, jb.LeaseToken, []byte("b")); err != nil {
		t.Fatal(err)
	}
	asStale(t, func() error { _, err := s.Complete(ctx, id, a, ja.LeaseToken, []byte("zombie")); return err }())
	asStale(t, func() error { _, err := s.Fail(ctx, id, a, ja.LeaseToken, "zombie", true); return err }())

	got, ev := mustGet(t, s, id)
	if got.Status != store.StatusSucceeded || string(got.Result) != "b" || got.LeaseOwner != b {
		t.Fatalf("job = %+v", got)
	}
	want := []string{"SUBMITTED", "LEASED", "REQUEUED", "LEASED", "SUCCEEDED"}
	if fmt.Sprint(kinds(ev)) != fmt.Sprint(want) {
		t.Fatalf("history = %v, want %v", kinds(ev), want)
	}
}

func testFailRetryWithBackoff(t *testing.T, f Factory) {
	s := newWith(t, f, func(c *store.Config) { c.BackoffBase = 200 * time.Millisecond })
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")
	id := mustSubmit(t, s, store.SubmitParams{MaxAttempts: 3})

	var lastToken int64
	for attempt := 1; attempt <= 2; attempt++ {
		j := mustAcquire(t, s, w)
		if j.Attempts != attempt {
			t.Fatalf("attempts = %d, want %d", j.Attempts, attempt)
		}
		if j.LeaseToken <= lastToken {
			t.Fatalf("token %d did not increase past %d", j.LeaseToken, lastToken)
		}
		out, err := s.Fail(ctx, id, w, j.LeaseToken, fmt.Sprintf("boom %d", attempt), true)
		if err != nil {
			t.Fatal(err)
		}
		// Equal jitter: delay is in [d/2, d] for d = base * 2^(attempt-1).
		d := s.Config().BackoffBase << (attempt - 1)
		if out.Status != store.StatusPending || out.RetryIn < d/2 || out.RetryIn > d {
			t.Fatalf("attempt %d: outcome %+v, want retry in [%v, %v]", attempt, out, d/2, d)
		}
		failed, _ := mustGet(t, s, id)
		if failed.LastError != fmt.Sprintf("boom %d", attempt) || failed.Status != store.StatusPending {
			t.Fatalf("after fail: %+v", failed)
		}

		// Not eligible yet: acquire reports how long until it is.
		a, err := s.Acquire(ctx, w)
		if err != nil || a.Job != nil || a.NextEligibleIn <= 0 || a.NextEligibleIn > d {
			t.Fatalf("acquire during backoff = %+v, %v", a, err)
		}
		var next *store.Job
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
		lastToken = next.LeaseToken
	}
}

func testFailExhaustedGoesDead(t *testing.T, f Factory) {
	s := newWith(t, f, func(c *store.Config) { c.BackoffBase = time.Millisecond })
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")
	id := mustSubmit(t, s, store.SubmitParams{MaxAttempts: 2})

	j := mustAcquire(t, s, w)
	if out, _ := s.Fail(ctx, id, w, j.LeaseToken, "e1", true); out.Status != store.StatusPending {
		t.Fatalf("first failure: %+v", out)
	}
	var j2 *store.Job
	eventually(t, 2*time.Second, "retry", func() bool {
		a, _ := s.Acquire(ctx, w)
		j2 = a.Job
		return j2 != nil
	})
	out, err := s.Fail(ctx, id, w, j2.LeaseToken, "e2", true)
	if err != nil || out.Status != store.StatusDead {
		t.Fatalf("second failure = %+v, %v", out, err)
	}
	// A retried report of the same failure is recognized, not re-applied.
	if out, err := s.Fail(ctx, id, w, j2.LeaseToken, "e2", true); err != nil || !out.Already || out.Status != store.StatusDead {
		t.Fatalf("retried fail = %+v, %v", out, err)
	}
	got, ev := mustGet(t, s, id)
	if got.Status != store.StatusDead || got.Attempts != 2 || got.LastError != "e2" || got.FinishedAt == 0 {
		t.Fatalf("job = %+v", got)
	}
	want := []string{"SUBMITTED", "LEASED", "RETRY", "LEASED", "DEAD"}
	if fmt.Sprint(kinds(ev)) != fmt.Sprint(want) {
		t.Fatalf("history = %v, want %v", kinds(ev), want)
	}
	if d := mustDepth(t, s); d.Pending != 0 || d.Leased != 0 {
		t.Fatalf("depth = %+v", d)
	}
}

func testFailNonRetryable(t *testing.T, f Factory) {
	s := newWith(t, f, nil)
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")
	id := mustSubmit(t, s, store.SubmitParams{MaxAttempts: 5})
	j := mustAcquire(t, s, w)
	out, err := s.Fail(ctx, id, w, j.LeaseToken, "bad input", false)
	if err != nil || out.Status != store.StatusFailed {
		t.Fatalf("fail = %+v, %v", out, err)
	}
	got, ev := mustGet(t, s, id)
	if got.Status != store.StatusFailed || got.LastError != "bad input" || ev[len(ev)-1].Kind != "FAILED" {
		t.Fatalf("job = %+v %v", got, kinds(ev))
	}
	if d := mustDepth(t, s); d.Pending != 0 || d.Leased != 0 {
		t.Fatalf("non-retryable failure was requeued: %+v", d)
	}
}

func testReapExpiredLeases(t *testing.T, f Factory) {
	s := newWith(t, f, func(c *store.Config) { c.LeaseDuration = 50 * time.Millisecond; c.ScanBatch = 2 })
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")
	var ids []string
	for i := 0; i < 5; i++ {
		// The last job is on its final attempt, so expiry kills it.
		max := 2
		if i == 4 {
			max = 1
		}
		ids = append(ids, mustSubmit(t, s, store.SubmitParams{MaxAttempts: max}))
		mustAcquire(t, s, w)
	}

	var total store.RevokeResult
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
		if j.Status != store.StatusPending || j.Attempts != 1 || ev[len(ev)-1].Kind != "REQUEUED" {
			t.Fatalf("job %s = %+v %v", id, j, kinds(ev))
		}
	}
	if j, ev := mustGet(t, s, ids[4]); j.Status != store.StatusDead || ev[len(ev)-1].Kind != "DEAD" {
		t.Fatalf("exhausted job = %+v", j)
	}
	if d := mustDepth(t, s); d.Leased != 0 || d.Pending != 4 {
		t.Fatalf("depth after reap = %+v", d)
	}
	if wk := workerByID(t, s, w); wk.ActiveJobs != 0 {
		t.Fatalf("reaped leases still held: %+v", wk)
	}
}

func testExpireWorkers(t *testing.T, f Factory) {
	s := newWith(t, f, nil)
	ctx := context.Background()
	a, b := mustRegister(t, s, "host-a"), mustRegister(t, s, "host-b")
	mustSubmit(t, s, store.SubmitParams{})
	mustSubmit(t, s, store.SubmitParams{})
	mustSubmit(t, s, store.SubmitParams{})
	held := []*store.Job{mustAcquire(t, s, a), mustAcquire(t, s, a)}
	jb := mustAcquire(t, s, b)

	const deadAfter = 100 * time.Millisecond
	var expired []string
	eventually(t, 3*time.Second, "worker a to be declared dead", func() bool {
		// b keeps renewing; a has stopped.
		if _, err := s.Renew(ctx, b, []store.Lease{{JobID: jb.ID, Token: jb.LeaseToken}}); err != nil {
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
		if got.Status != store.StatusPending || ev[len(ev)-1].Kind != "REQUEUED" {
			t.Fatalf("held job after worker death = %+v %v", got, kinds(ev))
		}
	}
	if got, _ := mustGet(t, s, jb.ID); got.Status != store.StatusLeased || got.LeaseOwner != b {
		t.Fatalf("live worker's job disturbed: %+v", got)
	}
	if _, err := s.Acquire(ctx, a); !errors.Is(err, store.ErrWorkerNotAlive) {
		t.Fatalf("dead worker acquire = %v", err)
	}
	if _, err := s.Renew(ctx, a, nil); !errors.Is(err, store.ErrWorkerNotAlive) {
		t.Fatalf("dead worker renew = %v", err)
	}
	if wa, wb := workerByID(t, s, a), workerByID(t, s, b); wa.State != store.WorkerDead || wa.ActiveJobs != 0 ||
		wb.State != store.WorkerAlive || wb.ActiveJobs != 1 || wb.Hostname != "host-b" {
		t.Fatalf("workers = %+v %+v", wa, wb)
	}
}

func testReleaseUndoesAttempt(t *testing.T, f Factory) {
	s := newWith(t, f, nil)
	ctx := context.Background()
	w := mustRegister(t, s, "host-a")
	first := mustSubmit(t, s, store.SubmitParams{})
	mustSubmit(t, s, store.SubmitParams{})
	j := mustAcquire(t, s, w)

	if err := s.Release(ctx, first, w, j.LeaseToken+1); err == nil {
		t.Fatal("release with wrong token succeeded")
	}
	if err := s.Release(ctx, first, w, j.LeaseToken); err != nil {
		t.Fatal(err)
	}
	got, ev := mustGet(t, s, first)
	if got.Status != store.StatusPending || got.Attempts != 0 || ev[len(ev)-1].Kind != "RELEASED" {
		t.Fatalf("released job = %+v %v", got, kinds(ev))
	}
	if d := mustDepth(t, s); d.Leased != 0 || d.Pending != 2 {
		t.Fatalf("depth after release = %+v", d)
	}
	// It keeps its place at the head of the queue, under a new token.
	again := mustAcquire(t, s, w)
	if again.ID != first || again.LeaseToken <= j.LeaseToken || again.Attempts != 1 {
		t.Fatalf("re-acquired %+v", again)
	}
	asStale(t, func() error { _, err := s.Complete(ctx, first, w, j.LeaseToken, nil); return err }())
}
