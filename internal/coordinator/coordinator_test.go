package coordinator

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/metrics"
	"github.com/akshajnad/relay/internal/redistest"
	"github.com/akshajnad/relay/internal/store"
	redisstore "github.com/akshajnad/relay/internal/store/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestMain(m *testing.M) { redistest.Main(m) }

type harness struct {
	c       *Coordinator
	jobs    relayv1.JobServiceClient
	workers relayv1.WorkerServiceClient
	metrics *metrics.Registry
}

func start(t *testing.T, mutCfg func(*Config), mutStore func(*store.Config)) *harness {
	t.Helper()
	rdb := redistest.Client(t)
	scfg := store.DefaultConfig()
	if mutStore != nil {
		mutStore(&scfg)
	}
	st := redisstore.New(rdb, scfg)
	if err := st.LoadScripts(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.StartupGrace = 0
	if mutCfg != nil {
		mutCfg(&cfg)
	}
	return serve(t, st, cfg)
}

// serve runs a coordinator on st behind an in-process gRPC server.
func serve(t *testing.T, st store.Store, cfg Config) *harness {
	t.Helper()
	m := metrics.NewRegistry()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if testing.Verbose() {
		log = slog.New(slog.NewTextHandler(testWriter{t}, nil))
	}
	c := New(st, cfg, m, log)

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	c.Register(srv)
	go srv.Serve(lis)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.Shutdown()
		conn.Close()
		srv.Stop()
	})
	return &harness{c: c, jobs: relayv1.NewJobServiceClient(conn), workers: relayv1.NewWorkerServiceClient(conn), metrics: m}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) { w.t.Log(string(p)); return len(p), nil }

func (h *harness) submit(t *testing.T, req *relayv1.SubmitJobRequest) string {
	t.Helper()
	if req.Type == "" {
		req.Type = "noop"
	}
	resp, err := h.jobs.SubmitJob(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return resp.JobId
}

func (h *harness) register(t *testing.T) string {
	t.Helper()
	resp, err := h.workers.RegisterWorker(context.Background(), &relayv1.RegisterWorkerRequest{Hostname: "test", Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	return resp.WorkerId
}

func (h *harness) acquire(t *testing.T, worker string) *relayv1.Job {
	t.Helper()
	resp, err := h.workers.AcquireJob(context.Background(), &relayv1.AcquireJobRequest{WorkerId: worker})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.HasJob {
		t.Fatal("AcquireJob timed out without a job")
	}
	return resp.Job
}

func (h *harness) get(t *testing.T, id string) *relayv1.GetJobResponse {
	t.Helper()
	resp, err := h.jobs.GetJob(context.Background(), &relayv1.GetJobRequest{JobId: id})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func wantCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("error = %v, want code %v", err, code)
	}
}

func TestJobLifecycle(t *testing.T) {
	h := start(t, nil, nil)
	ctx := context.Background()
	id := h.submit(t, &relayv1.SubmitJobRequest{Type: "sleep", Payload: []byte(`{"duration_ms":1}`), MaxAttempts: 2})
	if got := h.get(t, id); got.Job.Status != relayv1.JobStatus_JOB_STATUS_PENDING || got.Job.MaxAttempts != 2 {
		t.Fatalf("submitted job = %v", got.Job)
	}

	w := h.register(t)
	j := h.acquire(t, w)
	if j.Id != id || j.LeaseOwner != w || j.LeaseToken != 1 || j.Attempts != 1 || string(j.Payload) != `{"duration_ms":1}` {
		t.Fatalf("acquired %v", j)
	}
	resp, err := h.workers.CompleteJob(ctx, &relayv1.CompleteJobRequest{JobId: id, WorkerId: w, LeaseToken: j.LeaseToken, Result: []byte("ok")})
	if err != nil || resp.AlreadyCompleted {
		t.Fatalf("complete = %v, %v", resp, err)
	}
	resp, err = h.workers.CompleteJob(ctx, &relayv1.CompleteJobRequest{JobId: id, WorkerId: w, LeaseToken: j.LeaseToken, Result: []byte("again")})
	if err != nil || !resp.AlreadyCompleted {
		t.Fatalf("retried complete = %v, %v", resp, err)
	}

	got := h.get(t, id)
	if got.Job.Status != relayv1.JobStatus_JOB_STATUS_SUCCEEDED || string(got.Job.Result) != "ok" {
		t.Fatalf("finished job = %v", got.Job)
	}
	var kinds []string
	for _, e := range got.History {
		kinds = append(kinds, e.Kind)
	}
	if len(kinds) != 3 || kinds[0] != "SUBMITTED" || kinds[1] != "LEASED" || kinds[2] != "SUCCEEDED" {
		t.Fatalf("history = %v", kinds)
	}
	snap := h.metrics.Snapshot()
	if snap.Counters.Submitted != 1 || snap.Counters.Leased != 1 || snap.Counters.Succeeded != 1 || snap.SchedulingLatency.Count != 1 {
		t.Fatalf("metrics = %+v", snap)
	}

	_, err = h.jobs.GetJob(ctx, &relayv1.GetJobRequest{JobId: "nope"})
	wantCode(t, err, codes.NotFound)
}

func TestSubmitValidationAndDuplicates(t *testing.T) {
	h := start(t, func(c *Config) { c.MaxPayloadBytes = 8 }, nil)
	ctx := context.Background()
	_, err := h.jobs.SubmitJob(ctx, &relayv1.SubmitJobRequest{})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.jobs.SubmitJob(ctx, &relayv1.SubmitJobRequest{Type: "noop", Payload: make([]byte, 9)})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.jobs.SubmitJob(ctx, &relayv1.SubmitJobRequest{Type: "noop", MaxAttempts: -1})
	wantCode(t, err, codes.InvalidArgument)

	first, err := h.jobs.SubmitJob(ctx, &relayv1.SubmitJobRequest{Type: "noop", IdempotencyKey: "k"})
	if err != nil || first.Duplicate {
		t.Fatalf("first submit = %v, %v", first, err)
	}
	second, err := h.jobs.SubmitJob(ctx, &relayv1.SubmitJobRequest{Type: "noop", IdempotencyKey: "k"})
	if err != nil || !second.Duplicate || second.JobId != first.JobId {
		t.Fatalf("duplicate submit = %v, %v", second, err)
	}
}

func TestAcquireLongPollWakesOnSubmit(t *testing.T) {
	h := start(t, nil, nil)
	w := h.register(t)

	type result struct {
		job *relayv1.Job
		at  time.Time
		err error
	}
	done := make(chan result, 1)
	go func() {
		resp, err := h.workers.AcquireJob(context.Background(), &relayv1.AcquireJobRequest{WorkerId: w})
		done <- result{resp.GetJob(), time.Now(), err}
	}()
	eventually(t, 2*time.Second, "AcquireJob to start waiting", func() bool { return h.c.notifier.Waiting() == 1 })

	submitted := time.Now()
	id := h.submit(t, &relayv1.SubmitJobRequest{})
	select {
	case r := <-done:
		if r.err != nil || r.job.GetId() != id {
			t.Fatalf("long-poll returned %v, %v", r.job, r.err)
		}
		// The poll timeout is 5s; being woken by the submit takes ~1ms.
		// The bound is loose so that a loaded CI machine does not flake.
		if wake := r.at.Sub(submitted); wake > time.Second {
			t.Fatalf("long-poll took %v to wake after submit", wake)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("long-poll was not woken by submit")
	}
	if s := h.metrics.SchedulingLatency.Summary(); s.Count != 1 || s.MaxMs > 1000 {
		t.Fatalf("scheduling latency = %+v", s)
	}
}

func TestAcquireTimesOutEmpty(t *testing.T) {
	h := start(t, func(c *Config) { c.PollTimeout = 50 * time.Millisecond }, nil)
	w := h.register(t)
	resp, err := h.workers.AcquireJob(context.Background(), &relayv1.AcquireJobRequest{WorkerId: w})
	if err != nil || resp.HasJob {
		t.Fatalf("empty poll = %v, %v", resp, err)
	}
	if h.c.notifier.Waiting() != 0 {
		t.Fatal("timed-out poll left a waiter behind")
	}
}

func TestAcquireUnknownWorker(t *testing.T) {
	h := start(t, nil, nil)
	_, err := h.workers.AcquireJob(context.Background(), &relayv1.AcquireJobRequest{WorkerId: "w-ghost"})
	wantCode(t, err, codes.FailedPrecondition)
	_, err = h.workers.Heartbeat(context.Background(), &relayv1.HeartbeatRequest{WorkerId: "w-ghost"})
	wantCode(t, err, codes.FailedPrecondition)
}

// Twenty pollers wait, five jobs arrive: all five are dispatched promptly
// (no lost wake-ups) and the rest time out.
func TestManyWaitersFewJobs(t *testing.T) {
	h := start(t, func(c *Config) { c.PollTimeout = 3 * time.Second }, nil)
	const pollers, jobs = 20, 5
	ids := make([]string, pollers)
	for i := range ids {
		ids[i] = h.register(t)
	}
	var mu sync.Mutex
	var got []string
	var wg sync.WaitGroup
	for _, w := range ids {
		wg.Add(1)
		go func(w string) {
			defer wg.Done()
			resp, err := h.workers.AcquireJob(context.Background(), &relayv1.AcquireJobRequest{WorkerId: w})
			if err != nil {
				t.Error(err)
				return
			}
			if resp.HasJob {
				mu.Lock()
				got = append(got, resp.Job.Id)
				mu.Unlock()
			}
		}(w)
	}
	eventually(t, 2*time.Second, "pollers to wait", func() bool { return h.c.notifier.Waiting() == pollers })
	for i := 0; i < jobs; i++ {
		h.submit(t, &relayv1.SubmitJobRequest{})
	}
	eventually(t, time.Second, "all jobs dispatched", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == jobs
	})
	wg.Wait()
	seen := map[string]bool{}
	for _, id := range got {
		if seen[id] {
			t.Fatalf("job %s dispatched twice", id)
		}
		seen[id] = true
	}
	if len(got) != jobs {
		t.Fatalf("dispatched %d jobs, want %d", len(got), jobs)
	}
}

// A retry scheduled with backoff wakes a waiting poller when it becomes
// eligible, rather than waiting out the poll timeout.
func TestRetryWakesPollerAfterBackoff(t *testing.T) {
	h := start(t, func(c *Config) { c.PollTimeout = 4 * time.Second },
		func(s *store.Config) { s.BackoffBase = 100 * time.Millisecond })
	ctx := context.Background()
	w := h.register(t)
	id := h.submit(t, &relayv1.SubmitJobRequest{MaxAttempts: 3})
	j := h.acquire(t, w)
	out, err := h.workers.FailJob(ctx, &relayv1.FailJobRequest{JobId: id, WorkerId: w, LeaseToken: j.LeaseToken, Error: "transient", Retryable: true})
	if err != nil || out.Status != relayv1.JobStatus_JOB_STATUS_PENDING || out.RetryInMs < 50 || out.RetryInMs > 100 {
		t.Fatalf("fail = %v, %v", out, err)
	}
	start := time.Now()
	j2 := h.acquire(t, w)
	if j2.Id != id || j2.Attempts != 2 {
		t.Fatalf("retry = %v", j2)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("retry took %v; SignalAfter did not wake the poller", waited)
	}
	if h.metrics.Snapshot().Counters.Retried != 1 {
		t.Fatal("retry not counted")
	}
}

func TestStaleCompletionAborted(t *testing.T) {
	h := start(t, nil, func(s *store.Config) { s.LeaseDuration = 50 * time.Millisecond })
	ctx := context.Background()
	a, b := h.register(t), h.register(t)
	id := h.submit(t, &relayv1.SubmitJobRequest{})
	ja := h.acquire(t, a)

	eventually(t, 2*time.Second, "expired lease reaped", func() bool {
		h.c.reapExpiredLeases(ctx)
		return h.get(t, id).Job.Status == relayv1.JobStatus_JOB_STATUS_PENDING
	})
	jb := h.acquire(t, b)
	if jb.LeaseToken != ja.LeaseToken+1 {
		t.Fatalf("token did not advance: %d -> %d", ja.LeaseToken, jb.LeaseToken)
	}
	_, err := h.workers.CompleteJob(ctx, &relayv1.CompleteJobRequest{JobId: id, WorkerId: a, LeaseToken: ja.LeaseToken, Result: []byte("zombie")})
	wantCode(t, err, codes.Aborted)
	_, err = h.workers.FailJob(ctx, &relayv1.FailJobRequest{JobId: id, WorkerId: a, LeaseToken: ja.LeaseToken, Retryable: true})
	wantCode(t, err, codes.Aborted)
	if _, err := h.workers.CompleteJob(ctx, &relayv1.CompleteJobRequest{JobId: id, WorkerId: b, LeaseToken: jb.LeaseToken, Result: []byte("b")}); err != nil {
		t.Fatal(err)
	}
	if got := h.get(t, id).Job; string(got.Result) != "b" || got.LeaseOwner != b {
		t.Fatalf("job = %v", got)
	}
	if n := h.metrics.Snapshot().Counters.StaleRejections; n != 2 {
		t.Fatalf("stale rejections = %d, want 2", n)
	}
}

func TestShutdownReleasesPollers(t *testing.T) {
	h := start(t, nil, nil)
	w := h.register(t)
	errc := make(chan error, 1)
	go func() {
		_, err := h.workers.AcquireJob(context.Background(), &relayv1.AcquireJobRequest{WorkerId: w})
		errc <- err
	}()
	eventually(t, 2*time.Second, "poller waiting", func() bool { return h.c.notifier.Waiting() == 1 })
	h.c.Shutdown()
	select {
	case err := <-errc:
		wantCode(t, err, codes.Unavailable)
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not release the long-poll")
	}
}
