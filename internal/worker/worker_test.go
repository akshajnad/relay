package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/coordinator"
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

type cluster struct {
	jobs    relayv1.JobServiceClient
	workers relayv1.WorkerServiceClient
	metrics *metrics.Registry
}

func testLogger(t *testing.T) *slog.Logger {
	if testing.Verbose() {
		return slog.New(slog.NewTextHandler(logWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type logWriter struct{ t *testing.T }

func (w logWriter) Write(p []byte) (int, error) { w.t.Log(string(p)); return len(p), nil }

// startCluster runs an in-process coordinator (with liveness loops) against
// the test Redis.
func startCluster(t *testing.T, mutCfg func(*coordinator.Config), mutStore func(*store.Config)) *cluster {
	t.Helper()
	scfg := store.DefaultConfig()
	scfg.BackoffBase = 20 * time.Millisecond
	if mutStore != nil {
		mutStore(&scfg)
	}
	st := redisstore.New(redistest.Client(t), scfg)
	if err := st.LoadScripts(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := coordinator.DefaultConfig()
	cfg.StartupGrace = 0
	cfg.HeartbeatInterval = 50 * time.Millisecond
	cfg.DeadAfter = 150 * time.Millisecond
	cfg.ReapInterval = 50 * time.Millisecond
	cfg.PollTimeout = time.Second
	if mutCfg != nil {
		mutCfg(&cfg)
	}
	m := metrics.NewRegistry()
	c := coordinator.New(st, cfg, m, testLogger(t).With("component", "coordinator"))

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	c.Register(srv)
	go srv.Serve(lis)
	ctx, cancel := context.WithCancel(context.Background())
	livenessDone := make(chan struct{})
	go func() { c.RunLiveness(ctx); close(livenessDone) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		<-livenessDone
		c.Shutdown()
		conn.Close()
		srv.Stop()
	})
	return &cluster{jobs: relayv1.NewJobServiceClient(conn), workers: relayv1.NewWorkerServiceClient(conn), metrics: m}
}

type runningWorker struct {
	*Worker
	stop func()
	done chan struct{}
}

func (c *cluster) startWorker(t *testing.T, client relayv1.WorkerServiceClient, handlers Registry, mut func(*Config)) *runningWorker {
	t.Helper()
	if client == nil {
		client = c.workers
	}
	cfg := DefaultConfig()
	cfg.Concurrency = 4
	cfg.Hostname = fmt.Sprintf("test-%d", time.Now().UnixNano())
	if mut != nil {
		mut(&cfg)
	}
	w := New(client, handlers, cfg, testLogger(t).With("component", "worker"))
	ctx, cancel := context.WithCancel(context.Background())
	rw := &runningWorker{Worker: w, stop: cancel, done: make(chan struct{})}
	go func() {
		if err := w.Run(ctx); err != nil && ctx.Err() == nil {
			t.Errorf("worker exited: %v", err)
		}
		close(rw.done)
	}()
	t.Cleanup(func() { cancel(); <-rw.done })
	eventually(t, 2*time.Second, "worker registration", func() bool { return w.ID() != "" })
	return rw
}

func (c *cluster) submit(t *testing.T, typ, payload string, maxAttempts int32) string {
	t.Helper()
	resp, err := c.jobs.SubmitJob(context.Background(), &relayv1.SubmitJobRequest{Type: typ, Payload: []byte(payload), MaxAttempts: maxAttempts})
	if err != nil {
		t.Fatal(err)
	}
	return resp.JobId
}

func (c *cluster) get(t *testing.T, id string) *relayv1.GetJobResponse {
	t.Helper()
	resp, err := c.jobs.GetJob(context.Background(), &relayv1.GetJobRequest{JobId: id})
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func (c *cluster) waitStatus(t *testing.T, id string, want relayv1.JobStatus, timeout time.Duration) *relayv1.GetJobResponse {
	t.Helper()
	var got *relayv1.GetJobResponse
	eventually(t, timeout, fmt.Sprintf("job %s to reach %v", id, want), func() bool {
		got = c.get(t, id)
		return got.Job.Status == want
	})
	return got
}

func countEvents(h []*relayv1.JobEvent, kind string) int {
	n := 0
	for _, e := range h {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

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

func TestWorkersRunJobsToCompletion(t *testing.T) {
	c := startCluster(t, nil, nil)
	for i := 0; i < 3; i++ {
		c.startWorker(t, nil, DefaultHandlers(time.Millisecond), nil)
	}
	var ids []string
	for i := 0; i < 60; i++ {
		typ, payload := "noop", ""
		if i%2 == 0 {
			typ, payload = "sleep", `{"duration_ms":5}`
		}
		ids = append(ids, c.submit(t, typ, payload, 3))
	}
	for _, id := range ids {
		got := c.waitStatus(t, id, relayv1.JobStatus_JOB_STATUS_SUCCEEDED, 10*time.Second)
		if n := countEvents(got.History, "SUCCEEDED"); n != 1 || got.Job.Attempts != 1 {
			t.Fatalf("job %s: %d SUCCEEDED events, %d attempts", id, n, got.Job.Attempts)
		}
		var res map[string]any
		if err := json.Unmarshal(got.Job.Result, &res); err != nil || res["worker_id"] != got.Job.LeaseOwner {
			t.Fatalf("result %s does not name lease owner %s", got.Job.Result, got.Job.LeaseOwner)
		}
	}
}

func TestFailNTimes(t *testing.T) {
	c := startCluster(t, nil, nil)
	c.startWorker(t, nil, DefaultHandlers(time.Millisecond), nil)

	ok := c.submit(t, "fail-n-times", `{"n":2}`, 5)
	got := c.waitStatus(t, ok, relayv1.JobStatus_JOB_STATUS_SUCCEEDED, 10*time.Second)
	if got.Job.Attempts != 3 || countEvents(got.History, "RETRY") != 2 {
		t.Fatalf("n=2,max=5: attempts %d, history %v", got.Job.Attempts, got.History)
	}

	dead := c.submit(t, "fail-n-times", `{"n":3}`, 3)
	got = c.waitStatus(t, dead, relayv1.JobStatus_JOB_STATUS_DEAD, 10*time.Second)
	if got.Job.Attempts != 3 || got.Job.LastError != "planned failure 3 of 3" {
		t.Fatalf("n=3,max=3: %v", got.Job)
	}

	bad := c.submit(t, "sleep", `{oops`, 5)
	got = c.waitStatus(t, bad, relayv1.JobStatus_JOB_STATUS_FAILED, 10*time.Second)
	if got.Job.Attempts != 1 {
		t.Fatalf("permanent failure retried: %v", got.Job)
	}
}

// blockingHandlers returns a "block" handler that waits for release.
func blockingHandlers(release <-chan struct{}, started *atomic.Int32, result string) Registry {
	r := DefaultHandlers(time.Millisecond)
	r["block"] = func(ctx context.Context, job Job) ([]byte, error) {
		started.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return []byte(result), nil
	}
	return r
}

func TestGracefulShutdownDrainsInFlight(t *testing.T) {
	c := startCluster(t, nil, nil)
	release := make(chan struct{})
	var started atomic.Int32
	w := c.startWorker(t, nil, blockingHandlers(release, &started, "done"), func(cfg *Config) { cfg.Concurrency = 2 })

	inflight := []string{c.submit(t, "block", "", 3), c.submit(t, "block", "", 3)}
	eventually(t, 2*time.Second, "both jobs running", func() bool { return started.Load() == 2 })
	queued := c.submit(t, "noop", "", 3)

	w.stop() // SIGTERM equivalent
	// Give the shutdown a moment to cancel the long-polls before releasing
	// the handlers; the queued job must not be picked up afterwards.
	eventually(t, 2*time.Second, "worker to stop acquiring", func() bool {
		return c.get(t, queued).Job.Status == relayv1.JobStatus_JOB_STATUS_PENDING && w.InFlight() == 2
	})
	close(release)
	select {
	case <-w.done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not exit after draining")
	}
	for _, id := range inflight {
		if got := c.get(t, id).Job; got.Status != relayv1.JobStatus_JOB_STATUS_SUCCEEDED || string(got.Result) != "done" {
			t.Fatalf("in-flight job not drained: %v", got)
		}
	}
	if got := c.get(t, queued).Job; got.Status != relayv1.JobStatus_JOB_STATUS_PENDING || got.Attempts != 0 {
		t.Fatalf("worker acquired after shutdown began: %v", got)
	}
}

func TestShutdownTimeoutAbandonsJobs(t *testing.T) {
	c := startCluster(t, nil, nil)
	var started atomic.Int32
	w := c.startWorker(t, nil, blockingHandlers(make(chan struct{}), &started, ""), func(cfg *Config) {
		cfg.Concurrency = 1
		cfg.ShutdownTimeout = 100 * time.Millisecond
	})
	id := c.submit(t, "block", "", 3)
	eventually(t, 2*time.Second, "job running", func() bool { return started.Load() == 1 })
	w.stop()
	select {
	case <-w.done:
	case <-time.After(3 * time.Second):
		t.Fatal("worker ignored its shutdown timeout")
	}
	// The abandoned job is not reported; its lease lapses and it is
	// requeued once the worker stops heartbeating.
	c.waitStatus(t, id, relayv1.JobStatus_JOB_STATUS_PENDING, 5*time.Second)
}

// pausableClient drops heartbeats while paused, standing in for a worker
// process that is frozen or partitioned from the coordinator.
type pausableClient struct {
	relayv1.WorkerServiceClient
	paused atomic.Bool
}

func (p *pausableClient) Heartbeat(ctx context.Context, in *relayv1.HeartbeatRequest, opts ...grpc.CallOption) (*relayv1.HeartbeatResponse, error) {
	if p.paused.Load() {
		return nil, status.Error(codes.Unavailable, "paused")
	}
	return p.WorkerServiceClient.Heartbeat(ctx, in, opts...)
}

// TestZombieResultRejected: worker A stalls mid-job and is declared dead,
// worker B runs the job to completion, then A wakes up and reports. A's
// result must be rejected and dropped, and A must rejoin under a new id.
func TestZombieResultRejected(t *testing.T) {
	c := startCluster(t, nil, nil)
	release := make(chan struct{})
	var aStarted, bStarted atomic.Int32
	pc := &pausableClient{WorkerServiceClient: c.workers}
	a := c.startWorker(t, pc, blockingHandlers(release, &aStarted, "from-a"), func(cfg *Config) { cfg.Concurrency = 1 })
	aID := a.ID()

	id := c.submit(t, "block", "", 3)
	eventually(t, 2*time.Second, "A running the job", func() bool { return aStarted.Load() == 1 })
	pc.paused.Store(true)

	bRelease := make(chan struct{})
	close(bRelease)
	c.startWorker(t, nil, blockingHandlers(bRelease, &bStarted, "from-b"), nil)
	got := c.waitStatus(t, id, relayv1.JobStatus_JOB_STATUS_SUCCEEDED, 5*time.Second)
	if string(got.Job.Result) != "from-b" || got.Job.LeaseOwner == aID {
		t.Fatalf("job = %v", got.Job)
	}
	finished := got.Job.FinishedAtMs

	pc.paused.Store(false)
	close(release) // A's handler returns and A reports its stale result
	eventually(t, 5*time.Second, "A's stale completion to be rejected", func() bool {
		return c.metrics.Snapshot().Counters.StaleRejections == 1
	})
	eventually(t, 5*time.Second, "A to re-register", func() bool { return a.ID() != aID })

	got = c.get(t, id)
	if string(got.Job.Result) != "from-b" || got.Job.FinishedAtMs != finished || countEvents(got.History, "SUCCEEDED") != 1 {
		t.Fatalf("zombie result leaked: %v %v", got.Job, got.History)
	}
}
