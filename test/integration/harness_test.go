//go:build integration

// Package integration runs Relay's failure scenarios against a real
// multi-container cluster (Redis + coordinator + workers) driven through
// docker compose. Run with `make integration`.
package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/rpc"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var composeFile = func() string {
	_, here, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(here), "..", "..", "deploy", "docker-compose.yml")
}()

// Cluster is one compose project, torn down when the test ends.
type Cluster struct {
	t       *testing.T
	project string
	env     []string

	GRPCAddr    string
	MetricsAddr string
	conn        *grpc.ClientConn
	Jobs        relayv1.JobServiceClient
	Workers     relayv1.WorkerServiceClient
	Redis       *redis.Client
}

type ClusterOpts struct {
	Workers     int
	Concurrency int
	Env         map[string]string
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

var nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

// NewCluster starts a cluster and waits until the coordinator is healthy and
// every worker has registered. Host ports are fixed for the cluster's life
// so clients reconnect transparently across a coordinator restart.
func NewCluster(t *testing.T, opts ClusterOpts) *Cluster {
	t.Helper()
	if opts.Workers == 0 {
		opts.Workers = 3
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = 4
	}
	grpcPort, metricsPort, redisPort := freePort(t), freePort(t), freePort(t)
	c := &Cluster{
		t:           t,
		project:     "relayit-" + strings.Trim(nonAlnum.ReplaceAllString(strings.ToLower(t.Name()), "-"), "-"),
		GRPCAddr:    fmt.Sprintf("127.0.0.1:%d", grpcPort),
		MetricsAddr: fmt.Sprintf("127.0.0.1:%d", metricsPort),
	}
	c.env = append(os.Environ(),
		fmt.Sprintf("RELAY_GRPC_PUBLISH=127.0.0.1:%d:50051", grpcPort),
		fmt.Sprintf("RELAY_METRICS_PUBLISH=127.0.0.1:%d:9090", metricsPort),
		fmt.Sprintf("RELAY_REDIS_PUBLISH=127.0.0.1:%d:6379", redisPort),
		fmt.Sprintf("RELAY_WORKER_CONCURRENCY=%d", opts.Concurrency),
	)
	for k, v := range opts.Env {
		c.env = append(c.env, k+"="+v)
	}

	c.compose("down", "-v", "--remove-orphans") // leftovers from an aborted run
	t.Cleanup(c.teardown)
	c.mustCompose("up", "-d", "--wait", "--no-build", "--scale", fmt.Sprintf("worker=%d", opts.Workers))

	conn, err := rpc.Dial(c.GRPCAddr)
	if err != nil {
		t.Fatal(err)
	}
	c.conn = conn
	c.Jobs = relayv1.NewJobServiceClient(conn)
	c.Workers = relayv1.NewWorkerServiceClient(conn)
	c.Redis = redis.NewClient(&redis.Options{Addr: fmt.Sprintf("127.0.0.1:%d", redisPort)})
	c.WaitAliveWorkers(opts.Workers)
	return c
}

func (c *Cluster) teardown() {
	if c.t.Failed() {
		out, _ := c.compose("logs", "--no-color", "--tail", "200")
		c.t.Logf("compose logs (last 200 lines per service):\n%s", out)
	}
	if c.conn != nil {
		c.conn.Close()
	}
	if c.Redis != nil {
		c.Redis.Close()
	}
	if out, err := c.compose("down", "-v", "--remove-orphans", "--timeout", "5"); err != nil {
		c.t.Logf("compose down: %v\n%s", err, out)
	}
}

func (c *Cluster) compose(args ...string) (string, error) {
	full := append([]string{"compose", "-p", c.project, "-f", composeFile}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Env = c.env
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

func (c *Cluster) mustCompose(args ...string) string {
	c.t.Helper()
	out, err := c.compose(args...)
	if err != nil {
		c.t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func docker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

// Eventually polls cond every 100ms until it holds or timeout passes.
func Eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *Cluster) aliveWorkers() []*relayv1.Worker {
	resp, err := c.Workers.ListWorkers(ctx(c.t), &relayv1.ListWorkersRequest{})
	if err != nil {
		return nil
	}
	var alive []*relayv1.Worker
	for _, w := range resp.Workers {
		if w.State == "ALIVE" {
			alive = append(alive, w)
		}
	}
	return alive
}

func (c *Cluster) WaitAliveWorkers(n int) {
	c.t.Helper()
	Eventually(c.t, 60*time.Second, fmt.Sprintf("%d live workers", n), func() bool { return len(c.aliveWorkers()) == n })
}

// Scale changes the number of worker containers without touching the rest.
func (c *Cluster) Scale(workers int) {
	c.t.Helper()
	c.mustCompose("up", "-d", "--wait", "--no-build", "--no-recreate", "--scale", fmt.Sprintf("worker=%d", workers))
}

// ContainerOf maps a worker id to its container. Workers report their
// hostname, which Docker sets to the container's short id.
func (c *Cluster) ContainerOf(workerID string) string {
	c.t.Helper()
	resp, err := c.Workers.ListWorkers(ctx(c.t), &relayv1.ListWorkersRequest{})
	if err != nil {
		c.t.Fatal(err)
	}
	var host string
	for _, w := range resp.Workers {
		if w.Id == workerID {
			host = w.Hostname
		}
	}
	if host == "" {
		c.t.Fatalf("worker %s not registered", workerID)
	}
	for _, id := range strings.Fields(c.mustCompose("ps", "-a", "-q", "worker")) {
		if strings.HasPrefix(id, host) {
			return id
		}
	}
	c.t.Fatalf("no container with hostname %s", host)
	return ""
}

func (c *Cluster) Get(id string) *relayv1.GetJobResponse {
	c.t.Helper()
	resp, err := c.Jobs.GetJob(ctx(c.t), &relayv1.GetJobRequest{JobId: id})
	if err != nil {
		c.t.Fatalf("GetJob %s: %v", id, err)
	}
	return resp
}

// TryGet is Get that tolerates transient errors (e.g. during a restart).
func (c *Cluster) TryGet(id string) (*relayv1.GetJobResponse, bool) {
	resp, err := c.Jobs.GetJob(ctx(c.t), &relayv1.GetJobRequest{JobId: id})
	return resp, err == nil
}

func (c *Cluster) Submit(req *relayv1.SubmitJobRequest) *relayv1.SubmitJobResponse {
	c.t.Helper()
	resp, err := c.Jobs.SubmitJob(ctx(c.t), req)
	if err != nil {
		c.t.Fatalf("SubmitJob: %v", err)
	}
	return resp
}

// SubmitRetrying retries through transient failures, reusing the request's
// idempotency key, which is exactly what makes retrying safe.
func (c *Cluster) SubmitRetrying(req *relayv1.SubmitJobRequest, onError func(error)) *relayv1.SubmitJobResponse {
	deadline := time.Now().Add(60 * time.Second)
	for {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := c.Jobs.SubmitJob(cctx, req)
		cancel()
		if err == nil {
			return resp
		}
		if onError != nil {
			onError(err)
		}
		if code := status.Code(err); code != codes.Unavailable && code != codes.DeadlineExceeded {
			c.t.Errorf("SubmitJob: non-transient error %v", err)
			return nil
		}
		if time.Now().After(deadline) {
			c.t.Errorf("SubmitJob: still failing after 60s: %v", err)
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func isTerminal(s relayv1.JobStatus) bool {
	return s == relayv1.JobStatus_JOB_STATUS_SUCCEEDED || s == relayv1.JobStatus_JOB_STATUS_FAILED ||
		s == relayv1.JobStatus_JOB_STATUS_DEAD
}

// WaitTerminal polls until every job is in a terminal state and returns
// their final GetJob responses.
func (c *Cluster) WaitTerminal(ids []string, timeout time.Duration) map[string]*relayv1.GetJobResponse {
	c.t.Helper()
	final := make(map[string]*relayv1.GetJobResponse, len(ids))
	var mu sync.Mutex
	Eventually(c.t, timeout, fmt.Sprintf("%d jobs to finish", len(ids)), func() bool {
		var pending []string
		mu.Lock()
		for _, id := range ids {
			if _, ok := final[id]; !ok {
				pending = append(pending, id)
			}
		}
		mu.Unlock()
		sem := make(chan struct{}, 32)
		var wg sync.WaitGroup
		for _, id := range pending {
			wg.Add(1)
			sem <- struct{}{}
			go func(id string) {
				defer wg.Done()
				defer func() { <-sem }()
				if resp, ok := c.TryGet(id); ok && isTerminal(resp.Job.Status) {
					mu.Lock()
					final[id] = resp
					mu.Unlock()
				}
			}(id)
		}
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		return len(final) == len(ids)
	})
	return final
}

// CountJobs counts job hashes directly in Redis.
func (c *Cluster) CountJobs() int {
	c.t.Helper()
	n := 0
	iter := c.Redis.Scan(ctx(c.t), 0, "relay:job:*", 1000).Iterator()
	for iter.Next(ctx(c.t)) {
		if !strings.HasSuffix(iter.Val(), ":events") {
			n++
		}
	}
	if err := iter.Err(); err != nil {
		c.t.Fatal(err)
	}
	return n
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

// terminalEvents counts transitions into a terminal state.
func terminalEvents(h []*relayv1.JobEvent) int {
	return countEvents(h, "SUCCEEDED") + countEvents(h, "FAILED") + countEvents(h, "DEAD")
}

func kinds(h []*relayv1.JobEvent) string {
	var out []string
	for _, e := range h {
		out = append(out, e.Kind)
	}
	return strings.Join(out, " → ")
}
