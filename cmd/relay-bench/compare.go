package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/history"
	"github.com/akshajnad/relay/internal/metrics"
	"github.com/akshajnad/relay/internal/rpc"
)

// benchBackend describes one system under test.
type benchBackend struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	profile     string
	client      string // relay or asynq
	workerSvc   string
	// workerTarget is what Relay workers dial inside the cluster.
	workerTarget string
	// driverTarget is what the in-cluster driver dials.
	driverTarget string
	coords       []string
	nodeIDs      []string
	raft         bool
	// asynqCheckInterval is asynq's TaskCheckInterval ("0s" = default 1s).
	asynqCheckInterval string
	phases             []string
	// Timings records the failure-detection settings in effect.
	Timings map[string]string `json:"timings"`
}

var relayTimings = map[string]string{
	"lease_duration":     "10s",
	"heartbeat_interval": "1s",
	"missed_heartbeats":  "3 (worker declared dead after 3s)",
	"poll_timeout":       "5s",
}

var benchBackends = map[string]benchBackend{
	"relay-redis": {
		Name: "relay-redis", Description: "Relay, Redis store, 1 coordinator",
		profile: "redis", client: "relay", workerSvc: "worker",
		workerTarget: "coordinator:50051", driverTarget: "coordinator:50051",
		coords: []string{"coordinator"},
		phases: []string{"saturate", "latency", "recovery", "faults"}, Timings: relayTimings,
	},
	"relay-raft": {
		Name: "relay-raft", Description: "Relay, Raft store, 3 coordinators",
		profile: "raft", client: "relay", workerSvc: "worker",
		workerTarget: "n1=coord1:50051,n2=coord2:50051,n3=coord3:50051",
		driverTarget: "n1=coord1:50051,n2=coord2:50051,n3=coord3:50051",
		coords:       []string{"coord1", "coord2", "coord3"}, nodeIDs: []string{"n1", "n2", "n3"}, raft: true,
		phases: []string{"saturate", "latency", "recovery", "failover", "faults"},
		Timings: merge(relayTimings, map[string]string{
			"raft_heartbeat_timeout": "1s", "raft_election_timeout": "1s", "raft_leader_lease_timeout": "500ms",
			"raft_storage": "raft-boltdb (bbolt), fsync per batch",
		}),
	},
	"asynq": {
		Name: "asynq", Description: "hibiken/asynq v0.26.0, Redis, default config",
		profile: "asynq", client: "asynq", workerSvc: "asynq-worker", driverTarget: "redis:6379",
		asynqCheckInterval: "0s",
		phases:             []string{"saturate", "latency", "recovery", "faults"},
		Timings: map[string]string{
			"lease_duration":      "30s (asynq constant, extended by heartbeats)",
			"recoverer_interval":  "1m (asynq constant)",
			"task_check_interval": "1s (default)",
		},
	},
	"asynq-tuned": {
		Name: "asynq-tuned", Description: "hibiken/asynq v0.26.0, Redis, TaskCheckInterval=10ms",
		profile: "asynq", client: "asynq", workerSvc: "asynq-worker", driverTarget: "redis:6379",
		asynqCheckInterval: "10ms",
		phases:             []string{"saturate", "latency"},
		Timings: map[string]string{
			"lease_duration":      "30s (asynq constant)",
			"recoverer_interval":  "1m (asynq constant)",
			"task_check_interval": "10ms",
		},
	},
}

func merge(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

type compareConfig struct {
	Backends       []string `json:"backends"`
	Phases         []string `json:"phases"`
	Image          string   `json:"image"`
	OutDir         string   `json:"-"`
	ComposeFile    string   `json:"compose_file"`
	SatJobs        int      `json:"saturation_jobs"`
	SatWorkers     int      `json:"saturation_workers"`
	SatConcurrency int      `json:"saturation_concurrency"`
	LatRate        float64  `json:"latency_rate_per_sec"`
	LatJobs        int      `json:"latency_jobs"`
	LatSleepMs     int      `json:"latency_sleep_ms"`
	Trials         int      `json:"trials"`
	TrialWorkers   int      `json:"trial_workers"`
	TrialSlots     int      `json:"trial_concurrency"`
	FaultDuration  string   `json:"fault_duration"`
	FaultRate      float64  `json:"fault_submit_rate"`
	Seed           uint64   `json:"seed"`
}

// CompareReport is the results file.
type CompareReport struct {
	StartedAt  time.Time        `json:"started_at"`
	FinishedAt time.Time        `json:"finished_at"`
	Host       HostInfo         `json:"host"`
	Docker     DockerInfo       `json:"docker"`
	Config     compareConfig    `json:"config"`
	Results    []*BackendResult `json:"results"`
}

type BackendResult struct {
	Backend    benchBackend      `json:"backend"`
	Saturation *SaturationResult `json:"saturation,omitempty"`
	Latency    *LatencyResult    `json:"latency,omitempty"`
	Recovery   *TrialsResult     `json:"worker_recovery,omitempty"`
	Failover   *TrialsResult     `json:"leader_failover,omitempty"`
	Faults     *FaultsResult     `json:"faults,omitempty"`
	Errors     []string          `json:"errors,omitempty"`
}

type SaturationResult struct {
	Jobs          int     `json:"jobs"`
	Workers       int     `json:"workers"`
	Slots         int     `json:"slots"`
	DurationSec   float64 `json:"duration_sec"`
	JobsPerSec    float64 `json:"jobs_per_sec"`
	CompletedJobs int     `json:"completed_jobs"`
}

type LatencyResult struct {
	Rate    float64 `json:"rate_per_sec"`
	Jobs    int     `json:"jobs"`
	SleepMs int     `json:"sleep_ms"`
	// Dispatch is the externally observed dispatch latency (same for every
	// backend): handler start - max(submit invoked, worker slot free).
	Dispatch Dist `json:"dispatch_external"`
	// QueueWait is end to end: handler start - submit invoked.
	QueueWait Dist `json:"queue_wait_end_to_end"`
	// SubmitRTT is the client's enqueue round trip.
	SubmitRTT Dist `json:"submit_rtt"`
	// Internal is Relay's coordinator-measured dispatch latency (the
	// project's original definition); absent for asynq.
	Internal          *metrics.Summary `json:"dispatch_internal,omitempty"`
	InternalQueueWait *metrics.Summary `json:"queue_wait_internal,omitempty"`
}

type TrialsResult struct {
	Trials   int       `json:"trials"`
	Seconds  []float64 `json:"seconds"`
	Dist     Dist      `json:"dist"`
	Failures []string  `json:"failures,omitempty"`
	Method   string    `json:"method"`
}

type FaultsResult struct {
	DurationSec         float64  `json:"duration_sec"`
	Faults              []string `json:"faults"`
	AckedJobs           int      `json:"acked_jobs"`
	Lost                int      `json:"lost_jobs"`
	Unfinished          int      `json:"unfinished_jobs"`
	DuplicateExecutions int      `json:"duplicate_executions"`
	// DuplicateTerminal is -1 where the backend does not record transitions.
	DuplicateTerminal int `json:"duplicate_terminal_transitions"`
	Succeeded         int `json:"succeeded"`
	Dead              int `json:"dead"`
}

func compareMain(args []string) error {
	var cfg compareConfig
	var backends, phases string
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	fs.StringVar(&backends, "backends", "relay-redis,relay-raft,asynq,asynq-tuned", "backends to benchmark")
	fs.StringVar(&phases, "phases", "saturate,latency,recovery,failover,faults", "phases to run (each backend runs those that apply)")
	fs.StringVar(&cfg.Image, "image", "relay:dev", "image with every binary")
	fs.StringVar(&cfg.OutDir, "out", "bench/results", "results directory")
	fs.StringVar(&cfg.ComposeFile, "compose", "deploy/docker-compose.bench.yml", "benchmark compose file")
	fs.IntVar(&cfg.SatJobs, "sat-jobs", 20000, "saturation: noop jobs preloaded")
	fs.IntVar(&cfg.SatWorkers, "workers", 10, "saturation and latency: worker containers")
	fs.IntVar(&cfg.SatConcurrency, "concurrency", 10, "saturation and latency: slots per worker")
	fs.Float64Var(&cfg.LatRate, "lat-rate", 500, "latency: submits per second")
	fs.IntVar(&cfg.LatJobs, "lat-jobs", 30000, "latency: jobs")
	fs.IntVar(&cfg.LatSleepMs, "lat-sleep-ms", 20, "latency: sleep job duration")
	fs.IntVar(&cfg.Trials, "trials", 30, "recovery and failover trials")
	fs.IntVar(&cfg.TrialWorkers, "trial-workers", 4, "recovery, failover and faults: worker containers")
	fs.IntVar(&cfg.TrialSlots, "trial-concurrency", 2, "recovery and failover: slots per worker")
	fs.StringVar(&cfg.FaultDuration, "fault-duration", "2m", "faults: how long to inject faults")
	fs.Float64Var(&cfg.FaultRate, "fault-rate", 20, "faults: submits per second")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "fault schedule seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg.Backends = splitList(backends)
	cfg.Phases = splitList(phases)

	rep := &CompareReport{StartedAt: time.Now().UTC(), Host: hostInfo(), Docker: dockerInfo(), Config: cfg}
	stamp := rep.StartedAt.Format("20060102T150405Z")
	runDir, err := filepath.Abs(filepath.Join(cfg.OutDir, "compare-"+stamp))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return err
	}
	resultsPath := filepath.Join(cfg.OutDir, "compare-"+stamp+".json")
	save := func() {
		b, _ := json.MarshalIndent(rep, "", "  ")
		_ = os.WriteFile(resultsPath, b, 0o644)
	}
	for _, name := range cfg.Backends {
		be, ok := benchBackends[name]
		if !ok {
			return fmt.Errorf("unknown backend %q", name)
		}
		res := &BackendResult{Backend: be}
		rep.Results = append(rep.Results, res)
		r := &comparer{cfg: cfg, be: be, res: res, dir: filepath.Join(runDir, name)}
		fmt.Printf("\n=== %s: %s ===\n", be.Name, be.Description)
		if err := r.run(); err != nil {
			res.Errors = append(res.Errors, err.Error())
			fmt.Println("  ERROR:", err)
		}
		save()
	}
	rep.FinishedAt = time.Now().UTC()
	save()
	fmt.Printf("\nresults: %s\n", resultsPath)
	if err := plotReport(rep, runDir); err != nil {
		return fmt.Errorf("charts: %w", err)
	}
	fmt.Printf("charts:  %s\n", runDir)
	printCompare(rep)
	return nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// comparer runs every phase for one backend.
type comparer struct {
	cfg     compareConfig
	be      benchBackend
	res     *BackendResult
	dir     string
	project string
	// current worker settings
	histDir     string
	concurrency int
	rpcHistory  bool
	host        jobClient
}

func (r *comparer) env() []string {
	return append(os.Environ(),
		"BENCH_IMAGE="+r.cfg.Image,
		"BENCH_HISTORY_DIR="+r.histDir,
		"BENCH_RELAY_TARGET="+r.be.workerTarget,
		fmt.Sprintf("BENCH_CONCURRENCY=%d", r.concurrency),
		fmt.Sprintf("BENCH_HISTORY_RPCS=%v", r.rpcHistory),
		"BENCH_ASYNQ_TASK_CHECK_INTERVAL="+r.be.asynqCheckInterval,
	)
}

func (r *comparer) compose(args ...string) (string, error) {
	full := append([]string{"compose", "-p", r.project, "-f", r.cfg.ComposeFile, "--profile", r.be.profile}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Env = r.env()
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("docker %s: %w\n%s", strings.Join(full, " "), err, out.String())
	}
	return out.String(), nil
}

func docker(args ...string) (string, error) {
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out)), nil
}

func (r *comparer) hostPort(service string, port int) (string, error) {
	out, err := r.compose("port", service, fmt.Sprint(port))
	return strings.TrimSpace(out), err
}

func (r *comparer) containers(service string) ([]string, error) {
	out, err := docker("ps", "-a", "--filter", "label=com.docker.compose.project="+r.project,
		"--filter", "label=com.docker.compose.service="+service, "--format", "{{.ID}}")
	if err != nil {
		return nil, err
	}
	ids := strings.Fields(out)
	sort.Strings(ids)
	return ids, nil
}

func (r *comparer) run() error {
	r.project = "relaybench-" + r.be.Name
	r.histDir = filepath.Join(r.dir, "idle")
	r.concurrency = 1
	if err := os.MkdirAll(r.histDir, 0o777); err != nil {
		return err
	}
	_, _ = r.compose("down", "-v", "--remove-orphans", "--timeout", "1")
	defer func() { _, _ = r.compose("down", "-v", "--remove-orphans", "--timeout", "5") }()
	if _, err := r.compose("up", "-d", "--wait", "--scale", r.be.workerSvc+"=0"); err != nil {
		return err
	}
	defer func() {
		if r.host != nil {
			r.host.Close()
		}
	}()

	for _, ph := range r.be.phases {
		if !contains(r.cfg.Phases, ph) {
			continue
		}
		fmt.Printf("--- %s\n", ph)
		// Published host ports are ephemeral and change when a container
		// restarts (the failover phase restarts coordinators), so the
		// host-side client is re-dialled for every phase.
		if err := r.redialHost(); err != nil {
			return err
		}
		var err error
		switch ph {
		case "saturate":
			err = r.saturate()
		case "latency":
			err = r.latency()
		case "recovery":
			err = r.recovery()
		case "failover":
			err = r.failover()
		case "faults":
			err = r.faults()
		}
		if err != nil {
			r.res.Errors = append(r.res.Errors, ph+": "+err.Error())
			fmt.Printf("  %s failed: %v\n", ph, err)
		}
	}
	return nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func (r *comparer) redialHost() error {
	if r.host != nil {
		r.host.Close()
	}
	target, err := r.hostTarget()
	if err != nil {
		return err
	}
	r.host, err = newJobClient(r.be.client, target, false)
	return err
}

// hostTarget is how the orchestrator reaches the backend from the host.
func (r *comparer) hostTarget() (string, error) {
	if r.be.client == "asynq" {
		return r.hostPort("redis", 6379)
	}
	if len(r.be.coords) == 1 {
		return r.hostPort(r.be.coords[0], 50051)
	}
	var eps []string
	for i, c := range r.be.coords {
		addr, err := r.hostPort(c, 50051)
		if err != nil {
			return "", err
		}
		eps = append(eps, r.be.nodeIDs[i]+"="+addr)
	}
	return strings.Join(eps, ","), nil
}

// setWorkers (re)creates the worker containers for a phase.
func (r *comparer) setWorkers(n, concurrency int, phase string, rpcHistory bool) error {
	r.histDir = filepath.Join(r.dir, phase)
	if err := os.MkdirAll(r.histDir, 0o777); err != nil {
		return err
	}
	_ = os.Chmod(r.histDir, 0o777)
	r.concurrency, r.rpcHistory = concurrency, rpcHistory
	if _, err := r.compose("up", "-d", "--no-deps", "--force-recreate", "--scale", fmt.Sprintf("%s=%d", r.be.workerSvc, n), r.be.workerSvc); err != nil {
		return err
	}
	return r.waitLiveWorkers(n)
}

func (r *comparer) waitLiveWorkers(n int) error {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		live, err := r.host.LiveWorkers(ctx)
		cancel()
		if err == nil && live == n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d live workers, want %d (last error %v)", live, n, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (r *comparer) drive(args ...string) error {
	full := append([]string{"run", "--rm", "-T", "--no-deps", "driver",
		"--client", r.be.client, "--target", r.be.driverTarget, "--history-dir", "/history"}, args...)
	cmd := exec.Command("docker", append([]string{"compose", "-p", r.project, "-f", r.cfg.ComposeFile, "--profile", "driver"}, full...)...)
	cmd.Env = r.env()
	out, err := cmd.CombinedOutput()
	fmt.Printf("  driver: %s", out)
	if err != nil {
		return fmt.Errorf("driver: %w", err)
	}
	return nil
}

// waitExecEnds waits until the phase's history holds n handler ends.
func (r *comparer) waitExecEnds(n int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		got := countLines(r.histDir, `"m":"exec_end"`)
		if got >= n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("only %d of %d jobs finished within %v", got, n, timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// saturate preloads noop jobs with no workers running, then starts the
// workers and measures how fast they drain the queue.
func (r *comparer) saturate() error {
	if err := r.setWorkers(0, r.cfg.SatConcurrency, "saturate", false); err != nil {
		return err
	}
	n := r.cfg.SatJobs
	if err := r.drive("--mode", "preload", "--jobs", fmt.Sprint(n), "--type", "noop", "--key-prefix", "sat", "--submitters", "64"); err != nil {
		return err
	}
	if err := r.setWorkers(r.cfg.SatWorkers, r.cfg.SatConcurrency, "saturate", false); err != nil {
		return err
	}
	if err := r.waitExecEnds(n, 10*time.Minute); err != nil {
		return err
	}
	p, err := loadPhase(r.histDir, "sat")
	if err != nil {
		return err
	}
	first, last, done := p.span()
	secs := float64(last-first) / 1e9
	r.res.Saturation = &SaturationResult{Jobs: n, Workers: r.cfg.SatWorkers, Slots: r.cfg.SatWorkers * r.cfg.SatConcurrency,
		DurationSec: secs, JobsPerSec: float64(done) / secs, CompletedJobs: done}
	fmt.Printf("  saturation: %d jobs in %.2fs = %.0f jobs/s\n", done, secs, float64(done)/secs)
	return nil
}

// latency runs a steady, moderate load and measures dispatch latency and
// queue wait.
func (r *comparer) latency() error {
	if err := r.setWorkers(r.cfg.SatWorkers, r.cfg.SatConcurrency, "latency", false); err != nil {
		return err
	}
	const warm = 1000
	if err := r.drive("--mode", "steady", "--rate", "500", "--jobs", fmt.Sprint(warm), "--type", "noop", "--key-prefix", "warm"); err != nil {
		return err
	}
	if err := r.waitExecEnds(warm, 2*time.Minute); err != nil {
		return err
	}
	r.resetRelayMetrics()
	n := r.cfg.LatJobs
	if err := r.drive("--mode", "steady", "--rate", fmt.Sprint(r.cfg.LatRate), "--jobs", fmt.Sprint(n),
		"--type", "sleep", "--sleep-ms", fmt.Sprint(r.cfg.LatSleepMs), "--key-prefix", "lat", "--submitters", "64"); err != nil {
		return err
	}
	if err := r.waitExecEnds(warm+n, 5*time.Minute); err != nil {
		return err
	}
	p, err := loadPhase(r.histDir, "lat")
	if err != nil {
		return err
	}
	disp, wait := p.dispatch(r.cfg.SatConcurrency)
	lr := &LatencyResult{Rate: r.cfg.LatRate, Jobs: n, SleepMs: r.cfg.LatSleepMs,
		Dispatch: distOf(disp), QueueWait: distOf(wait), SubmitRTT: distOf(p.rtt)}
	if snap, ok := r.scrapeRelayMetrics(); ok {
		lr.Internal, lr.InternalQueueWait = &snap.SchedulingLatency, &snap.QueueWait
	}
	r.res.Latency = lr
	fmt.Printf("  dispatch (external) p50=%.2fms p95=%.2fms p99=%.2fms; queue wait p50=%.2fms p99=%.2fms\n",
		lr.Dispatch.P50Ms, lr.Dispatch.P95Ms, lr.Dispatch.P99Ms, lr.QueueWait.P50Ms, lr.QueueWait.P99Ms)
	if lr.Internal != nil {
		fmt.Printf("  dispatch (coordinator) p50=%.2fms p95=%.2fms p99=%.2fms\n", lr.Internal.P50Ms, lr.Internal.P95Ms, lr.Internal.P99Ms)
	}
	return nil
}

func (r *comparer) metricsAddrs() []string {
	var addrs []string
	if r.be.client != "relay" {
		return nil
	}
	for _, c := range r.be.coords {
		if a, err := r.hostPort(c, 9090); err == nil {
			addrs = append(addrs, a)
		}
	}
	return addrs
}

func (r *comparer) resetRelayMetrics() {
	for _, a := range r.metricsAddrs() {
		if resp, err := http.Post("http://"+a+"/metrics/reset", "", nil); err == nil {
			resp.Body.Close()
		}
	}
}

// scrapeRelayMetrics returns the snapshot of the coordinator that served
// the phase (the one with the most assignments).
func (r *comparer) scrapeRelayMetrics() (metrics.Snapshot, bool) {
	var best metrics.Snapshot
	found := false
	for _, a := range r.metricsAddrs() {
		resp, err := http.Get("http://" + a + "/metrics.json")
		if err != nil {
			continue
		}
		var s metrics.Snapshot
		err = json.NewDecoder(resp.Body).Decode(&s)
		resp.Body.Close()
		if err == nil && s.SchedulingLatency.Count > best.SchedulingLatency.Count {
			best, found = s, true
		}
	}
	return best, found
}

// noteIndex tracks handler starts and ends per job from a tailer.
type noteIndex struct {
	t      *tailer
	starts map[string][]history.Record
	ends   map[string][]history.Record
	ops    map[string]history.Record // pending invokes by proc/op
	// acquires are completed acquire ops that returned a job.
	acquires []acquireOp
}

type acquireOp struct{ invokeWall, doneWall int64 }

func newNoteIndex(dir string) *noteIndex {
	return &noteIndex{t: newTailer(dir), starts: map[string][]history.Record{}, ends: map[string][]history.Record{},
		ops: map[string]history.Record{}}
}

func (x *noteIndex) poll() {
	recs, _ := x.t.poll()
	for _, rec := range recs {
		switch {
		case rec.Method == history.ExecStart:
			x.starts[rec.JobID] = append(x.starts[rec.JobID], rec)
		case rec.Method == history.ExecEnd:
			x.ends[rec.JobID] = append(x.ends[rec.JobID], rec)
		case rec.Method == history.Acquire:
			k := fmt.Sprintf("%s/%d", rec.Process, rec.Op)
			if rec.Type == history.Invoke {
				x.ops[k] = rec
			} else if inv, ok := x.ops[k]; ok {
				delete(x.ops, k)
				if rec.Type == history.OK && rec.HasJob {
					x.acquires = append(x.acquires, acquireOp{inv.WallNs, rec.WallNs})
				}
			}
		}
	}
}

// containerOfProcess maps a history process name (role-host-pid-start) to
// the worker container whose hostname (its short id) it carries.
func (r *comparer) containerOfProcess(proc string) (string, error) {
	rest := strings.TrimPrefix(strings.TrimPrefix(proc, "asynq-worker-"), "worker-")
	host, _, _ := strings.Cut(rest, "-")
	ids, err := r.containers(r.be.workerSvc)
	if err != nil {
		return "", err
	}
	for _, id := range ids {
		if strings.HasPrefix(id, host) || strings.HasPrefix(host, id) {
			return id, nil
		}
	}
	return "", fmt.Errorf("no container for process %s", proc)
}

// dieTime returns when a killed container died, on the Docker daemon's
// (VM) clock, which is also the workers' wall clock.
func dieTime(id string) (time.Time, error) {
	out, err := docker("inspect", "-f", "{{.State.FinishedAt}}", id)
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339Nano, out)
}

func (r *comparer) submitHost(key, typ string, sleepMs, maxAttempts int) (string, error) {
	for attempt := 0; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		id, err := r.host.Submit(ctx, key, typ, payloadFor(typ, sleepMs), maxAttempts)
		cancel()
		if err == nil {
			return id, nil
		}
		if attempt > 100 {
			return "", err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// recovery: per trial, run long jobs on spare capacity, docker kill a
// worker holding at least one, and time until each of its jobs starts
// again on another worker.
func (r *comparer) recovery() error {
	if err := r.setWorkers(r.cfg.TrialWorkers, r.cfg.TrialSlots, "recovery", false); err != nil {
		return err
	}
	x := newNoteIndex(r.histDir)
	tr := &TrialsResult{Method: "docker kill of a worker holding jobs (container FinishedAt, VM clock) -> " +
		"the last of its jobs starts on another worker (handler start note, VM wall clock)"}
	r.res.Recovery = tr
	const perTrial = 4
	for trial := 0; trial < r.cfg.Trials; trial++ {
		secs, err := r.recoveryTrial(x, trial, perTrial)
		if err != nil {
			tr.Failures = append(tr.Failures, fmt.Sprintf("trial %d: %v", trial, err))
			fmt.Printf("  trial %d failed: %v\n", trial, err)
			if err := r.setWorkers(r.cfg.TrialWorkers, r.cfg.TrialSlots, "recovery", false); err != nil {
				return err
			}
			x = newNoteIndex(r.histDir)
			continue
		}
		tr.Seconds = append(tr.Seconds, secs)
		tr.Trials++
		fmt.Printf("  trial %2d: recovered in %.2fs\n", trial, secs)
	}
	tr.Dist = distOf(secondsToDurations(tr.Seconds))
	return nil
}

func (r *comparer) recoveryTrial(x *noteIndex, trial, perTrial int) (float64, error) {
	var ids []string
	for k := 0; k < perTrial; k++ {
		id, err := r.submitHost(fmt.Sprintf("rec%d-%d", trial, k), "sleep", 8000, 5)
		if err != nil {
			return 0, err
		}
		ids = append(ids, id)
	}
	// Wait for every job to start, then pick the worker running the most.
	deadline := time.Now().Add(2 * time.Minute)
	holder := map[string]string{}
	for len(holder) < perTrial {
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("only %d of %d jobs started", len(holder), perTrial)
		}
		time.Sleep(50 * time.Millisecond)
		x.poll()
		for _, id := range ids {
			if s := x.starts[id]; len(s) > 0 {
				holder[id] = s[len(s)-1].Process
			}
		}
	}
	count := map[string]int{}
	for _, p := range holder {
		count[p]++
	}
	var victim string
	for p, n := range count {
		if victim == "" || n > count[victim] || (n == count[victim] && p < victim) {
			victim = p
		}
	}
	var victimJobs []string
	for id, p := range holder {
		if p == victim {
			victimJobs = append(victimJobs, id)
		}
	}
	container, err := r.containerOfProcess(victim)
	if err != nil {
		return 0, err
	}
	if _, err := docker("kill", container); err != nil {
		return 0, err
	}
	died, err := dieTime(container)
	if err != nil {
		return 0, err
	}
	// Wait for each victim job to start on another worker after the kill.
	deadline = time.Now().Add(5 * time.Minute)
	var worst time.Duration
	for _, id := range victimJobs {
		for {
			x.poll()
			var restarted *history.Record
			for i := range x.starts[id] {
				s := &x.starts[id][i]
				if s.Process != victim && s.WallNs > died.UnixNano() {
					restarted = s
					break
				}
			}
			if restarted != nil {
				worst = max(worst, time.Duration(restarted.WallNs-died.UnixNano()))
				break
			}
			if time.Now().After(deadline) {
				return 0, fmt.Errorf("job %s not restarted within 5m of the kill", id)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	// Restore the worker and let the trial's jobs finish before the next.
	if _, err := docker("start", container); err != nil {
		return 0, err
	}
	deadline = time.Now().Add(3 * time.Minute)
	for {
		x.poll()
		done := 0
		for _, id := range ids {
			if len(x.ends[id]) > 0 {
				done++
			}
		}
		if done == perTrial {
			break
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("trial jobs did not finish")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err := r.waitLiveWorkers(r.cfg.TrialWorkers); err != nil {
		return 0, err
	}
	return worst.Seconds(), nil
}

func secondsToDurations(s []float64) []time.Duration {
	out := make([]time.Duration, len(s))
	for i, v := range s {
		out[i] = time.Duration(v * float64(time.Second))
	}
	return out
}

// failover (Raft only): under a steady trickle of jobs, docker kill the
// leader and time until a worker's acquire, sent after the kill, returns a
// job (which only a new leader can grant).
func (r *comparer) failover() error {
	if err := r.setWorkers(r.cfg.TrialWorkers, r.cfg.TrialSlots, "failover", true); err != nil {
		return err
	}
	x := newNoteIndex(r.histDir)
	tr := &TrialsResult{Method: "docker kill of the Raft leader (container FinishedAt, VM clock) -> first AcquireJob " +
		"invoked after the kill that returns a job (worker history, VM wall clock)"}
	r.res.Failover = tr

	// A steady trickle of jobs from inside the cluster (by DNS name, so it
	// keeps working while coordinators restart on new host ports).
	name := r.project + "-failover-trickle"
	_, _ = docker("rm", "-f", name)
	trickle := exec.Command("docker", "compose", "-p", r.project, "-f", r.cfg.ComposeFile, "--profile", "driver",
		"run", "--rm", "-T", "--no-deps", "--name", name, "driver",
		"--client", r.be.client, "--target", r.be.driverTarget, "--history-dir", "/history",
		"--mode", "steady", "--rate", "50", "--jobs", "10000000", "--type", "noop", "--key-prefix", "fo", "--submitters", "4")
	trickle.Env = r.env()
	if err := trickle.Start(); err != nil {
		return err
	}
	defer func() {
		_, _ = docker("rm", "-f", name)
		_ = trickle.Process.Kill()
		_ = trickle.Wait()
	}()

	for trial := 0; trial < r.cfg.Trials; trial++ {
		secs, err := r.failoverTrial(x)
		if err != nil {
			tr.Failures = append(tr.Failures, fmt.Sprintf("trial %d: %v", trial, err))
			fmt.Printf("  trial %d failed: %v\n", trial, err)
			continue
		}
		tr.Seconds = append(tr.Seconds, secs)
		tr.Trials++
		fmt.Printf("  trial %2d: new leader serving after %.3fs\n", trial, secs)
	}
	tr.Dist = distOf(secondsToDurations(tr.Seconds))
	return nil
}

func (r *comparer) clusterStatus(svc string) (*relayv1.ClusterStatusResponse, error) {
	addr, err := r.hostPort(svc, 50051)
	if err != nil {
		return nil, err
	}
	conn, err := rpc.Dial(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return relayv1.NewClusterServiceClient(conn).ClusterStatus(ctx, &relayv1.ClusterStatusRequest{})
}

// waitHealthyCluster waits until every node is up, one leads, and the
// others have caught up with it.
func (r *comparer) waitHealthyCluster() (leader string, err error) {
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		leader = ""
		var commit uint64
		var applied []uint64
		ok := true
		for _, svc := range r.be.coords {
			st, err := r.clusterStatus(svc)
			if err != nil {
				ok = false
				break
			}
			if st.State == "Leader" {
				leader, commit = svc, st.CommitIndex
			}
			applied = append(applied, st.AppliedIndex)
		}
		if ok && leader != "" {
			caughtUp := true
			for _, a := range applied {
				if a+50 < commit {
					caughtUp = false
				}
			}
			if caughtUp {
				return leader, nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return "", errors.New("cluster did not become healthy")
}

func (r *comparer) failoverTrial(x *noteIndex) (float64, error) {
	leader, err := r.waitHealthyCluster()
	if err != nil {
		return 0, err
	}
	time.Sleep(2 * time.Second) // steady state before the kill
	ids, err := r.containers(leader)
	if err != nil || len(ids) == 0 {
		return 0, fmt.Errorf("leader %s container: %v", leader, err)
	}
	if _, err := docker("kill", ids[0]); err != nil {
		return 0, err
	}
	// The killed node always comes back, whatever happens in the trial.
	defer func() { _, _ = docker("start", ids[0]) }()
	died, err := dieTime(ids[0])
	if err != nil {
		return 0, err
	}
	deadline := time.Now().Add(time.Minute)
	var served time.Duration
	for served == 0 {
		if time.Now().After(deadline) {
			return 0, errors.New("no acquire served within 1m of the kill")
		}
		time.Sleep(10 * time.Millisecond)
		x.poll()
		for _, a := range x.acquires {
			if a.invokeWall > died.UnixNano() {
				d := time.Duration(a.doneWall - died.UnixNano())
				if served == 0 || d < served {
					served = d
				}
			}
		}
	}
	return served.Seconds(), nil
}

// faults runs a seeded fault workload (worker kill, pause and partition)
// and counts lost jobs, duplicate executions and, where observable,
// duplicate terminal transitions.
func (r *comparer) faults() error {
	if err := r.setWorkers(r.cfg.TrialWorkers, 4, "faults", false); err != nil {
		return err
	}
	target, err := r.hostTarget()
	if err != nil {
		return err
	}
	client, err := newJobClient(r.be.client, target, true)
	if err != nil {
		return err
	}
	defer client.Close()
	dur, err := time.ParseDuration(r.cfg.FaultDuration)
	if err != nil {
		return err
	}
	fr := &FaultsResult{DurationSec: dur.Seconds()}
	r.res.Faults = fr
	rng := rand.New(rand.NewPCG(r.cfg.Seed, 7))

	var mu sync.Mutex
	acked := map[string]bool{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var n atomic.Int64
	interval := time.Duration(float64(time.Second) / r.cfg.FaultRate)
	for s := 0; s < 4; s++ {
		wg.Add(1)
		go func(s int) {
			defer wg.Done()
			jr := rand.New(rand.NewPCG(r.cfg.Seed, uint64(100+s)))
			t := time.NewTicker(interval * 4)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
				}
				key := fmt.Sprintf("flt-%d", n.Add(1))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				id, err := client.Submit(ctx, key, "sleep", payloadFor("sleep", 200+jr.IntN(1300)), 5)
				cancel()
				if err == nil {
					mu.Lock()
					acked[id] = true
					mu.Unlock()
				}
			}
		}(s)
	}

	// Faults: one at a time per worker; heal everything at the end.
	workers, err := r.containers(r.be.workerSvc)
	if err != nil {
		return err
	}
	network := r.project + "_default"
	type pending struct {
		at   time.Time
		heal func()
	}
	var heals []pending
	busy := map[string]time.Time{}
	end := time.Now().Add(dur)
	for time.Now().Before(end) {
		time.Sleep(time.Duration(3000+rng.IntN(5000)) * time.Millisecond)
		now := time.Now()
		var still []pending
		for _, h := range heals {
			if now.After(h.at) {
				h.heal()
			} else {
				still = append(still, h)
			}
		}
		heals = still
		id := workers[rng.IntN(len(workers))]
		hold := time.Duration(2000+rng.IntN(8000)) * time.Millisecond
		kind := []string{"kill", "pause", "partition"}[rng.IntN(3)]
		if busy[id].After(now) || now.Add(hold).After(end) {
			continue
		}
		busy[id] = now.Add(hold + time.Second)
		short := id[:12]
		switch kind {
		case "kill":
			_, err = docker("kill", id)
			heals = append(heals, pending{now.Add(hold), func() { _, _ = docker("start", id) }})
		case "pause":
			_, err = docker("pause", id)
			heals = append(heals, pending{now.Add(hold), func() { _, _ = docker("unpause", id) }})
		case "partition":
			_, err = docker("network", "disconnect", network, id)
			heals = append(heals, pending{now.Add(hold), func() { _, _ = docker("network", "connect", network, id) }})
		}
		desc := fmt.Sprintf("t+%.1fs %s %s for %v", time.Until(end).Seconds()*-1+dur.Seconds(), kind, short, hold.Round(100*time.Millisecond))
		if err != nil {
			desc += " (failed: " + firstLine(err.Error()) + ")"
		}
		fr.Faults = append(fr.Faults, desc)
	}
	close(stop)
	wg.Wait()
	for _, h := range heals {
		h.heal()
	}
	for _, id := range workers {
		_, _ = docker("unpause", id)
		_, _ = docker("start", id)
		_, _ = docker("network", "connect", network, id)
	}
	fmt.Printf("  %d faults injected; %d jobs acknowledged; quiescing...\n", len(fr.Faults), len(acked))

	// Quiesce: wait for every acknowledged job to reach a terminal state.
	ids := make([]string, 0, len(acked))
	for id := range acked {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	states := map[string]string{}
	terminals := map[string]int{}
	deadline := time.Now().Add(8 * time.Minute)
	for {
		pendingN := 0
		for _, id := range ids {
			if st := states[id]; st == stateSucceeded || st == stateFailed || st == stateDead {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			st, term, err := client.State(ctx, id)
			cancel()
			if err != nil {
				st = "missing"
			}
			states[id], terminals[id] = st, term
			if st != stateSucceeded && st != stateFailed && st != stateDead {
				pendingN++
			}
		}
		if pendingN == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Second)
	}
	// Give workers a moment to flush their last notes.
	time.Sleep(2 * time.Second)
	// The orchestrator submitted these jobs itself, so count executions
	// straight from the workers' handler notes.
	h, err := history.ReadDir(r.histDir)
	if err != nil {
		return err
	}
	ends := map[string]int{}
	for _, n := range h.Notes {
		if n.Method == history.ExecEnd && acked[n.JobID] {
			ends[n.JobID]++
		}
	}
	fr.AckedJobs = len(ids)
	fr.DuplicateTerminal = 0
	for _, id := range ids {
		switch states[id] {
		case stateSucceeded:
			fr.Succeeded++
		case stateDead, stateFailed:
			fr.Dead++
		case "missing":
			fr.Lost++
		default:
			fr.Unfinished++
		}
		if terminals[id] < 0 {
			fr.DuplicateTerminal = -1
		} else if fr.DuplicateTerminal >= 0 && terminals[id] > 1 {
			fr.DuplicateTerminal++
		}
		if ends[id] > 1 {
			fr.DuplicateExecutions++
		}
	}
	fmt.Printf("  faults: %d acked, %d succeeded, %d dead, %d lost, %d unfinished, %d duplicate executions, duplicate terminal=%d\n",
		fr.AckedJobs, fr.Succeeded, fr.Dead, fr.Lost, fr.Unfinished, fr.DuplicateExecutions, fr.DuplicateTerminal)
	return nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
