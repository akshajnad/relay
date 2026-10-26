// Command relay-bench brings up a compose cluster, drives load through it,
// and reports the coordinator-measured scheduling latency, queue wait and
// throughput.
//
// Scheduling latency is measured by the coordinator (one clock) for every
// assignment as: time the job was handed to a worker minus max(time the job
// became eligible, time that worker's AcquireJob request arrived). It
// excludes time a job waits because every worker is busy; that wait shows up
// in queue wait instead.
//
//	go run ./cmd/relay-bench                      # burst + steady, 10x10 workers, 5000 jobs
//	go run ./cmd/relay-bench --modes preload --jobs 5000
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/envflag"
	"github.com/akshajnad/relay/internal/metrics"
	"github.com/akshajnad/relay/internal/rpc"
)

type Config struct {
	ComposeFile    string        `json:"compose_file"`
	Project        string        `json:"project"`
	Manage         bool          `json:"manage_cluster"`
	Build          bool          `json:"build"`
	Down           bool          `json:"down"`
	Coordinator    string        `json:"coordinator"`
	MetricsAddr    string        `json:"metrics_addr"`
	Workers        int           `json:"worker_containers"`
	Concurrency    int           `json:"worker_concurrency"`
	Jobs           int           `json:"jobs"`
	Modes          []string      `json:"modes"`
	Rate           float64       `json:"steady_rate_per_sec"`
	Sleep          time.Duration `json:"sleep_duration"`
	Submitters     int           `json:"submitters"`
	Warmup         int           `json:"warmup_jobs"`
	Profile        bool          `json:"profile"`
	ProfileSeconds int           `json:"profile_seconds"`
	OutDir         string        `json:"out_dir"`
	Label          string        `json:"label,omitempty"`
	Timeout        time.Duration `json:"run_timeout"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "relay-bench:", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg Config
	var modes string
	fs := flag.NewFlagSet("relay-bench", flag.ExitOnError)
	fs.StringVar(&cfg.ComposeFile, "compose", "deploy/docker-compose.yml", "compose file")
	fs.StringVar(&cfg.Project, "project", "relay-bench", "compose project name")
	fs.BoolVar(&cfg.Manage, "manage", true, "bring the compose cluster up (and down); false targets --coordinator as-is")
	fs.BoolVar(&cfg.Build, "build", true, "build the image before starting the cluster")
	fs.BoolVar(&cfg.Down, "down", true, "tear the cluster down afterwards")
	fs.StringVar(&cfg.Coordinator, "coordinator", "127.0.0.1:50051", "coordinator gRPC address")
	fs.StringVar(&cfg.MetricsAddr, "metrics", "127.0.0.1:9090", "coordinator metrics address")
	fs.IntVar(&cfg.Workers, "workers", 10, "worker containers")
	fs.IntVar(&cfg.Concurrency, "concurrency", 10, "slots per worker container")
	fs.IntVar(&cfg.Jobs, "jobs", 5000, "jobs per run")
	fs.StringVar(&modes, "modes", "burst,steady", "comma-separated runs: burst (submit as fast as possible), steady (--rate jobs/s), preload (enqueue everything, then start workers)")
	fs.Float64Var(&cfg.Rate, "rate", 2000, "submission rate for steady mode, jobs/s")
	fs.DurationVar(&cfg.Sleep, "sleep", 20*time.Millisecond, "duration of each sleep job")
	fs.IntVar(&cfg.Submitters, "submitters", 32, "concurrent submitting goroutines")
	fs.IntVar(&cfg.Warmup, "warmup", 500, "jobs run before measuring, to warm connections and caches")
	fs.BoolVar(&cfg.Profile, "profile", false, "capture a coordinator CPU profile during each run")
	fs.IntVar(&cfg.ProfileSeconds, "profile-seconds", 5, "CPU profile length")
	fs.StringVar(&cfg.OutDir, "out", "bench/results", "results directory")
	fs.StringVar(&cfg.Label, "label", "", "free-form label stored with the results")
	fs.DurationVar(&cfg.Timeout, "timeout", 5*time.Minute, "per-run timeout")
	if err := envflag.Parse(fs, "RELAY_BENCH", os.Args[1:]); err != nil {
		return err
	}
	for _, m := range strings.Split(modes, ",") {
		m = strings.TrimSpace(m)
		switch m {
		case "burst", "steady", "preload":
			cfg.Modes = append(cfg.Modes, m)
		case "":
		default:
			return fmt.Errorf("unknown mode %q", m)
		}
	}

	b := &bench{cfg: cfg, http: &http.Client{Timeout: 60 * time.Second}}
	if cfg.Manage {
		b.compose = newCompose(cfg)
		fmt.Printf("bringing up %d workers x %d slots (project %s)...\n", cfg.Workers, cfg.Concurrency, cfg.Project)
		if err := b.compose.up(cfg.Workers); err != nil {
			return err
		}
		if cfg.Down {
			defer func() {
				fmt.Println("tearing down cluster...")
				_ = b.compose.down()
			}()
		}
	}

	conn, err := rpc.Dial(cfg.Coordinator)
	if err != nil {
		return err
	}
	defer conn.Close()
	b.jobs = relayv1.NewJobServiceClient(conn)
	b.workers = relayv1.NewWorkerServiceClient(conn)

	if err := b.waitWorkers(cfg.Workers, 2*time.Minute); err != nil {
		return err
	}
	if cfg.Warmup > 0 {
		fmt.Printf("warmup: %d jobs...\n", cfg.Warmup)
		if _, err := b.runOnce("burst", cfg.Warmup); err != nil {
			return fmt.Errorf("warmup: %w", err)
		}
	}

	report := Report{
		Timestamp: time.Now().UTC(),
		Config:    cfg,
		Host:      hostInfo(),
		Docker:    dockerInfo(),
		Target:    Target{Description: "p95 scheduling latency < 25ms with 100 worker slots and 5,000 queued jobs", P95Ms: 25},
	}
	for _, mode := range cfg.Modes {
		fmt.Printf("\n=== %s: %d jobs ===\n", mode, cfg.Jobs)
		r, err := b.runOnce(mode, cfg.Jobs)
		if err != nil {
			return fmt.Errorf("%s run: %w", mode, err)
		}
		report.Runs = append(report.Runs, *r)
		printRun(r)
	}
	report.Target.evaluate(report.Runs)

	path, err := report.write(cfg.OutDir)
	if err != nil {
		return err
	}
	printSummary(&report)
	fmt.Printf("\nresults written to %s\n", path)
	return nil
}

type bench struct {
	cfg     Config
	compose *compose
	jobs    relayv1.JobServiceClient
	workers relayv1.WorkerServiceClient
	http    *http.Client
}

func (b *bench) waitWorkers(n int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := b.workers.ListWorkers(ctx, &relayv1.ListWorkersRequest{})
		cancel()
		alive, slots := 0, 0
		if err == nil {
			for _, w := range resp.Workers {
				if w.State == "ALIVE" {
					alive++
					slots += int(w.Concurrency)
				}
			}
			if alive == n {
				fmt.Printf("%d workers alive, %d slots\n", alive, slots)
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %d live workers (have %d, err %v)", n, alive, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Run is the measured outcome of one mode.
type Run struct {
	Mode string `json:"mode"`
	Jobs int    `json:"jobs"`
	// SubmitSeconds is how long submitting all jobs took (client clock).
	SubmitSeconds float64 `json:"submit_seconds"`
	// MakespanSeconds runs from the start of dispatch (first submit, or
	// worker start for preload) until the last job succeeded.
	MakespanSeconds   float64           `json:"makespan_seconds"`
	ThroughputPerSec  float64           `json:"throughput_jobs_per_sec"`
	SchedulingLatency metrics.Summary   `json:"scheduling_latency"`
	QueueWait         metrics.Summary   `json:"queue_wait"`
	AcquireScript     metrics.Summary   `json:"acquire_script_rtt"`
	Counters          metrics.Counters  `json:"counters"`
	Profile           string            `json:"cpu_profile,omitempty"`
	Notes             map[string]string `json:"notes,omitempty"`
}

func (b *bench) runOnce(mode string, n int) (*Run, error) {
	if mode == "preload" {
		if b.compose == nil {
			return nil, errors.New("preload mode needs --manage to stop and start workers")
		}
		fmt.Println("stopping workers to preload the queue...")
		if err := b.compose.scale(0); err != nil {
			return nil, err
		}
	}
	if err := b.resetMetrics(); err != nil {
		return nil, err
	}

	var profile chan string
	if b.cfg.Profile && n == b.cfg.Jobs {
		profile = make(chan string, 1)
		go func() { profile <- b.captureProfile(mode) }()
	}

	payload := []byte(fmt.Sprintf(`{"duration_ms":%d}`, b.cfg.Sleep.Milliseconds()))
	start := time.Now()
	if err := b.submitAll(mode, n, payload); err != nil {
		return nil, err
	}
	submitDur := time.Since(start)
	fmt.Printf("submitted %d jobs in %.2fs (%.0f/s)\n", n, submitDur.Seconds(), float64(n)/submitDur.Seconds())

	dispatchStart := start
	if mode == "preload" {
		fmt.Printf("queue preloaded; starting %d workers...\n", b.cfg.Workers)
		dispatchStart = time.Now()
		if err := b.compose.scale(b.cfg.Workers); err != nil {
			return nil, err
		}
	}

	done, err := b.waitDone(int64(n), b.cfg.Timeout)
	if err != nil {
		return nil, err
	}
	snap, err := b.scrape()
	if err != nil {
		return nil, err
	}
	makespan := done.Sub(dispatchStart)
	r := &Run{
		Mode:              mode,
		Jobs:              n,
		SubmitSeconds:     submitDur.Seconds(),
		MakespanSeconds:   makespan.Seconds(),
		ThroughputPerSec:  float64(n) / makespan.Seconds(),
		SchedulingLatency: snap.SchedulingLatency,
		QueueWait:         snap.QueueWait,
		AcquireScript:     snap.AcquireScript,
		Counters:          snap.Counters,
	}
	if mode == "preload" {
		r.Notes = map[string]string{"makespan": "includes worker container start-up after the queue was filled"}
	}
	if profile != nil {
		r.Profile = <-profile
	}
	return r, nil
}

func (b *bench) submitAll(mode string, n int, payload []byte) error {
	var next atomic.Int64
	var firstErr error
	var errOnce sync.Once
	var tick <-chan time.Time
	if mode == "steady" {
		t := time.NewTicker(time.Duration(float64(time.Second) / b.cfg.Rate))
		defer t.Stop()
		tick = t.C
	}
	var wg sync.WaitGroup
	for i := 0; i < b.cfg.Submitters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if tick != nil {
					<-tick
				}
				if next.Add(1) > int64(n) {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				_, err := b.jobs.SubmitJob(ctx, &relayv1.SubmitJobRequest{Type: "sleep", Payload: payload, MaxAttempts: 3})
				cancel()
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
			}
		}()
	}
	wg.Wait()
	return firstErr
}

// waitDone polls the coordinator's success counter (reset before the run)
// until n jobs have succeeded.
func (b *bench) waitDone(n int64, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	for {
		snap, err := b.scrape()
		if err == nil {
			c := snap.Counters
			if c.Succeeded >= n {
				return time.Now(), nil
			}
			if c.Failed+c.Dead > 0 {
				return time.Time{}, fmt.Errorf("%d jobs failed and %d died during the run", c.Failed, c.Dead)
			}
		}
		if time.Now().After(deadline) {
			return time.Time{}, fmt.Errorf("timed out: %d of %d jobs succeeded", snap.Counters.Succeeded, n)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (b *bench) scrape() (metrics.Snapshot, error) {
	var s metrics.Snapshot
	resp, err := b.http.Get("http://" + b.cfg.MetricsAddr + "/metrics")
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

func (b *bench) resetMetrics() error {
	resp, err := b.http.Post("http://"+b.cfg.MetricsAddr+"/metrics/reset", "", nil)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("metrics reset: %s", resp.Status)
	}
	return nil
}

func (b *bench) captureProfile(mode string) string {
	url := fmt.Sprintf("http://%s/debug/pprof/profile?seconds=%d", b.cfg.MetricsAddr, b.cfg.ProfileSeconds)
	resp, err := b.http.Get(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, "profile:", err)
		return ""
	}
	defer resp.Body.Close()
	if err := os.MkdirAll(b.cfg.OutDir, 0o755); err != nil {
		return ""
	}
	path := filepath.Join(b.cfg.OutDir, fmt.Sprintf("%s-%s.cpu.pprof", time.Now().UTC().Format("20060102T150405Z"), mode))
	f, err := os.Create(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	if _, err := io.Copy(f, resp.Body); err != nil {
		return ""
	}
	return path
}
