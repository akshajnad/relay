package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/buildrev"
	"github.com/akshajnad/relay/internal/check"
	"github.com/akshajnad/relay/internal/history"
	"github.com/akshajnad/relay/internal/rpc"
	"github.com/akshajnad/relay/internal/store"
	"google.golang.org/grpc"
)

type runConfig struct {
	Seed           uint64
	Duration       time.Duration
	Faults         string
	Backend        string
	Workers        int
	Concurrency    int
	Image          string
	ImageBuild     buildrev.Image
	OutRoot        string
	ComposeFile    string
	Keep           bool
	Clients        int
	Rate           float64
	SleepMinMs     int
	SleepMaxMs     int
	QuiesceTimeout time.Duration
	ExpectFail     bool
}

// backend describes how one store backend is laid out in the chaos compose
// file.
type backend struct {
	profile      string
	coordinators []string // compose service names
	// workerTarget is what workers dial (through toxiproxy).
	workerTarget string
	// loadTarget is what the load generator dials (directly).
	loadTarget string
	raft       bool
	// nodeIDs are the coordinators' Raft server ids, by index.
	nodeIDs []string
}

var backends = map[string]backend{
	"redis": {
		profile:      "redis",
		coordinators: []string{"coordinator"},
		workerTarget: "toxiproxy:17000",
		loadTarget:   "coordinator:50051",
	},
	"raft": {
		profile:      "raft",
		coordinators: []string{"coord1", "coord2", "coord3"},
		nodeIDs:      []string{"n1", "n2", "n3"},
		workerTarget: "n1=toxiproxy:17001,n2=toxiproxy:17002,n3=toxiproxy:17003",
		loadTarget:   "n1=coord1:50051,n2=coord2:50051,n3=coord3:50051",
		raft:         true,
	},
}

func runMain(args []string) error {
	var cfg runConfig
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	fs.Uint64Var(&cfg.Seed, "seed", 1, "seed for the fault schedule and the workload")
	fs.DurationVar(&cfg.Duration, "duration", 5*time.Minute, "how long to run the workload and inject faults")
	fs.StringVar(&cfg.Faults, "faults", "all", `fault kinds: "all" or a comma-separated list`)
	fs.StringVar(&cfg.Backend, "backend", "raft", "store backend: redis or raft")
	fs.IntVar(&cfg.Workers, "workers", 5, "worker containers")
	fs.IntVar(&cfg.Concurrency, "concurrency", 4, "slots per worker")
	fs.StringVar(&cfg.Image, "image", "relay:dev", "relay image (relay:nofence for the deliberate-bug test)")
	fs.StringVar(&cfg.OutRoot, "out", "chaos-out", "directory for run artifacts")
	fs.StringVar(&cfg.ComposeFile, "compose", "deploy/docker-compose.chaos.yml", "chaos compose file")
	fs.BoolVar(&cfg.Keep, "keep", false, "leave the cluster running afterwards")
	fs.IntVar(&cfg.Clients, "clients", 8, "concurrent submitters")
	fs.Float64Var(&cfg.Rate, "rate", 40, "total submits per second")
	fs.IntVar(&cfg.SleepMinMs, "sleep-min-ms", 20, "shortest sleep job")
	fs.IntVar(&cfg.SleepMaxMs, "sleep-max-ms", 1500, "longest sleep job")
	fs.DurationVar(&cfg.QuiesceTimeout, "quiesce-timeout", 3*time.Minute, "how long jobs may take to finish after faults stop")
	fs.BoolVar(&cfg.ExpectFail, "expect-fail", false, "exit 0 only if the checker finds a violation (deliberate-bug runs)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	be, ok := backends[cfg.Backend]
	if !ok {
		return fmt.Errorf("unknown backend %q", cfg.Backend)
	}
	topo := Topology{Workers: cfg.Workers, Coordinators: len(be.coordinators), Raft: be.raft}
	kinds, err := parseKinds(cfg.Faults, topo)
	if err != nil {
		return err
	}
	sched := []Fault{}
	if cfg.Faults != "none" {
		sched = Schedule(cfg.Seed, cfg.Duration, kinds, topo)
	}

	dir := filepath.Join(absPath(cfg.OutRoot), fmt.Sprintf("%s-seed%d-%s", cfg.Backend, cfg.Seed, time.Now().UTC().Format("20060102T150405Z")))
	histDir := filepath.Join(dir, "history")
	if err := os.MkdirAll(histDir, 0o777); err != nil {
		return err
	}
	_ = os.Chmod(histDir, 0o777) // containers write here
	writeJSON(filepath.Join(dir, "schedule.json"), sched)
	cfg.ImageBuild = buildrev.Inspect(cfg.Image)
	writeJSON(filepath.Join(dir, "config.json"), cfg)

	fmt.Printf("relay-chaos: backend=%s seed=%d duration=%s workers=%dx%d image=%s (revision %s)\n",
		cfg.Backend, cfg.Seed, cfg.Duration, cfg.Workers, cfg.Concurrency, cfg.Image, cfg.ImageBuild.Revision)
	fmt.Printf("artifacts: %s\n\nfault schedule (%d faults):\n", dir, len(sched))
	for _, f := range sched {
		fmt.Println("  " + f.String())
	}
	fmt.Println()

	r := &runner{cfg: cfg, be: be, dir: dir}
	r.c = &cluster{
		project: fmt.Sprintf("relaychaos-%s-s%d", cfg.Backend, cfg.Seed),
		file:    cfg.ComposeFile,
		profile: be.profile,
		env: append(os.Environ(),
			"CHAOS_IMAGE="+cfg.Image,
			"CHAOS_HISTORY_DIR="+histDir,
			"CHAOS_OUT_DIR="+dir,
			fmt.Sprintf("CHAOS_SEED=%d", cfg.Seed),
			"CHAOS_WORKER_COORDINATOR="+be.workerTarget,
			fmt.Sprintf("CHAOS_WORKER_CONCURRENCY=%d", cfg.Concurrency),
		),
	}
	rep, err := r.execute(sched)
	if err != nil {
		return err
	}
	failed := !rep.OK()
	if cfg.ExpectFail {
		if !failed {
			return errors.New("expected the checker to find violations, but the run passed")
		}
		fmt.Println("checker flagged violations, as expected for this build")
		return nil
	}
	if failed {
		return errors.New("invariant violations found")
	}
	return nil
}

type runner struct {
	cfg  runConfig
	be   backend
	dir  string
	c    *cluster
	toxi *toxiproxy

	mu       sync.Mutex
	timeline []timelineEntry
	start    time.Time
}

type timelineEntry struct {
	Offset    time.Duration `json:"offset"`
	Action    string        `json:"action"`
	Target    string        `json:"target"`
	Container string        `json:"container,omitempty"`
	Err       string        `json:"err,omitempty"`
}

func (r *runner) note(action, target, container string, err error) {
	e := timelineEntry{Offset: time.Since(r.start), Action: action, Target: target, Container: container}
	if err != nil {
		e.Err = err.Error()
	}
	r.mu.Lock()
	r.timeline = append(r.timeline, e)
	r.mu.Unlock()
	line := fmt.Sprintf("  t+%-8s %-24s %s %s", e.Offset.Round(100*time.Millisecond), action, target, shortID(container))
	if err != nil {
		line += "  ERROR: " + firstLine(err.Error())
	}
	fmt.Println(line)
}

func (r *runner) execute(sched []Fault) (*check.Report, error) {
	c := r.c
	_, _ = c.compose("down", "-v", "--remove-orphans", "--timeout", "1")
	if !r.cfg.Keep {
		defer func() {
			if _, err := c.compose("down", "-v", "--remove-orphans", "--timeout", "5"); err != nil {
				fmt.Fprintln(os.Stderr, "compose down:", err)
			}
		}()
	}
	if _, err := c.compose("up", "-d", "--wait", "--no-build", "--scale", fmt.Sprintf("worker=%d", r.cfg.Workers)); err != nil {
		return nil, err
	}
	toxiAddr, err := c.hostPort("toxiproxy", 8474)
	if err != nil {
		return nil, err
	}
	r.toxi = newToxiproxy(toxiAddr)
	if err := r.waitWorkers(90 * time.Second); err != nil {
		return nil, err
	}

	// The workload runs in its own container on clientnet.
	loadLog, err := os.Create(filepath.Join(r.dir, "load.log"))
	if err != nil {
		return nil, err
	}
	defer loadLog.Close()
	loadArgs := []string{"--profile", "load", "run", "--rm", "-T", "--no-deps", "load",
		"--coordinators", r.be.loadTarget,
		"--seed", fmt.Sprint(r.cfg.Seed),
		"--duration", r.cfg.Duration.String(),
		"--clients", fmt.Sprint(r.cfg.Clients),
		"--rate", fmt.Sprint(r.cfg.Rate),
		"--sleep-min-ms", fmt.Sprint(r.cfg.SleepMinMs),
		"--sleep-max-ms", fmt.Sprint(r.cfg.SleepMaxMs),
		"--quiesce-timeout", r.cfg.QuiesceTimeout.String(),
		"--history-dir", "/history",
		"--out", "/out/final.json",
		"--freeze-dir", "/out",
	}
	loadCmd := exec.Command("docker", c.composeArgs(loadArgs...)...)
	loadCmd.Env = c.env
	loadCmd.Stdout, loadCmd.Stderr = loadLog, loadLog
	r.start = time.Now()
	if err := loadCmd.Start(); err != nil {
		return nil, err
	}
	loadDone := make(chan error, 1)
	go func() { loadDone <- loadCmd.Wait() }()

	fmt.Println("timeline:")
	r.runFaults(sched)
	r.healAll()
	r.note("faults-stopped", "cluster", "", nil)

	var loadErr error
	timeout := time.After(r.cfg.QuiesceTimeout + 3*time.Minute)
	freezePoll := time.NewTicker(100 * time.Millisecond)
	defer freezePoll.Stop()
wait:
	for {
		select {
		case loadErr = <-loadDone:
			break wait
		case <-freezePoll.C:
			if _, err := os.Stat(filepath.Join(r.dir, "quiesced")); err == nil {
				r.freezeWorkers()
				freezePoll.Stop()
			}
		case <-timeout:
			_ = loadCmd.Process.Kill()
			loadErr = errors.New("load generator did not finish")
			break wait
		}
	}
	r.note("load-finished", "load", "", loadErr)
	if logs, err := c.compose("logs", "--no-color", "--timestamps"); err == nil {
		_ = os.WriteFile(filepath.Join(r.dir, "cluster.log"), []byte(logs), 0o644)
	}
	writeJSON(filepath.Join(r.dir, "timeline.json"), r.timeline)
	if loadErr != nil {
		return nil, fmt.Errorf("load generator: %w (see %s)", loadErr, filepath.Join(r.dir, "load.log"))
	}
	return r.check()
}

func (r *runner) check() (*check.Report, error) {
	h, err := history.ReadDir(filepath.Join(r.dir, "history"))
	if err != nil {
		return nil, err
	}
	final, err := readFinal(filepath.Join(r.dir, "final.json"))
	if err != nil {
		return nil, err
	}
	rep := check.Check(h, final, check.Options{})
	return rep, r.report(rep)
}

func (r *runner) report(rep *check.Report) error {
	fmt.Println()
	fmt.Print(rep.String())
	if err := rep.WriteJSON(filepath.Join(r.dir, "report.json")); err != nil {
		return err
	}
	text := rep.String()
	if !rep.OK() {
		paths, err := rep.Visualize(r.dir, 20)
		if err != nil {
			return err
		}
		for _, p := range paths {
			fmt.Println("  visualization:", p)
		}
		text += fmt.Sprintf("\nhistory: %s\nvisualizations: %v\n", filepath.Join(r.dir, "history"), paths)
		fmt.Printf("  history saved in %s\n", filepath.Join(r.dir, "history"))
	}
	return os.WriteFile(filepath.Join(r.dir, "report.txt"), []byte(text), 0o644)
}

func (r *runner) waitWorkers(timeout time.Duration) error {
	addr, err := r.c.hostPort(r.be.coordinators[0], 50051)
	if err != nil {
		return err
	}
	conn, closeConn, err := dialCoordinators(r.hostEndpoints(addr))
	if err != nil {
		return err
	}
	defer closeConn()
	client := relayv1.NewWorkerServiceClient(conn)
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.ListWorkers(ctx, &relayv1.ListWorkersRequest{})
		cancel()
		alive := 0
		if err == nil {
			for _, w := range resp.Workers {
				if w.State == store.WorkerAlive {
					alive++
				}
			}
		}
		if alive >= r.cfg.Workers {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("only %d of %d workers registered (last error: %v)", alive, r.cfg.Workers, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// hostEndpoints is the endpoint list the host uses to reach coordinators.
func (r *runner) hostEndpoints(first string) string {
	if len(r.be.coordinators) == 1 {
		return first
	}
	var eps []string
	for i, svc := range r.be.coordinators {
		addr, err := r.c.hostPort(svc, 50051)
		if err == nil {
			eps = append(eps, r.be.nodeIDs[i]+"="+addr)
		}
	}
	return strings.Join(eps, ",")
}

// runFaults injects each fault at its time and heals it after its duration.
func (r *runner) runFaults(sched []Fault) {
	var wg sync.WaitGroup
	for _, f := range sched {
		if d := time.Until(r.start.Add(f.At)); d > 0 {
			time.Sleep(d)
		}
		heal := r.inject(f)
		if heal == nil {
			continue
		}
		wg.Add(1)
		go func(f Fault) {
			defer wg.Done()
			time.Sleep(time.Until(r.start.Add(f.At + f.Duration)))
			heal()
		}(f)
	}
	if d := time.Until(r.start.Add(r.cfg.Duration)); d > 0 {
		time.Sleep(d)
	}
	wg.Wait()
}

// inject applies f and returns the function that heals it (nil if there is
// nothing to heal or injection failed).
func (r *runner) inject(f Fault) func() {
	c := r.c
	pick := func(service string) (string, string) {
		ids, err := c.containers(service)
		if err != nil || len(ids) == 0 {
			return "", ""
		}
		return ids[f.Index%len(ids)], service
	}
	var id, svc string
	switch f.Target {
	case "worker":
		id, svc = pick("worker")
	case "coordinator":
		id, svc = pick(r.be.coordinators[f.Index%len(r.be.coordinators)])
	case "leader":
		var err error
		id, svc, err = r.leaderContainer()
		if err != nil {
			r.note(f.Kind, "leader", "", err)
			return nil
		}
	}
	target := f.target()
	if svc != "" && f.Target != "worker" {
		target = svc
	}
	act := func(action string, fn func() error) bool {
		err := fn()
		r.note(action, target, id, err)
		return err == nil
	}
	switch f.Kind {
	case KillWorker, KillCoord, KillLeader:
		if act(f.Kind, func() error { return c.kill(id) }) {
			return func() { act("heal:start", func() error { return c.start(id) }) }
		}
	case PauseWorker, PauseCoord, PauseLeader:
		if act(f.Kind, func() error { return c.pause(id) }) {
			return func() { act("heal:unpause", func() error { return c.unpause(id) }) }
		}
	case PartitionWorker, IsolateCoord:
		if act(f.Kind, func() error { return c.disconnect(id, "clientnet") }) {
			return func() { act("heal:reconnect", func() error { return c.connectAs(id, "clientnet", svc) }) }
		}
	case PartitionCoord, PartitionLeader:
		if act(f.Kind, func() error { return c.disconnect(id, "backnet") }) {
			return func() { act("heal:reconnect", func() error { return c.connectAs(id, "backnet", svc) }) }
		}
	case RestartCoord:
		act(f.Kind, func() error { return c.restart(id) })
	case Latency:
		target = fmt.Sprintf("workers<->coordinators %dms±%dms", f.LatencyMs, f.JitterMs)
		if act(f.Kind, func() error { return r.toxi.addLatency(f.LatencyMs, f.JitterMs) }) {
			return func() { act("heal:clear-latency", r.toxi.clearLatency) }
		}
	case RestartCluster:
		var ids []string
		for _, s := range r.be.coordinators {
			cs, _ := c.containers(s)
			ids = append(ids, cs...)
		}
		target = "all coordinators"
		if act(f.Kind, func() error {
			var errs []error
			for _, id := range ids {
				errs = append(errs, c.kill(id))
			}
			return errors.Join(errs...)
		}) {
			return func() {
				act("heal:start-all", func() error {
					var errs []error
					for _, id := range ids {
						errs = append(errs, c.start(id))
					}
					return errors.Join(errs...)
				})
			}
		}
	}
	return nil
}

// freezeWorkers SIGKILLs every worker so nothing can change job state
// after the final snapshot (in-flight calls become indeterminate), then
// tells the load generator to take it.
func (r *runner) freezeWorkers() {
	ids, err := r.c.containers("worker")
	if err == nil {
		for _, id := range ids {
			if r.c.state(id) == "paused" {
				_ = r.c.unpause(id)
			}
			if r.c.state(id) == "running" {
				if kerr := r.c.kill(id); kerr != nil {
					err = kerr
				}
			}
		}
	}
	r.note("freeze:kill-workers", "workers", "", err)
	if err := os.WriteFile(filepath.Join(r.dir, "frozen"), nil, 0o644); err != nil {
		r.note("freeze:signal", "load", "", err)
	}
}

// leaderContainer finds the current Raft leader's container: the node
// that reports itself as leader. If none does (an election is under way),
// it waits briefly.
func (r *runner) leaderContainer() (id, service string, err error) {
	if !r.be.raft {
		return "", "", errors.New("leader faults need the raft backend")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		for i, svc := range r.be.coordinators {
			addr, perr := r.c.hostPort(svc, 50051)
			if perr != nil {
				continue
			}
			if r.nodeIsLeader(addr) {
				ids, cerr := r.c.containers(svc)
				if cerr != nil || len(ids) == 0 {
					return "", "", fmt.Errorf("leader %s has no container: %v", r.be.nodeIDs[i], cerr)
				}
				return ids[0], svc, nil
			}
		}
		if time.Now().After(deadline) {
			return "", "", errors.New("no coordinator reports being leader")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (r *runner) nodeIsLeader(addr string) bool {
	conn, err := rpc.Dial(addr)
	if err != nil {
		return false
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	st, err := relayv1.NewClusterServiceClient(conn).ClusterStatus(ctx, &relayv1.ClusterStatusRequest{})
	return err == nil && st.State == "Leader"
}

// healAll returns every container to running and fully connected and
// removes latency, whatever state the faults left behind.
func (r *runner) healAll() {
	c := r.c
	services := append([]string{"worker"}, r.be.coordinators...)
	for _, svc := range services {
		ids, _ := c.containers(svc)
		for _, id := range ids {
			switch c.state(id) {
			case "paused":
				r.note("heal-all:unpause", svc, id, c.unpause(id))
			case "exited", "created", "dead":
				r.note("heal-all:start", svc, id, c.start(id))
			}
			nets := c.networks(id)
			want := []string{"clientnet"}
			if svc != "worker" {
				want = append(want, "backnet")
			}
			for _, n := range want {
				if !nets[n] {
					r.note("heal-all:reconnect", svc+" "+n, id, c.connectAs(id, n, svc))
				}
			}
		}
	}
	if err := r.toxi.clearLatency(); err != nil {
		r.note("heal-all:clear-latency", "toxiproxy", "", err)
	}
}

// connectAs reconnects a container under its service's DNS aliases, which
// a plain `docker network connect` would drop. On backnet a coordinator
// also answers to <service>-raft, the name its Raft peers use.
func (c *cluster) connectAs(id, network, service string) error {
	args := []string{"network", "connect", "--alias", service}
	if network == "backnet" {
		args = append(args, "--alias", service+"-raft")
	}
	err := c.docker(append(args, c.network(network), id)...)
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}

func checkMain(args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	dir := fs.String("dir", "", "a run's artifact directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" {
		return errors.New("--dir is required")
	}
	r := &runner{dir: *dir}
	rep, err := r.check()
	if err != nil {
		return err
	}
	if !rep.OK() {
		return errors.New("invariant violations found")
	}
	return nil
}

func readFinal(path string) (check.Final, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var f check.Final
	return f, json.Unmarshal(b, &f)
}

func writeJSON(path string, v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	_ = os.WriteFile(path, b, 0o644)
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// dialCoordinators connects to the coordinator endpoint list, following
// the leader.
func dialCoordinators(spec string, extra ...grpc.DialOption) (grpc.ClientConnInterface, func(), error) {
	conn, err := rpc.DialCluster(spec, extra...)
	if err != nil {
		return nil, nil, err
	}
	return conn, func() { conn.Close() }, nil
}
