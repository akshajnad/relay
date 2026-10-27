package main

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
)

var (
	errNoCluster = errors.New("no cluster is running: launch one from the Cluster tab")
	errNoDocker  = errors.New("docker controls are off (docker is unavailable, or the dashboard is watching an external cluster)")
	errBusy      = errors.New("another cluster operation is still running")
)

// attachAtStartup finds the cluster to watch when the dashboard starts.
func (d *Dash) attachAtStartup(ctx context.Context, mode string) error {
	if mode == "none" || d.docker == nil {
		return nil
	}
	if mode == "auto" {
		mode = ""
		for _, b := range []string{"raft", "redis"} {
			if d.projectRunning(ctx, layouts[b]) {
				mode = b
				break
			}
		}
		if mode == "" {
			d.log.Info("no Relay compose project is running; launch one from the dashboard")
			return nil
		}
	}
	l, ok := layouts[mode]
	if !ok {
		return fmt.Errorf("--attach must be auto, raft, redis or none, not %q", mode)
	}
	return d.attach(l, d.readConfig(ctx, l))
}

// projectRunning reports whether a layout's first coordinator is running.
func (d *Dash) projectRunning(ctx context.Context, l layout) bool {
	cs, err := d.docker.containers(ctx, l.Project)
	if err != nil {
		return false
	}
	for _, c := range cs {
		if c.Service == l.Coordinators[0].Service && c.State == "running" {
			return true
		}
	}
	return false
}

// readConfig recovers a running cluster's configuration from its
// containers, or returns nil if it cannot.
func (d *Dash) readConfig(ctx context.Context, l layout) *ClusterConfig {
	cs, err := d.docker.containers(ctx, l.Project)
	if err != nil {
		return nil
	}
	var coordEnv, workerEnv map[string]string
	workers := 0
	for _, c := range cs {
		switch {
		case c.Service == l.Coordinators[0].Service && coordEnv == nil:
			coordEnv, _ = d.docker.env(ctx, c.ID)
		case c.Service == "worker":
			workers++
			if workerEnv == nil {
				workerEnv, _ = d.docker.env(ctx, c.ID)
			}
		}
	}
	if coordEnv == nil {
		return nil
	}
	cfg := configFromEnv(l.Backend, coordEnv, workerEnv, workers)
	return &cfg
}

// beginOp claims the single slot for a long-running cluster operation.
func (d *Dash) beginOp(name string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.busy != "" {
		return false
	}
	d.busy, d.opLog = name, nil
	return true
}

func (d *Dash) endOp(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.busy = ""
	if err != nil {
		d.phase, d.phaseErr = "error", err.Error()
		d.opLog = append(d.opLog, "error: "+err.Error())
		d.addFeedLocked("error", "bad", err.Error(), false)
	}
}

func (d *Dash) setPhase(p string) {
	d.mu.Lock()
	d.phase, d.phaseErr = p, ""
	d.mu.Unlock()
}

// launch tears down any Relay compose cluster and starts a new one with
// cfg. It returns once the operation has started; progress streams into
// the op log.
func (d *Dash) launch(cfg ClusterConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if d.docker == nil {
		return errNoDocker
	}
	if !d.beginOp("launching") {
		return errBusy
	}
	go func() {
		err := d.doLaunch(cfg)
		d.endOp(err)
	}()
	return nil
}

func (d *Dash) doLaunch(cfg ClusterConfig) error {
	ctx := d.ctx
	d.load.stop()
	d.stopScenario()
	d.detach()
	d.setPhase("launching")
	d.addFeed("action", "info", fmt.Sprintf("Launching a %s cluster: %d workers × %d slots, lease %s, heartbeat %s × %d missed (dead after %v).",
		cfg.Backend, cfg.Workers, cfg.Concurrency, cfg.LeaseDuration, cfg.HeartbeatInterval, cfg.MissedHeartbeats, cfg.DeadAfter()), true)

	// Both layouts publish the same host ports, so whichever is up must go.
	for _, b := range []string{"raft", "redis"} {
		l := layouts[b]
		if cs, err := d.docker.containers(ctx, l.Project); err != nil || len(cs) == 0 {
			continue
		}
		d.logOp(fmt.Sprintf("$ docker compose -p %s down -v --remove-orphans", l.Project))
		if err := d.docker.stream(ctx, nil, d.logOp, d.docker.composeArgs(l, true, "down", "-v", "--remove-orphans", "--timeout", "5")...); err != nil {
			return err
		}
	}

	l := layouts[cfg.Backend]
	env := cfg.env()
	args := []string{"up", "-d", "--wait", "--scale", fmt.Sprintf("worker=%d", cfg.Workers)}
	if cfg.Rebuild || !d.docker.imageExists(ctx, "relay:dev") {
		d.logOp("building relay:dev (the first build takes a minute or two)")
		env = append(env, "RELAY_GIT_REVISION="+gitRevision(ctx))
		args = append(args, "--build")
	} else {
		args = append(args, "--no-build")
	}
	d.logOp("$ docker compose -p " + l.Project + " " + strings.Join(args, " "))
	if err := d.docker.stream(ctx, env, d.logOp, d.docker.composeArgs(l, cfg.Observability, args...)...); err != nil {
		return err
	}
	if err := d.attach(l, &cfg); err != nil {
		return err
	}
	d.refreshContainers(ctx)
	msg := "cluster is up"
	if cfg.Observability {
		msg += "; Grafana http://localhost:3000, Jaeger http://localhost:16686"
	}
	d.logOp(msg)
	return nil
}

// gitRevision mirrors the Makefile's RELAY_GIT_REVISION.
func gitRevision(ctx context.Context) string {
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	rev := strings.TrimSpace(string(out))
	if exec.CommandContext(ctx, "git", "diff", "--quiet", "HEAD").Run() != nil {
		rev += "-dirty"
	}
	return rev
}

// stop tears the watched compose cluster down, volumes included.
func (d *Dash) stop() error {
	if d.docker == nil {
		return errNoDocker
	}
	d.mu.Lock()
	t := d.target
	d.mu.Unlock()
	if t == nil || t.l.Project == "" {
		return errNoCluster
	}
	if !d.beginOp("stopping") {
		return errBusy
	}
	go func() {
		d.load.stop()
		d.stopScenario()
		d.detach()
		d.setPhase("stopping")
		d.addFeed("action", "info", "Stopping the cluster and deleting its volumes.", true)
		d.logOp(fmt.Sprintf("$ docker compose -p %s down -v --remove-orphans", t.l.Project))
		err := d.docker.stream(d.ctx, nil, d.logOp, d.docker.composeArgs(t.l, true, "down", "-v", "--remove-orphans", "--timeout", "5")...)
		if err == nil {
			d.setPhase("idle")
			d.logOp("cluster stopped")
		}
		d.endOp(err)
	}()
	return nil
}

// scale changes the number of worker containers without touching the rest.
// Removed workers get SIGTERM and drain their jobs first.
func (d *Dash) scale(workers int) error {
	if d.docker == nil {
		return errNoDocker
	}
	if workers < 0 || workers > 32 {
		return errors.New("workers must be between 0 and 32")
	}
	d.mu.Lock()
	t, cfg := d.target, d.cfg
	d.mu.Unlock()
	if t == nil || t.l.Project == "" {
		return errNoCluster
	}
	if cfg == nil {
		return errors.New("the running cluster's configuration is unknown; relaunch it from the dashboard")
	}
	if !d.beginOp("scaling") {
		return errBusy
	}
	next := *cfg
	next.Workers = workers
	go func() {
		d.addFeed("action", "info", fmt.Sprintf("Scaling workers from %d to %d.", cfg.Workers, workers), true)
		args := []string{"up", "-d", "--no-build", "--no-recreate", "--wait", "--scale", fmt.Sprintf("worker=%d", workers)}
		d.logOp("$ docker compose -p " + t.l.Project + " " + strings.Join(args, " "))
		err := d.docker.stream(d.ctx, next.env(), d.logOp, d.docker.composeArgs(t.l, next.Observability, args...)...)
		if err == nil {
			d.mu.Lock()
			if d.target == t {
				d.cfg = &next
			}
			d.mu.Unlock()
			d.logOp("scaled")
		}
		d.endOp(err)
		d.refreshContainers(d.ctx)
	}()
	return nil
}

// faultActions are the fault buttons, with what each one does to whom.
var faultActions = map[string]bool{"kill": true, "start": true, "restart": true, "pause": true, "resume": true, "isolate": true, "reconnect": true}

// fault applies one fault (or its healing) to a container of the watched
// project. ref is a container id, or "leader".
func (d *Dash) fault(ctx context.Context, action, ref string) (container, error) {
	if d.docker == nil {
		return container{}, errNoDocker
	}
	if !faultActions[action] {
		return container{}, fmt.Errorf("unknown action %q", action)
	}
	d.mu.Lock()
	t := d.target
	var c container
	found := false
	if t != nil {
		svc := ""
		if ref == "leader" && d.view != nil {
			for _, n := range d.view.Nodes {
				if n.ID == d.view.Leader || (n.ID == "" && n.Service == d.view.Leader) {
					svc = n.Service
				}
			}
		}
		for _, x := range d.containers {
			if x.ID == ref || (svc != "" && x.Service == svc) {
				c, found = x, true
			}
		}
	}
	d.mu.Unlock()
	if t == nil || t.l.Project == "" {
		return container{}, errNoCluster
	}
	if !found {
		return container{}, fmt.Errorf("no container %q in project %s", ref, t.l.Project)
	}
	if err := d.docker.fault(ctx, action, c, t.l.Project); err != nil {
		return c, err
	}
	d.addFeed("action", actionLevel(action), d.describeFault(action, c), true)
	d.refreshContainers(ctx)
	return c, nil
}

func actionLevel(action string) string {
	switch action {
	case "start", "resume", "reconnect":
		return "good"
	}
	return "warn"
}

// describeFault says what was done and what the engine should do about it.
func (d *Dash) describeFault(action string, c container) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	deadAfter := "a few seconds"
	if d.cfg != nil {
		deadAfter = d.cfg.DeadAfter().String()
	}
	who, role := c.Service, "worker"
	if c.Service == "worker" {
		who = "worker container " + c.ID
		for _, w := range d.viewWorkers() {
			if w.Host == c.ID && w.ID != "" {
				who = fmt.Sprintf("worker %s (container %s)", short(w.ID), c.ID)
			}
		}
	} else if t := d.target; t != nil && c.Service == t.l.StoreService {
		role = "store"
	} else {
		role = "follower"
		if d.view != nil {
			for _, n := range d.view.Nodes {
				if n.Service == c.Service {
					who = nodeName(n)
					if n.ID == d.view.Leader || !n.Replicated {
						role = "leader"
					}
				}
			}
		}
	}

	verb := map[string]string{"kill": "Killed", "start": "Started", "restart": "Restarted", "pause": "Paused",
		"resume": "Resumed", "isolate": "Cut off the network:", "reconnect": "Reconnected"}[action]
	what := fmt.Sprintf("%s %s.", verb, who)
	switch action {
	case "kill", "pause", "isolate":
		switch role {
		case "worker":
			what += fmt.Sprintf(" Its heartbeats stop; after %s of silence the failure detector declares it dead and requeues its jobs under new tokens.", deadAfter)
			if action == "pause" || action == "isolate" {
				what += " The process itself keeps its state, so when it comes back it is a zombie: its late reports will be fenced."
			}
		case "leader":
			if d.target != nil && d.target.l.Backend == "raft" {
				what += " The followers stop hearing from the leader and elect a new one after the election timeout; clients follow NOT_LEADER to it."
			} else {
				what += " Without a coordinator nothing is dispatched; workers retry until it is back. State lives in Redis, so nothing is lost."
			}
		case "follower":
			what += " The leader still has a majority, so the cluster keeps serving."
		case "store":
			what += " Every transition is an atomic Lua script on Redis, so the coordinator stalls until Redis is back; nothing is half-applied."
		}
	case "start", "restart":
		if role == "worker" {
			what += " It registers under a new worker id."
		} else if role != "store" {
			what += " It rejoins and catches up from the log or the store."
		}
	}
	return what
}

func (d *Dash) viewWorkers() []workerView {
	if d.view == nil {
		return nil
	}
	return d.view.Workers
}

// submit sends one job and starts following it.
func (d *Dash) submit(ctx context.Context, req *relayv1.SubmitJobRequest, origin string, force bool) (*relayv1.SubmitJobResponse, error) {
	c := d.jobClient()
	if c == nil {
		return nil, errNoCluster
	}
	sent := time.Now()
	resp, err := c.SubmitJob(ctx, req)
	if err != nil {
		return nil, err
	}
	if !resp.GetDuplicate() {
		d.tracker.add(resp.GetJobId(), req.GetType(), origin, sent, time.Now(), force)
	}
	return resp, nil
}
