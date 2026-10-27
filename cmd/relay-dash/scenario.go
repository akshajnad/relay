package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/store"
)

// scenarioInfo describes a guided scenario: a fault (or workload) the
// dashboard applies, then narrates the engine's response to as it watches.
type scenarioInfo struct {
	Name     string `json:"name"`
	Title    string `json:"title"`
	Blurb    string `json:"blurb"`
	Watch    string `json:"watch"`
	Backend  string `json:"backend"` // "" for any
	Disabled string `json:"disabled,omitempty"`
}

type scenarioDef struct {
	scenarioInfo
	run func(ctx context.Context, r *scenarioRun) error
}

var scenarioDefs = []scenarioDef{
	{scenarioInfo{Name: "zombie", Title: "Zombie worker vs. fencing tokens",
		Blurb: "Pause a busy worker until it is declared dead, then let it wake up and report on leases that now belong to someone else.",
		Watch: "The worker's heartbeat bar runs out, its jobs reappear on other lanes with larger tokens, then the stale-report counter ticks when the zombie reports."},
		runZombie},
	{scenarioInfo{Name: "worker-crash", Title: "Worker crash and recovery",
		Blurb: "Kill the busiest worker outright and time how long its jobs take to run elsewhere.",
		Watch: "Its lanes end in REQUEUED and the same jobs start on other workers a few seconds later; the container comes back as a new worker id."},
		runWorkerCrash},
	{scenarioInfo{Name: "leader-failover", Title: "Leader failover", Backend: "raft",
		Blurb: "Kill the Raft leader. The followers elect a new one; clients follow NOT_LEADER to it.",
		Watch: "A gap in throughput while there is no leader, a new term, and the old leader rejoining as a follower."},
		runLeaderFailover},
	{scenarioInfo{Name: "leader-partition", Title: "Partition the leader", Backend: "raft",
		Blurb: "Cut the leader off the network while it is still running. It cannot reach a quorum, so it steps down.",
		Watch: "A new leader on the majority side; when the partition heals, the old leader rejoins in the newer term."},
		runLeaderPartition},
	{scenarioInfo{Name: "redis-stall", Title: "Redis stall", Backend: "redis",
		Blurb: "Pause Redis for a few seconds. Every transition is one atomic Lua script there, so everything waits.",
		Watch: "Throughput drops to zero and resumes; whether any worker was declared dead because its heartbeats could not land."},
		runRedisStall},
	{scenarioInfo{Name: "retries", Title: "Retries, backoff and dead letters",
		Blurb: "Submit fail-n-times jobs: some succeed after retrying, some exhaust their attempts.",
		Watch: "Amber RETRY lease ends, growing gaps from exponential backoff, and purple DEAD jobs in the table."},
		runRetries},
}

// scenarioList returns the scenarios, marking those the backend cannot run.
func scenarioList(backend string) []scenarioInfo {
	out := make([]scenarioInfo, len(scenarioDefs))
	for i, s := range scenarioDefs {
		out[i] = s.scenarioInfo
		switch {
		case backend == "":
			out[i].Disabled = "no cluster"
		case s.Backend != "" && s.Backend != backend:
			out[i].Disabled = "needs the " + s.Backend + " backend"
		}
	}
	return out
}

type scenarioLine struct {
	At    int64  `json:"at"`
	Level string `json:"level"`
	Text  string `json:"text"`
}

type scenarioView struct {
	Name    string         `json:"name"`
	Title   string         `json:"title"`
	Running bool           `json:"running"`
	Lines   []scenarioLine `json:"lines"`
}

type scenarioRun struct {
	d      *Dash
	def    scenarioDef
	cancel context.CancelFunc

	mu      sync.Mutex
	lines   []scenarioLine
	running bool
	heals   []func()
}

func (r *scenarioRun) view() scenarioView {
	r.mu.Lock()
	defer r.mu.Unlock()
	return scenarioView{Name: r.def.Name, Title: r.def.Title, Running: r.running, Lines: append([]scenarioLine(nil), r.lines...)}
}

// say narrates a step, in the scenario panel and the activity feed.
func (r *scenarioRun) say(level, format string, args ...any) {
	text := fmt.Sprintf(format, args...)
	r.mu.Lock()
	r.lines = append(r.lines, scenarioLine{At: time.Now().UnixMilli(), Level: level, Text: text})
	r.mu.Unlock()
	r.d.addFeed("scenario", level, text, level != "info")
}

// onExit registers a healing step that runs however the scenario ends.
func (r *scenarioRun) onExit(f func()) {
	r.mu.Lock()
	r.heals = append(r.heals, f)
	r.mu.Unlock()
}

// faultUntilHealed applies a fault and arms its healing for when the
// scenario ends. The returned heal applies it early; either way it runs
// once.
func (r *scenarioRun) faultUntilHealed(ctx context.Context, action, heal, id string) (func(context.Context) error, error) {
	if _, err := r.d.fault(ctx, action, id); err != nil {
		return nil, err
	}
	var once sync.Once
	do := func(ctx context.Context) (err error) {
		once.Do(func() { _, err = r.d.fault(ctx, heal, id) })
		return err
	}
	r.onExit(func() { _ = do(context.Background()) })
	return do, nil
}

func (d *Dash) startScenario(name string) error {
	var def *scenarioDef
	for i := range scenarioDefs {
		if scenarioDefs[i].Name == name {
			def = &scenarioDefs[i]
		}
	}
	if def == nil {
		return fmt.Errorf("unknown scenario %q", name)
	}
	d.mu.Lock()
	backend := d.backendLocked()
	for _, s := range scenarioList(backend) {
		if s.Name == name && s.Disabled != "" {
			d.mu.Unlock()
			return fmt.Errorf("%s: %s", def.Title, s.Disabled)
		}
	}
	if d.scenario != nil && d.scenario.running {
		d.mu.Unlock()
		return errors.New("a scenario is already running")
	}
	if def.Name != "retries" && d.docker == nil {
		d.mu.Unlock()
		return errNoDocker
	}
	ctx, cancel := context.WithCancel(d.ctx)
	r := &scenarioRun{d: d, def: *def, cancel: cancel, running: true}
	d.scenario = r
	d.mu.Unlock()

	go func() {
		defer cancel()
		r.say("info", "Scenario: %s.", def.Title)
		err := def.run(ctx, r)
		r.mu.Lock()
		heals := r.heals
		r.mu.Unlock()
		for i := len(heals) - 1; i >= 0; i-- {
			heals[i]()
		}
		switch {
		case ctx.Err() != nil:
			r.say("warn", "Scenario stopped.")
		case err != nil:
			r.say("bad", "Scenario aborted: %v", err)
		default:
			r.say("good", "Scenario complete.")
		}
		r.mu.Lock()
		r.running = false
		r.mu.Unlock()
	}()
	return nil
}

func (d *Dash) stopScenario() {
	d.mu.Lock()
	r := d.scenario
	d.mu.Unlock()
	if r != nil {
		r.cancel()
	}
}

// Helpers the scenarios share.

// latest returns the most recent snapshot.
func (r *scenarioRun) latest() *snapshot {
	r.d.mu.Lock()
	defer r.d.mu.Unlock()
	return r.d.view
}

// waitFor polls the latest snapshot until cond holds, timeout passes, or
// the scenario is stopped.
func (r *scenarioRun) waitFor(ctx context.Context, timeout time.Duration, cond func(*snapshot) bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s := r.latest(); s != nil && cond(s) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(150 * time.Millisecond):
		}
	}
	return false
}

func (r *scenarioRun) sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// ensureLoad starts the load generator with cfg unless it is already
// running, so a fault lands on a busy cluster.
func (r *scenarioRun) ensureLoad(cfg LoadConfig) {
	if r.d.load.view().Running {
		return
	}
	r.d.load.start(r.d.ctx, cfg, r.d.submit)
	r.say("info", "Started load so there is work in flight: %s, sleeps of %d–%d ms. It keeps running afterwards; stop it in the Workload tab.",
		cfg.String(), cfg.SleepMinMs, cfg.SleepMaxMs)
}

// slots is the cluster's total worker capacity.
func slots(s *snapshot) int {
	n := 0
	for _, w := range s.Workers {
		if w.State == store.WorkerAlive {
			n += int(w.Concurrency)
		}
	}
	return n
}

// busiestWorker picks the live, running worker holding the most leases.
func busiestWorker(s *snapshot) (workerView, bool) {
	var best workerView
	found := false
	for _, w := range s.Workers {
		if w.State != store.WorkerAlive || w.Container == nil || w.Container.State != "running" || w.Container.Isolated {
			continue
		}
		if !found || w.Active > best.Active || (w.Active == best.Active && len(w.Held) > len(best.Held)) {
			best, found = w, true
		}
	}
	return best, found
}

func workerByID(s *snapshot, id string) (workerView, bool) {
	for _, w := range s.Workers {
		if w.ID == id {
			return w, true
		}
	}
	return workerView{}, false
}

func workerByHost(s *snapshot, host string) (workerView, bool) {
	for _, w := range s.Workers {
		if w.Host == host {
			return w, true
		}
	}
	return workerView{}, false
}

func nodeByService(s *snapshot, svc string) (nodeView, bool) {
	for _, n := range s.Nodes {
		if n.Service == svc {
			return n, true
		}
	}
	return nodeView{}, false
}

func describeHeld(h []heldJob) string {
	if len(h) == 0 {
		return ""
	}
	parts := make([]string, 0, len(h))
	for _, j := range h[:min(len(h), 4)] {
		parts = append(parts, fmt.Sprintf("%s (token %d)", short(j.Job), j.Token))
	}
	s := strings.Join(parts, ", ")
	if len(h) > 4 {
		s += fmt.Sprintf(" and %d more", len(h)-4)
	}
	return ": " + s
}

func (r *scenarioRun) deadAfter() time.Duration {
	r.d.mu.Lock()
	defer r.d.mu.Unlock()
	if r.d.cfg != nil {
		return r.d.cfg.DeadAfter()
	}
	return 3 * time.Second
}

// longJobs is a load whose jobs outlast a fault, sized to keep about 60% of
// the slots busy.
func longJobs(s *snapshot, minMs, maxMs int) LoadConfig {
	cfg := defaultLoadConfig()
	cfg.SleepMinMs, cfg.SleepMaxMs = minMs, maxMs
	avg := float64(minMs+maxMs) / 2000
	cfg.Rate = math.Round(max(2, 0.6*float64(max(slots(s), 4))/avg))
	return cfg
}

// The scenarios.

func runZombie(ctx context.Context, r *scenarioRun) error {
	s := r.latest()
	if s == nil {
		return errNoCluster
	}
	r.ensureLoad(longJobs(s, 2000, 4000))
	var w workerView
	if !r.waitFor(ctx, 15*time.Second, func(s *snapshot) bool {
		var ok bool
		w, ok = busiestWorker(s)
		return ok && w.Active > 0
	}) {
		return errors.New("no running worker picked up a job within 15s")
	}
	resume, err := r.faultUntilHealed(ctx, "pause", "resume", w.Host)
	if err != nil {
		return err
	}
	paused := time.Now()
	r.say("warn", "Paused worker %s while it holds %d lease(s)%s. A paused process sends nothing but keeps all its state: the textbook zombie.",
		short(w.ID), w.Active, describeHeld(w.Held))

	if !r.waitFor(ctx, r.deadAfter()+10*time.Second, func(s *snapshot) bool {
		x, ok := workerByID(s, w.ID)
		return ok && x.State == store.WorkerDead
	}) {
		return errors.New("the worker was not declared dead in time")
	}
	r.say("bad", "After %.1fs of silence the failure detector declared %s dead and revoked its leases in the same atomic step. Each job went back to PENDING and will be leased again under a larger fencing token.",
		time.Since(paused).Seconds(), short(w.ID))

	if err := r.sleep(ctx, 2*time.Second); err != nil {
		return err
	}
	if moved := r.d.tracker.reassigned(w.Held); moved != "" {
		r.say("info", "Already re-leased elsewhere: %s.", moved)
	}
	before := r.latest().Totals["stale"]
	if err := resume(ctx); err != nil {
		return err
	}
	r.say("warn", "Resumed the zombie. It still believes it holds those leases; as its jobs finish it reports them under the old tokens.")

	if r.waitFor(ctx, 12*time.Second, func(s *snapshot) bool { return s.Totals["stale"] > before }) {
		if err := r.sleep(ctx, 2*time.Second); err != nil {
			return err
		}
		n := r.latest().Totals["stale"] - before
		r.say("good", "The store rejected %s carrying superseded tokens (ABORTED). The zombie's results were never written, and each job still has exactly one terminal transition.",
			plural(n, "report", "reports"))
	} else {
		r.say("info", "No stale report within 12s: the zombie's jobs are still running. When they finish, their reports carry the old tokens and will be rejected the same way.")
	}
	return nil
}

func runWorkerCrash(ctx context.Context, r *scenarioRun) error {
	s := r.latest()
	if s == nil {
		return errNoCluster
	}
	r.ensureLoad(longJobs(s, 1000, 3000))
	var w workerView
	if !r.waitFor(ctx, 15*time.Second, func(s *snapshot) bool {
		var ok bool
		w, ok = busiestWorker(s)
		return ok && w.Active > 0
	}) {
		return errors.New("no running worker picked up a job within 15s")
	}
	start, err := r.faultUntilHealed(ctx, "kill", "start", w.Host)
	if err != nil {
		return err
	}
	killed := time.Now()
	r.say("warn", "Killed worker %s with %d job(s) in flight%s.", short(w.ID), w.Active, describeHeld(w.Held))

	if !r.waitFor(ctx, r.deadAfter()+10*time.Second, func(s *snapshot) bool {
		x, ok := workerByID(s, w.ID)
		return ok && x.State == store.WorkerDead
	}) {
		return errors.New("the worker was not declared dead in time")
	}
	r.say("bad", "Declared dead %.1fs after the kill: the %s window runs from its last heartbeat, plus up to one detector tick. Its leases were revoked and the jobs requeued in that same step.",
		time.Since(killed).Seconds(), r.deadAfter())
	if err := r.sleep(ctx, 1500*time.Millisecond); err != nil {
		return err
	}
	if moved := r.d.tracker.reassigned(w.Held); moved != "" {
		r.say("info", "Running again elsewhere: %s.", moved)
	}

	if err := r.sleep(ctx, 2*time.Second); err != nil {
		return err
	}
	if err := start(ctx); err != nil {
		return err
	}
	if r.waitFor(ctx, 20*time.Second, func(s *snapshot) bool {
		x, ok := workerByHost(s, w.Host)
		return ok && x.State == store.WorkerAlive && x.ID != w.ID
	}) {
		x, _ := workerByHost(r.latest(), w.Host)
		r.say("good", "The container is back as worker %s. Ids are never reused, so the dead %s can never hold a lease again.", short(x.ID), short(w.ID))
	}
	return nil
}

func raftLeader(s *snapshot) (nodeView, bool) {
	for _, n := range s.Nodes {
		if n.ID == s.Leader && n.Reachable && n.Container != nil {
			return n, true
		}
	}
	return nodeView{}, false
}

func runLeaderFailover(ctx context.Context, r *scenarioRun) error {
	return leaderFault(ctx, r, "kill", "start")
}

func runLeaderPartition(ctx context.Context, r *scenarioRun) error {
	return leaderFault(ctx, r, "isolate", "reconnect")
}

func leaderFault(ctx context.Context, r *scenarioRun, action, heal string) error {
	s := r.latest()
	if s == nil {
		return errNoCluster
	}
	r.ensureLoad(longJobs(s, 100, 600))
	var old nodeView
	if !r.waitFor(ctx, 10*time.Second, func(s *snapshot) bool {
		var ok bool
		old, ok = raftLeader(s)
		return ok
	}) {
		return errors.New("there is no reachable leader to fault")
	}
	if err := r.sleep(ctx, 2*time.Second); err != nil {
		return err
	}
	healFn, err := r.faultUntilHealed(ctx, action, heal, old.Container.ID)
	if err != nil {
		return err
	}
	at := time.Now()
	if action == "isolate" {
		r.say("warn", "Cut leader %s (term %d) off the network. It is still running, but it cannot reach a quorum: after its leader lease times out it steps down, so it can never serve a stale read or accept a write alone.", old.ID, old.Term)
	} else {
		r.say("warn", "Killed leader %s (term %d). Writes stop until the followers notice and elect a new leader.", old.ID, old.Term)
	}

	var next nodeView
	if !r.waitFor(ctx, 30*time.Second, func(s *snapshot) bool {
		var ok bool
		next, ok = raftLeader(s)
		return ok && next.ID != old.ID
	}) {
		return errors.New("no new leader was elected within 30s")
	}
	r.say("good", "%s won the election for term %d, %.1fs after the fault. Lease deadlines are soft state the new leader did not inherit, so it gave every lease a full grace period instead of guessing.",
		next.ID, next.Term, time.Since(at).Seconds())

	if err := r.sleep(ctx, 6*time.Second); err != nil {
		return err
	}
	if err := healFn(ctx); err != nil {
		return err
	}
	if r.waitFor(ctx, 30*time.Second, func(s *snapshot) bool {
		n, ok := nodeByService(s, old.Service)
		return ok && n.Reachable && n.State == "Follower" && n.AppliedIndex > 0
	}) {
		n, _ := nodeByService(r.latest(), old.Service)
		r.say("good", "%s rejoined as a follower in term %d and caught up its log (applied index %d).", old.ID, n.Term, n.AppliedIndex)
	} else {
		r.say("info", "%s has not rejoined as a follower yet; it may still be starting.", old.ID)
	}
	return nil
}

func runRedisStall(ctx context.Context, r *scenarioRun) error {
	s := r.latest()
	if s == nil || s.Store == nil {
		return errors.New("no Redis container in this cluster")
	}
	r.ensureLoad(longJobs(s, 100, 600))
	if err := r.sleep(ctx, 2*time.Second); err != nil {
		return err
	}
	id := s.Store.ID
	before := r.latest().Totals["workers_expired"]
	resume, err := r.faultUntilHealed(ctx, "pause", "resume", id)
	if err != nil {
		return err
	}
	r.say("warn", "Paused Redis. Submits, acquires, heartbeats and reports all wait on it now.")
	if err := r.sleep(ctx, 4*time.Second); err != nil {
		return err
	}
	if err := resume(ctx); err != nil {
		return err
	}
	r.say("good", "Resumed Redis after 4s. Each queued Lua script runs atomically in order; no transition was half-applied.")
	if err := r.sleep(ctx, 5*time.Second); err != nil {
		return err
	}
	if n := r.latest().Totals["workers_expired"] - before; n > 0 {
		r.say("warn", "The stall was longer than the failure-detection window, so %s declared dead as Redis came back: their heartbeats had not landed. Their jobs were requeued, and fencing makes the overlap safe.",
			plural(n, "worker was", "workers were"))
	} else {
		r.say("good", "No worker was declared dead: heartbeats resumed before the detector's window ran out.")
	}
	return nil
}

func runRetries(ctx context.Context, r *scenarioRun) error {
	type batch struct{ count, n int }
	batches := []batch{{12, 1}, {8, 2}, {6, 5}}
	var ids []string
	for _, b := range batches {
		for range b.count {
			payload, _ := json.Marshal(map[string]int{"n": b.n})
			sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			resp, err := r.d.submit(sctx, &relayv1.SubmitJobRequest{Type: "fail-n-times", Payload: payload, MaxAttempts: 3}, "scenario", true)
			cancel()
			if err != nil {
				return err
			}
			ids = append(ids, resp.GetJobId())
		}
	}
	r.say("info", "Submitted 26 fail-n-times jobs with max attempts 3: 12 fail once, 8 fail twice, 6 fail five times. Each failed attempt ends its lease with RETRY and waits out an exponential backoff before the next lease, under a new token.")
	var counts map[string]int
	ok := r.waitFor(ctx, 90*time.Second, func(*snapshot) bool {
		counts = r.d.tracker.statusCounts(ids)
		return counts[store.StatusSucceeded]+counts[store.StatusFailed]+counts[store.StatusDead] == len(ids)
	})
	if !ok && ctx.Err() != nil {
		return ctx.Err()
	}
	r.say("good", "%d succeeded after retrying, %d were dead-lettered after 3 attempts, %d still in flight. The 6 that needed 6 attempts could never make it, and no job ran past its max.",
		counts[store.StatusSucceeded], counts[store.StatusDead], len(ids)-counts[store.StatusSucceeded]-counts[store.StatusDead]-counts[store.StatusFailed])
	return nil
}

// reassigned describes where the given leases' jobs are leased now.
func (t *tracker) reassigned(held []heldJob) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var parts []string
	for _, h := range held {
		j := t.jobs[h.Job]
		if j == nil || j.Job == nil {
			continue
		}
		if tok := j.Job.GetLeaseToken(); tok != h.Token && j.Job.GetStatus() == relayv1.JobStatus_JOB_STATUS_LEASED {
			parts = append(parts, fmt.Sprintf("%s token %d → %d on worker %s", short(h.Job), h.Token, tok, short(j.Job.GetLeaseOwner())))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}

// statusCounts counts the followed jobs among ids by status.
func (t *tracker) statusCounts(ids []string) map[string]int {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := map[string]int{}
	for _, id := range ids {
		if j := t.jobs[id]; j != nil && j.Job != nil {
			out[statusName(j.Job.GetStatus())]++
		}
	}
	return out
}
