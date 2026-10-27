package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	tickInterval = 500 * time.Millisecond
	// seriesLen is five minutes of ticks.
	seriesLen   = 600
	feedLen     = 300
	opLogLen    = 400
	spanWindow  = 60 * time.Second
	maxSpans    = 1200
	recentJobs  = 40
	nodeTimeout = 400 * time.Millisecond
)

// Dash is the dashboard's state: the cluster it watches, what it last saw,
// and the activity (load, scenario, launch) it is running.
type Dash struct {
	ctx     context.Context
	log     *slog.Logger
	docker  *docker // nil when docker controls are off
	hub     *hub
	tracker *tracker
	load    *loadGen
	httpc   *http.Client

	mu         sync.Mutex
	target     *target
	phase      string // idle, launching, running, stopping, error
	phaseErr   string
	busy       string // the long-running cluster operation in progress, if any
	opLog      []string
	cfg        *ClusterConfig // the watched cluster's configuration, when known
	containers []container
	metrics    *metricState
	series     []point
	feed       []feedItem
	feedSeq    int64
	sentSeq    int64
	prev       *seen
	view       *snapshot // the last snapshot, for scenarios to wait on
	scenario   *scenarioRun
}

func newDash(ctx context.Context, log *slog.Logger) *Dash {
	return &Dash{
		ctx: ctx, log: log, hub: newHub(), tracker: newTracker(), load: newLoadGen(),
		httpc:   &http.Client{Timeout: 2 * time.Second},
		phase:   "idle",
		metrics: newMetricState(),
	}
}

func (d *Dash) jobClient() relayv1.JobServiceClient {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.target == nil {
		return nil
	}
	return d.target.jobs
}

// attach starts watching a cluster, replacing whatever was watched before.
func (d *Dash) attach(l layout, cfg *ClusterConfig) error {
	t, err := dialTarget(l)
	if err != nil {
		return err
	}
	d.tracker.reset()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.target != nil {
		d.target.close()
	}
	d.target, d.cfg, d.phase, d.phaseErr = t, cfg, "running", ""
	d.metrics, d.series, d.prev = newMetricState(), nil, nil
	where := l.Project + " (docker compose)"
	if l.Project == "" {
		where = l.grpcSpec()
	}
	d.addFeedLocked("cluster", "info", fmt.Sprintf("Watching %s: %d coordinator(s), %s backend.", where, len(l.Coordinators), l.Backend), false)
	return nil
}

// detach stops watching the current cluster.
func (d *Dash) detach() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.target != nil {
		d.target.close()
		d.target = nil
	}
	d.containers, d.prev, d.cfg = nil, nil, nil
}

func (d *Dash) shutdown() {
	d.load.stop()
	d.stopScenario()
	d.detach()
}

func (d *Dash) runPoller(ctx context.Context) {
	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		d.mu.Lock()
		t := d.target
		d.mu.Unlock()
		var obs *observation
		if t != nil {
			obs = t.observe(ctx, d.httpc)
		}
		jobs := d.tracker.view(time.Now(), spanWindow, maxSpans, recentJobs)
		clock := d.tracker.offset()

		d.mu.Lock()
		if t != d.target {
			d.mu.Unlock() // the cluster was swapped mid-poll; this observation is stale
			continue
		}
		snap := d.integrateLocked(obs, jobs, clock)
		d.mu.Unlock()
		d.hub.publish("tick", snap)
	}
}

// runContainerWatch refreshes the compose project's container list, which
// maps workers to containers and drives the fault buttons.
func (d *Dash) runContainerWatch(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		d.refreshContainers(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (d *Dash) refreshContainers(ctx context.Context) {
	d.mu.Lock()
	t := d.target
	d.mu.Unlock()
	if t == nil || t.l.Project == "" {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	cs, err := d.docker.containers(cctx, t.l.Project)
	cancel()
	if err != nil {
		return
	}
	d.mu.Lock()
	if d.target == t {
		d.containers = cs
	}
	d.mu.Unlock()
}

// observation is one poll of the cluster.
type observation struct {
	at         time.Time
	nodes      []nodeObs
	workers    []*relayv1.Worker
	workersErr error
}

type nodeObs struct {
	spec      coordSpec
	status    *relayv1.ClusterStatusResponse
	statusErr error
	scrape    *scrape
}

func (t *target) observe(ctx context.Context, httpc *http.Client) *observation {
	obs := &observation{at: time.Now(), nodes: make([]nodeObs, len(t.l.Coordinators))}
	var wg sync.WaitGroup
	for i, c := range t.l.Coordinators {
		obs.nodes[i].spec = c
		wg.Add(2)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, nodeTimeout)
			defer cancel()
			obs.nodes[i].status, obs.nodes[i].statusErr = relayv1.NewClusterServiceClient(t.nodes[i]).
				ClusterStatus(cctx, &relayv1.ClusterStatusRequest{})
		}()
		go func() {
			defer wg.Done()
			if c.Metrics == "" {
				return
			}
			cctx, cancel := context.WithTimeout(ctx, nodeTimeout)
			defer cancel()
			obs.nodes[i].scrape, _ = fetchScrape(cctx, httpc, c.Metrics)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		cctx, cancel := context.WithTimeout(ctx, 2*nodeTimeout)
		defer cancel()
		resp, err := t.workers.ListWorkers(cctx, &relayv1.ListWorkersRequest{})
		obs.workers, obs.workersErr = resp.GetWorkers(), err
	}()
	wg.Wait()
	return obs
}

// seen is what the previous tick saw, for turning changes into feed items.
type seen struct {
	leader    string
	term      uint64
	reachable map[string]bool
	workers   map[string]string // worker id -> state
	hosts     map[string]string // hostname -> current worker id
}

type nodeView struct {
	ID           string     `json:"id"`
	Service      string     `json:"service"`
	GRPC         string     `json:"grpc"`
	Reachable    bool       `json:"reachable"`
	Replicated   bool       `json:"replicated"`
	State        string     `json:"state"`
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	CommitIndex  uint64     `json:"commit_index"`
	AppliedIndex uint64     `json:"applied_index"`
	LastIndex    uint64     `json:"last_index"`
	Serving      bool       `json:"serving"`
	MetricsOK    bool       `json:"metrics_ok"`
	Container    *container `json:"container,omitempty"`
}

type workerView struct {
	Host          string     `json:"host"`
	ID            string     `json:"id"`
	State         string     `json:"state"` // ALIVE, DEAD, or STARTING before it registers
	Concurrency   int32      `json:"concurrency"`
	Active        int32      `json:"active"`
	LastHeartbeat int64      `json:"last_heartbeat"` // dashboard ms
	RegisteredAt  int64      `json:"registered_at"`
	Previous      []string   `json:"previous"` // earlier ids of this host, all declared dead
	Container     *container `json:"container,omitempty"`
	Held          []heldJob  `json:"held"`
}

type gauges struct {
	Pending *float64 `json:"pending"`
	Leased  *float64 `json:"leased"`
}

// point is one sample of the time series the charts draw.
type point struct {
	T           int64    `json:"t"`
	Submitted   float64  `json:"submitted"`
	Succeeded   float64  `json:"succeeded"`
	Retried     float64  `json:"retried"`
	Requeued    float64  `json:"requeued"`
	Failed      float64  `json:"failed"` // failed + dead-lettered
	Stale       float64  `json:"stale"`
	Pending     *float64 `json:"pending"`
	Leased      *float64 `json:"leased"`
	DispatchP50 *float64 `json:"dispatch_p50"`
	DispatchP99 *float64 `json:"dispatch_p99"`
	QueueP50    *float64 `json:"queue_p50"`
	QueueP99    *float64 `json:"queue_p99"`
	CommitP50   *float64 `json:"commit_p50"`
	CommitP99   *float64 `json:"commit_p99"`
	AcquireP99  *float64 `json:"acquire_p99"`
}

type snapshot struct {
	Now         int64              `json:"now"`
	Phase       string             `json:"phase"`
	PhaseErr    string             `json:"phase_error,omitempty"`
	Busy        string             `json:"busy,omitempty"`
	OpLog       []string           `json:"op_log"`
	Backend     string             `json:"backend"`
	Project     string             `json:"project"`
	Docker      bool               `json:"docker"`
	Config      *ClusterConfig     `json:"config"`
	DeadAfterMs int64              `json:"dead_after_ms"`
	LeaseMs     int64              `json:"lease_ms"`
	Leader      string             `json:"leader"`
	Term        uint64             `json:"term"`
	Nodes       []nodeView         `json:"nodes"`
	Store       *container         `json:"store,omitempty"`
	Workers     []workerView       `json:"workers"`
	WorkersOK   bool               `json:"workers_ok"`
	Totals      map[string]float64 `json:"totals"`
	Rates       map[string]float64 `json:"rates"`
	Gauges      gauges             `json:"gauges"`
	Latency     map[string]latency `json:"latency"`
	Point       *point             `json:"point,omitempty"`
	Jobs        trackerView        `json:"jobs"`
	Load        loadView           `json:"load"`
	Scenario    *scenarioView      `json:"scenario,omitempty"`
	Scenarios   []scenarioInfo     `json:"scenarios"`
	Feed        []feedItem         `json:"feed"`
}

// integrateLocked folds one observation into the dashboard's state and
// returns the snapshot to publish.
func (d *Dash) integrateLocked(obs *observation, jobs trackerView, clock *clockOffset) *snapshot {
	now := time.Now()
	s := &snapshot{
		Now: now.UnixMilli(), Phase: d.phase, PhaseErr: d.phaseErr, Busy: d.busy, OpLog: append([]string(nil), d.opLog...),
		Docker: d.docker != nil, Jobs: jobs, Load: d.load.view(), Scenarios: scenarioList(d.backendLocked()),
		Totals: map[string]float64{}, Rates: map[string]float64{}, Latency: map[string]latency{},
	}
	if d.scenario != nil {
		v := d.scenario.view()
		s.Scenario = &v
	}
	if d.cfg != nil {
		cfg := *d.cfg // published after the lock is released, so not shared
		s.Config = &cfg
		s.DeadAfterMs = d.cfg.DeadAfter().Milliseconds()
		s.LeaseMs = d.cfg.dur(d.cfg.LeaseDuration).Milliseconds()
	}
	if obs == nil || d.target == nil {
		s.Feed = d.newFeedLocked()
		d.view = s
		return s
	}
	l := d.target.l
	s.Backend, s.Project = l.Backend, l.Project
	byService := map[string]*container{}
	for i := range d.containers {
		c := &d.containers[i]
		byService[c.Service] = c // a single-replica service; workers are matched by hostname below
	}
	if l.StoreService != "" {
		s.Store = byService[l.StoreService]
	}

	// Coordinators.
	scrapes := map[string]*scrape{}
	var leaderView *nodeView
	for _, n := range obs.nodes {
		v := nodeView{ID: n.spec.ID, Service: n.spec.Service, GRPC: n.spec.GRPC, MetricsOK: n.scrape != nil,
			Container: byService[n.spec.Service]}
		switch {
		case n.statusErr == nil:
			st := n.status
			v.Reachable, v.Replicated = true, true
			v.State, v.Term, v.LeaderID = st.GetState(), st.GetTerm(), st.GetLeaderId()
			v.CommitIndex, v.AppliedIndex, v.LastIndex = st.GetCommitIndex(), st.GetAppliedIndex(), st.GetLastIndex()
		case status.Code(n.statusErr) == codes.Unimplemented:
			// A Redis-backed coordinator: no Raft, and it always serves.
			v.Reachable, v.State, v.LeaderID = true, "Serving", n.spec.ID
		default:
			v.State = "Unreachable"
		}
		if n.scrape != nil {
			scrapes[n.spec.Service] = n.scrape
			v.Serving = n.scrape.values["relay_is_leader"] == 1
		}
		s.Nodes = append(s.Nodes, v)
	}
	for i := range s.Nodes {
		v := &s.Nodes[i]
		isLeader := v.State == "Leader" || (!v.Replicated && v.Reachable)
		if isLeader && (leaderView == nil || v.Term > leaderView.Term) {
			leaderView = v
		}
	}
	if leaderView != nil {
		s.Leader, s.Term = leaderView.ID, leaderView.Term
		if s.Leader == "" {
			s.Leader = leaderView.Service
		}
	}

	// Metrics.
	d.metrics.observe(obs.at, scrapes)
	for k, v := range d.metrics.totals {
		s.Totals[k] = v
	}
	s.Rates = d.metrics.rates()
	for key := range histograms {
		s.Latency[key] = d.metrics.latency(key)
	}
	gaugeFrom := func(sc *scrape) {
		if sc == nil || s.Gauges.Pending != nil {
			return
		}
		if p, ok := sc.values["relay_queue_depth"]; ok && p >= 0 {
			l := sc.values["relay_leases_active"]
			s.Gauges = gauges{Pending: &p, Leased: &l}
		}
	}
	if leaderView != nil {
		gaugeFrom(scrapes[leaderView.Service])
	}
	for _, n := range obs.nodes {
		gaugeFrom(n.scrape)
	}

	// Workers.
	s.WorkersOK = obs.workersErr == nil
	workers := obs.workers
	if obs.workersErr != nil && d.view != nil {
		// No leader to ask (an election, say): keep showing the last list.
		s.Workers = d.view.Workers
	} else {
		s.Workers = d.workerViewsLocked(workers, jobs, clock)
	}

	// Series.
	r := s.Rates
	p := &point{T: s.Now, Submitted: r["submitted"], Succeeded: r["succeeded"], Retried: r["retried"],
		Requeued: r["requeued"], Failed: r["failed"] + r["dead"], Stale: r["stale"],
		Pending: s.Gauges.Pending, Leased: s.Gauges.Leased,
		DispatchP50: s.Latency["dispatch"].P50, DispatchP99: s.Latency["dispatch"].P99,
		QueueP50: s.Latency["queue_wait"].P50, QueueP99: s.Latency["queue_wait"].P99,
		CommitP50: s.Latency["raft_commit"].P50, CommitP99: s.Latency["raft_commit"].P99,
		AcquireP99: s.Latency["acquire"].P99}
	s.Point = p
	d.series = append(d.series, *p)
	if len(d.series) > seriesLen {
		d.series = d.series[len(d.series)-seriesLen:]
	}

	d.diffLocked(s, workers, obs.workersErr == nil)
	s.Feed = d.newFeedLocked()
	d.view = s
	return s
}

// workerViewsLocked groups workers by hostname. In the compose files a
// worker's hostname is its container's short id, and a container that was
// declared dead re-registers under a new id, so one card per host shows a
// worker's whole life: the current id, and the ids it was declared dead
// under.
func (d *Dash) workerViewsLocked(ws []*relayv1.Worker, jobs trackerView, clock *clockOffset) []workerView {
	byHost := map[string][]*relayv1.Worker{}
	for _, w := range ws {
		byHost[w.GetHostname()] = append(byHost[w.GetHostname()], w)
	}
	view := func(host string, c *container) workerView {
		v := workerView{Host: host, State: "STARTING", Container: c, Previous: []string{}, Held: []heldJob{}}
		incs := byHost[host]
		sort.Slice(incs, func(i, j int) bool { return incs[i].GetRegisteredAtMs() < incs[j].GetRegisteredAtMs() })
		for i, w := range incs {
			if i < len(incs)-1 {
				v.Previous = append(v.Previous, w.GetId())
				continue
			}
			v.ID, v.State, v.Concurrency, v.Active = w.GetId(), w.GetState(), w.GetConcurrency(), w.GetActiveJobs()
			v.LastHeartbeat, v.RegisteredAt = clock.local(w.GetLastHeartbeatMs()), clock.local(w.GetRegisteredAtMs())
			if h := jobs.Held[w.GetId()]; h != nil {
				v.Held = h
			}
		}
		return v
	}

	var out []workerView
	if d.docker != nil && d.target.l.Project != "" {
		for i := range d.containers {
			if c := &d.containers[i]; c.Service == "worker" {
				out = append(out, view(c.ID, c))
			}
		}
		return out
	}
	// An external cluster: every host with a live worker, then the most
	// recently registered dead ones.
	hosts := make([]string, 0, len(byHost))
	for h := range byHost {
		hosts = append(hosts, h)
	}
	for _, h := range hosts {
		out = append(out, view(h, nil))
	}
	sort.Slice(out, func(i, j int) bool {
		if (out[i].State == store.WorkerAlive) != (out[j].State == store.WorkerAlive) {
			return out[i].State == store.WorkerAlive
		}
		return out[i].RegisteredAt > out[j].RegisteredAt
	})
	return out[:min(len(out), 24)]
}

// diffLocked turns what changed since the previous tick into feed items.
func (d *Dash) diffLocked(s *snapshot, ws []*relayv1.Worker, workersOK bool) {
	cur := &seen{leader: s.Leader, term: s.Term, reachable: map[string]bool{}, workers: map[string]string{}, hosts: map[string]string{}}
	for _, n := range s.Nodes {
		cur.reachable[n.Service] = n.Reachable
	}
	for _, w := range ws {
		cur.workers[w.GetId()] = w.GetState()
	}
	for _, w := range s.Workers {
		if w.ID != "" {
			cur.hosts[w.Host] = w.ID
		}
	}
	prev := d.prev
	if !workersOK && prev != nil {
		cur.workers, cur.hosts = prev.workers, prev.hosts
	}
	d.prev = cur
	if prev == nil {
		return
	}

	replicated := false
	for _, n := range s.Nodes {
		replicated = replicated || n.Replicated
	}
	for _, n := range s.Nodes {
		was, ok := prev.reachable[n.Service]
		switch {
		case ok && was && !n.Reachable:
			d.addFeedLocked("cluster", "warn", fmt.Sprintf("%s stopped answering.", nodeName(n)), true)
		case ok && !was && n.Reachable:
			d.addFeedLocked("cluster", "good", fmt.Sprintf("%s is answering again (%s).", nodeName(n), n.State), true)
		}
	}
	if replicated && cur.leader != prev.leader {
		switch {
		case cur.leader == "":
			d.addFeedLocked("cluster", "warn", fmt.Sprintf("No leader: %s is gone and an election is under way. Writes wait until one wins.", prev.leader), true)
		case prev.leader == "":
			d.addFeedLocked("cluster", "good", fmt.Sprintf("%s won the election for term %d and now serves every client; the others redirect to it.", cur.leader, cur.term), true)
		default:
			d.addFeedLocked("cluster", "good", fmt.Sprintf("Leadership moved from %s to %s (term %d).", prev.leader, cur.leader, cur.term), true)
		}
	}

	if workersOK {
		for _, w := range ws {
			id, st := w.GetId(), w.GetState()
			was, known := prev.workers[id]
			switch {
			case known && was == store.WorkerAlive && st == store.WorkerDead:
				d.addFeedLocked("worker", "bad", fmt.Sprintf(
					"Worker %s (host %s) missed its heartbeats and was declared dead. Its leases were revoked in the same step and the jobs requeued under new fencing tokens.",
					short(id), short(w.GetHostname())), true)
			case !known && st == store.WorkerAlive:
				if old, ok := prev.hosts[w.GetHostname()]; ok && old != id {
					d.addFeedLocked("worker", "info", fmt.Sprintf(
						"Host %s re-registered as worker %s. Its old id %s stays dead, so anything it reports under the old leases is rejected.",
						short(w.GetHostname()), short(id), short(old)), true)
				} else {
					d.addFeedLocked("worker", "info", fmt.Sprintf("Worker %s joined (host %s, %d slots).", short(id), short(w.GetHostname()), w.GetConcurrency()), false)
				}
			}
		}
	}

	t := d.metrics.tick
	if n := t["stale"]; n > 0 {
		d.addFeedLocked("fencing", "bad", fmt.Sprintf(
			"Fenced %s: a worker reported on a lease that had since been reassigned. The token no longer matched, so the store refused it (ABORTED) and the result was never written.",
			plural(n, "stale report", "stale reports")), true)
	}
	if n := t["expired"]; n > 0 {
		d.addFeedLocked("lease", "warn", fmt.Sprintf("The reaper revoked %s whose deadline passed without a renewal.", plural(n, "lease", "leases")), false)
	}
	if n := t["requeued"]; n > 0 {
		d.addFeedLocked("lease", "warn", fmt.Sprintf("%s back to PENDING after losing its worker.", plural(n, "job went", "jobs went")), false)
	}
	if n := t["dead"]; n > 0 {
		d.addFeedLocked("job", "bad", fmt.Sprintf("%s after exhausting max attempts.", plural(n, "job was dead-lettered", "jobs were dead-lettered")), false)
	}
	if n := t["released"]; n > 0 {
		d.addFeedLocked("lease", "info", fmt.Sprintf("%s: the acquire reply could not be delivered, so the coordinator handed the lease straight back.", plural(n, "lease released", "leases released")), false)
	}
}

func (d *Dash) backendLocked() string {
	if d.target == nil {
		return ""
	}
	return d.target.l.Backend
}

// feedItem is one line of the activity feed.
type feedItem struct {
	Seq   int64  `json:"seq"`
	At    int64  `json:"at"`
	Kind  string `json:"kind"`  // action, cluster, worker, fencing, lease, job, scenario, error
	Level string `json:"level"` // info, good, warn, bad
	Text  string `json:"text"`
	// Mark also draws the item on the charts and the timeline.
	Mark bool `json:"mark"`
}

func (d *Dash) addFeed(kind, level, text string, mark bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.addFeedLocked(kind, level, text, mark)
}

func (d *Dash) addFeedLocked(kind, level, text string, mark bool) {
	d.feedSeq++
	d.feed = append(d.feed, feedItem{Seq: d.feedSeq, At: time.Now().UnixMilli(), Kind: kind, Level: level, Text: text, Mark: mark})
	if len(d.feed) > feedLen {
		d.feed = d.feed[len(d.feed)-feedLen:]
	}
}

// newFeedLocked returns the items added since the last published tick.
func (d *Dash) newFeedLocked() []feedItem {
	var out []feedItem
	for _, f := range d.feed {
		if f.Seq > d.sentSeq {
			out = append(out, f)
		}
	}
	d.sentSeq = d.feedSeq
	return out
}

// initMessage is what a newly connected browser gets: the latest snapshot
// plus the history it missed.
func (d *Dash) initMessage() any {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.view
	if s == nil {
		s = &snapshot{Now: time.Now().UnixMilli(), Phase: d.phase, Docker: d.docker != nil, Load: d.load.view(),
			Scenarios: scenarioList(d.backendLocked()), OpLog: append([]string(nil), d.opLog...)}
	}
	return map[string]any{"snapshot": s, "series": append([]point(nil), d.series...), "feed": append([]feedItem(nil), d.feed...)}
}

func (d *Dash) logOp(line string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.opLog = append(d.opLog, line)
	if len(d.opLog) > opLogLen {
		d.opLog = d.opLog[len(d.opLog)-opLogLen:]
	}
}

// hub fans published messages out to every connected browser.
type hub struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

func newHub() *hub { return &hub{subs: map[chan []byte]struct{}{}} }

func (h *hub) subscribe() (<-chan []byte, func()) {
	ch := make(chan []byte, 16)
	h.mu.Lock()
	h.subs[ch] = struct{}{}
	h.mu.Unlock()
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.subs[ch]; ok {
			delete(h.subs, ch)
			close(ch)
		}
	}
}

// publish sends an SSE message to every subscriber. A subscriber too slow
// to keep up is disconnected; its browser reconnects and gets a fresh
// init message instead of a gap.
func (h *hub) publish(event string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	msg := []byte("event: " + event + "\ndata: " + string(b) + "\n\n")
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- msg:
		default:
			delete(h.subs, ch)
			close(ch)
		}
	}
}

func nodeName(n nodeView) string {
	if n.ID != "" {
		return n.ID
	}
	return n.Service
}

// short abbreviates an id for prose. Worker ids are w-<hostname>-<random>,
// and every incarnation of a host shares the prefix, so a worker is named
// by its random suffix. Job ids are time-ordered UUIDs whose leading
// digits are shared by every job submitted in the same moment, so a job is
// named by its last digits.
func short(id string) string {
	switch {
	case strings.HasPrefix(id, "w-"):
		return id[strings.LastIndex(id, "-")+1:]
	case strings.Contains(id, "-") && len(id) > 8:
		return id[len(id)-8:]
	case len(id) > 8:
		return id[:8]
	}
	return id
}

func plural(n float64, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return fmt.Sprintf("%.0f %s", n, many)
}
