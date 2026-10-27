package main

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
)

const sampleMetrics = `# TYPE relay_jobs_succeeded_total counter
relay_jobs_succeeded_total 10
# TYPE relay_queue_depth gauge
relay_queue_depth{state="pending"} 4
# TYPE relay_dispatch_latency_seconds histogram
relay_dispatch_latency_seconds_bucket{le="0.001"} 2
relay_dispatch_latency_seconds_bucket{le="0.01"} 6
relay_dispatch_latency_seconds_bucket{le="0.1"} 10
relay_dispatch_latency_seconds_bucket{le="+Inf"} 10
relay_dispatch_latency_seconds_sum 0.2
relay_dispatch_latency_seconds_count 10
`

func mustParse(t *testing.T, text string) *scrape {
	t.Helper()
	s, err := parseScrape(strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestParseScrape(t *testing.T) {
	s := mustParse(t, sampleMetrics)
	if got := s.values["relay_jobs_succeeded_total"]; got != 10 {
		t.Errorf("counter = %v, want 10", got)
	}
	if got := s.values["relay_queue_depth"]; got != 4 {
		t.Errorf("gauge = %v, want 4", got)
	}
	h := s.hists["relay_dispatch_latency_seconds"]
	if len(h.bounds) != 4 || !math.IsInf(h.bounds[3], 1) || h.total() != 10 {
		t.Errorf("histogram = %+v", h)
	}
}

func TestQuantileInterpolates(t *testing.T) {
	h := histogram{bounds: []float64{1, 2, 4, math.Inf(1)}, counts: []float64{0, 10, 20, 20}}
	cases := []struct{ q, want float64 }{
		{0.25, 1.5}, // rank 5 of the 10 in (1,2]
		{0.5, 2},    // rank 10: the top of (1,2]
		{0.75, 3},   // rank 15: halfway through (2,4]
	}
	for _, c := range cases {
		if got, ok := h.quantile(c.q); !ok || math.Abs(got-c.want) > 1e-9 {
			t.Errorf("quantile(%v) = %v, %v; want %v", c.q, got, ok, c.want)
		}
	}
	if _, ok := (histogram{}).quantile(0.5); ok {
		t.Error("an empty histogram has no quantile")
	}
	inf := histogram{bounds: []float64{1, math.Inf(1)}, counts: []float64{1, 10}}
	if got, _ := inf.quantile(0.99); got != 1 {
		t.Errorf("a rank in +Inf reports the highest finite bound, got %v", got)
	}
}

func TestHistogramSinceDetectsReset(t *testing.T) {
	b := []float64{1, math.Inf(1)}
	prev := histogram{bounds: b, counts: []float64{5, 8}}
	if d := (histogram{bounds: b, counts: []float64{7, 11}}).since(prev); d.counts[0] != 2 || d.counts[1] != 3 {
		t.Errorf("since = %v, want [2 3]", d.counts)
	}
	restarted := histogram{bounds: b, counts: []float64{1, 1}}
	if d := restarted.since(prev); d.counts[0] != 1 || d.counts[1] != 1 {
		t.Errorf("after a restart every observation is new, got %v", d.counts)
	}
}

func TestMetricStateSumsNodesAndSurvivesRestarts(t *testing.T) {
	m := newMetricState()
	at := time.Unix(0, 0)
	counter := func(v int) *scrape {
		return mustParse(t, "# TYPE relay_jobs_succeeded_total counter\nrelay_jobs_succeeded_total "+strconv.Itoa(v)+"\n")
	}
	// The first scrape of each node is only a baseline.
	m.observe(at, map[string]*scrape{"a": counter(100), "b": counter(50)})
	if m.totals["succeeded"] != 0 {
		t.Fatalf("baseline counted as increments: %v", m.totals["succeeded"])
	}
	at = at.Add(time.Second)
	m.observe(at, map[string]*scrape{"a": counter(110), "b": counter(55)})
	// b restarts: its counter drops to 3, all of it new.
	at = at.Add(time.Second)
	m.observe(at, map[string]*scrape{"a": counter(110), "b": counter(3)})
	if got := m.totals["succeeded"]; got != 18 {
		t.Errorf("totals = %v, want 10+5+3 = 18", got)
	}
	if got := m.tick["succeeded"]; got != 3 {
		t.Errorf("last tick = %v, want 3", got)
	}
	if got := m.rates()["succeeded"]; got != 9 {
		t.Errorf("rate = %v/s, want 18 over 2s", got)
	}
}

func ev(at int64, kind, worker string, token int64) *relayv1.JobEvent {
	return &relayv1.JobEvent{AtMs: at, Kind: kind, WorkerId: worker, LeaseToken: token}
}

func TestLeaseSpans(t *testing.T) {
	id := func(ms int64) int64 { return ms }
	// Leased to w1, which is declared dead; requeued, leased to w2 under a
	// new token, which fails once and retries; then w3 completes it.
	events := []*relayv1.JobEvent{
		ev(0, "SUBMITTED", "", 0),
		ev(10, "LEASED", "w1", 4),
		ev(40, "REQUEUED", "w1", 4),
		ev(41, "LEASED", "w2", 9),
		ev(60, "RETRY", "w2", 9),
		ev(80, "LEASED", "w3", 12),
		ev(95, "SUCCEEDED", "w3", 12),
	}
	got := leaseSpans("j", "sleep", events, id)
	want := []span{
		{Job: "j", Type: "sleep", Worker: "w1", Token: 4, Lease: 1, Start: 10, End: 40, Outcome: "REQUEUED"},
		{Job: "j", Type: "sleep", Worker: "w2", Token: 9, Lease: 2, Start: 41, End: 60, Outcome: "RETRY"},
		{Job: "j", Type: "sleep", Worker: "w3", Token: 12, Lease: 3, Start: 80, End: 95, Outcome: "SUCCEEDED"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d spans, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("span %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	open := leaseSpans("j", "noop", events[:4], id)
	if last := open[len(open)-1]; last.End != 0 || last.Worker != "w2" {
		t.Errorf("a lease with no closing event stays open, got %+v", last)
	}
}

func TestClusterConfigValidate(t *testing.T) {
	ok := defaultClusterConfig()
	if err := ok.Validate(); err != nil {
		t.Fatalf("defaults must validate: %v", err)
	}
	bad := map[string]func(*ClusterConfig){
		"lease inside detection window": func(c *ClusterConfig) { c.LeaseDuration = "3s" },
		"not a duration":                func(c *ClusterConfig) { c.PollTimeout = "five" },
		"leader lease > raft heartbeat": func(c *ClusterConfig) { c.RaftLeaderLeaseTimeout = "2s" },
		"election < raft heartbeat":     func(c *ClusterConfig) { c.RaftElectionTimeout = "500ms" },
		"unknown backend":               func(c *ClusterConfig) { c.Backend = "etcd" },
		"no slots":                      func(c *ClusterConfig) { c.Concurrency = 0 },
	}
	for name, mutate := range bad {
		c := defaultClusterConfig()
		mutate(&c)
		if c.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// Raft-only constraints do not apply to Redis.
	c := defaultClusterConfig()
	c.Backend, c.RaftLeaderLeaseTimeout = "redis", "2s"
	if err := c.Validate(); err != nil {
		t.Errorf("redis with odd raft settings: %v", err)
	}
}

func TestConfigFromEnvRoundTrip(t *testing.T) {
	want := defaultClusterConfig()
	want.Concurrency, want.LeaseDuration, want.MissedHeartbeats, want.Workers = 8, "20s", 5, 4
	env := map[string]string{}
	for _, kv := range want.env() {
		k, v, _ := strings.Cut(kv, "=")
		env[k] = v
	}
	// Inside the worker container compose renames the concurrency variable.
	worker := map[string]string{"RELAY_CONCURRENCY": env["RELAY_WORKER_CONCURRENCY"], "RELAY_SLEEP_DURATION": env["RELAY_SLEEP_DURATION"]}
	got := configFromEnv("raft", env, worker, 4)
	if got != want {
		t.Errorf("round trip:\n got %+v\nwant %+v", got, want)
	}
}

func TestGuard(t *testing.T) {
	cases := []struct {
		host, origin, ctype, method string
		ok                          bool
	}{
		{"127.0.0.1:8080", "", "", "GET", true},
		{"localhost:8080", "http://localhost:8080", "application/json", "POST", true},
		{"evil.example:8080", "", "", "GET", false},                                  // DNS rebinding
		{"127.0.0.1:8080", "", "text/plain", "POST", false},                          // a cross-site simple form
		{"127.0.0.1:8080", "http://evil.example", "application/json", "POST", false}, // cross-origin
	}
	ok := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	h := guard("127.0.0.1", ok)
	for _, c := range cases {
		r := httptest.NewRequest(c.method, "http://"+c.host+"/api/fault", strings.NewReader("{}"))
		if c.origin != "" {
			r.Header.Set("Origin", c.origin)
		}
		if c.ctype != "" {
			r.Header.Set("Content-Type", c.ctype)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if got := w.Code == http.StatusOK; got != c.ok {
			t.Errorf("%+v: status %d", c, w.Code)
		}
	}
}
