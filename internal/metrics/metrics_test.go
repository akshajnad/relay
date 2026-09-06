package metrics

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHistogramPercentiles(t *testing.T) {
	h := NewHistogram()
	for i := 1; i <= 1000; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	s := h.Summary()
	if s.Count != 1000 {
		t.Fatalf("count = %d", s.Count)
	}
	// HdrHistogram at 3 significant digits is accurate to 0.1%.
	for _, c := range []struct {
		name      string
		got, want float64
	}{{"p50", s.P50Ms, 500}, {"p95", s.P95Ms, 950}, {"p99", s.P99Ms, 990}, {"max", s.MaxMs, 1000}} {
		if math.Abs(c.got-c.want)/c.want > 0.002 {
			t.Errorf("%s = %v, want ~%v", c.name, c.got, c.want)
		}
	}
	h.Record(10 * time.Minute) // beyond range: clamped, not dropped
	if h.Summary().Count != 1001 {
		t.Fatal("out-of-range value dropped")
	}
	h.Reset()
	if h.Summary().Count != 0 {
		t.Fatal("reset did not clear")
	}
}

func TestHandler(t *testing.T) {
	r := NewRegistry()
	r.SchedulingLatency.Record(3 * time.Millisecond)
	r.Succeeded.Add(7)
	srv := httptest.NewServer(r.Handler(r.Prometheus(PromSources{
		QueueDepth: func(context.Context) (int64, int64, error) { return 5, 2, nil },
	})))
	defer srv.Close()

	var snap Snapshot
	resp, err := http.Get(srv.URL + "/metrics.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if snap.Counters.Succeeded != 7 || snap.SchedulingLatency.Count != 1 {
		t.Fatalf("snapshot = %+v", snap)
	}

	resp, err = http.Post(srv.URL+"/metrics/reset", "", nil)
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("reset = %v, %v", resp, err)
	}
	if s := r.Snapshot(); s.Counters.Succeeded != 0 || s.SchedulingLatency.Count != 0 {
		t.Fatalf("after reset = %+v", s)
	}
}

func TestPrometheusExposition(t *testing.T) {
	r := NewRegistry()
	// Like the coordinator, build the Prometheus registry before recording:
	// histograms feed Prometheus from the moment it is attached.
	srv := httptest.NewServer(r.Handler(r.Prometheus(PromSources{
		QueueDepth: func(context.Context) (int64, int64, error) { return 5, 2, nil },
		IsLeader:   func() bool { return false },
	})))
	defer srv.Close()
	r.SchedulingLatency.Record(3 * time.Millisecond)
	r.RaftCommit.Record(2 * time.Millisecond)
	r.StaleRejections.Add(4)
	r.LeaderChanges.Add(1)
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		"relay_dispatch_latency_seconds_count 1",
		"relay_raft_commit_seconds_count 1",
		"relay_stale_completion_rejections_total 4",
		"relay_leader_changes_total 1",
		`relay_queue_depth{state="pending"} 5`,
		"relay_leases_active 2",
		"relay_is_leader 0",
		"relay_reassignments_total 0",
		"relay_lease_expirations_total 0",
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("exposition lacks %q", want)
		}
	}
}
