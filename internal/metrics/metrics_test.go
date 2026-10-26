package metrics

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
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
	srv := httptest.NewServer(r.Handler())
	defer srv.Close()

	var snap Snapshot
	resp, err := http.Get(srv.URL + "/metrics")
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
