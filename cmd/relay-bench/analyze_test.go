package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/akshajnad/relay/internal/history"
)

func TestDistOfNearestRank(t *testing.T) {
	var ds []time.Duration
	for i := 1; i <= 100; i++ {
		ds = append(ds, time.Duration(i)*time.Millisecond)
	}
	d := distOf(ds)
	if d.Count != 100 || d.P50Ms != 50 || d.P95Ms != 95 || d.P99Ms != 99 || d.MaxMs != 100 || d.MinMs != 1 || d.MeanMs != 50.5 {
		t.Fatalf("dist = %+v", d)
	}
	if (distOf(nil) != Dist{}) {
		t.Fatal("empty dist")
	}
}

// One worker with 2 slots. Times in ms (converted to ns).
//
//	j1 submitted 0, starts 1      (slot free since forever)   -> dispatch 1, wait 1
//	j2 submitted 0, starts 2      (second free slot)          -> dispatch 2, wait 2
//	j3 submitted 3, starts 12     (both busy; j1 ends 10)     -> dispatch 12-max(3,10)=2, wait 9
//	j1 ends 10, j2 ends 20
func TestDispatchSlotSimulation(t *testing.T) {
	ms := func(v int64) int64 { return v * int64(time.Millisecond) }
	p := &phase{
		submit: map[string]int64{"j1": ms(0), "j2": ms(0), "j3": ms(3)},
		notes: map[string][]history.Record{"w": {
			{Method: history.ExecStart, JobID: "j1", TimeNs: ms(1)},
			{Method: history.ExecStart, JobID: "j2", TimeNs: ms(2)},
			{Method: history.ExecEnd, JobID: "j1", TimeNs: ms(10)},
			{Method: history.ExecStart, JobID: "j3", TimeNs: ms(12)},
			{Method: history.ExecEnd, JobID: "j2", TimeNs: ms(20)},
			{Method: history.ExecStart, JobID: "other", TimeNs: ms(21)}, // not in the phase
		}},
	}
	disp, wait := p.dispatch(2)
	want := []time.Duration{time.Millisecond, 2 * time.Millisecond, 2 * time.Millisecond}
	wantWait := []time.Duration{time.Millisecond, 2 * time.Millisecond, 9 * time.Millisecond}
	if len(disp) != 3 {
		t.Fatalf("dispatch = %v", disp)
	}
	for i := range want {
		if disp[i] != want[i] || wait[i] != wantWait[i] {
			t.Fatalf("job %d: dispatch %v wait %v, want %v %v", i, disp[i], wait[i], want[i], wantWait[i])
		}
	}
	first, last, done := p.span()
	if first != ms(1) || last != ms(20) || done != 2 {
		t.Fatalf("span = %d %d %d", first, last, done)
	}
}

func TestPlotReportRendersEveryChart(t *testing.T) {
	d := func(p50, p95, p99 float64) Dist { return Dist{P50Ms: p50, P95Ms: p95, P99Ms: p99, Count: 10} }
	rep := &CompareReport{Results: []*BackendResult{
		{Backend: benchBackends["relay-redis"], Saturation: &SaturationResult{JobsPerSec: 9000},
			Latency:  &LatencyResult{Dispatch: d(0.5, 1, 2), QueueWait: d(1, 2, 3)},
			Recovery: &TrialsResult{Seconds: []float64{3.1, 3.4, 3.2}}},
		{Backend: benchBackends["relay-raft"], Saturation: &SaturationResult{JobsPerSec: 2500},
			Latency:  &LatencyResult{Dispatch: d(1, 3, 6), QueueWait: d(2, 4, 8)},
			Recovery: &TrialsResult{Seconds: []float64{3.3, 3.9}}, Failover: &TrialsResult{Seconds: []float64{1.2, 1.9, 2.4}}},
		{Backend: benchBackends["asynq"], Saturation: &SaturationResult{JobsPerSec: 12000},
			Latency:  &LatencyResult{Dispatch: d(400, 900, 990), QueueWait: d(500, 950, 1000)},
			Recovery: &TrialsResult{Seconds: []float64{45, 61, 88}}},
		{Backend: benchBackends["asynq-tuned"], Saturation: &SaturationResult{JobsPerSec: 12500},
			Latency: &LatencyResult{Dispatch: d(5, 9, 12), QueueWait: d(6, 10, 14)}},
	}}
	dir := t.TempDir()
	if out := os.Getenv("RELAY_BENCH_PLOT_DIR"); out != "" {
		dir = out
	}
	if err := plotReport(rep, dir); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"throughput.png", "dispatch_latency.png", "queue_wait.png", "worker_recovery.png", "leader_failover.png"} {
		if st, err := os.Stat(filepath.Join(dir, f)); err != nil || st.Size() < 1000 {
			t.Fatalf("%s: %v", f, err)
		}
	}
}
