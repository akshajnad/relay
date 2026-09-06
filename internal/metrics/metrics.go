// Package metrics records coordinator-side latency histograms (HdrHistogram)
// and counters, and serves them over HTTP for relay-bench to scrape.
package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Histogram is a mutex-guarded HdrHistogram recording microseconds from
// 1µs to 5 minutes at 3 significant digits.
type Histogram struct {
	mu sync.Mutex
	h  *hdrhistogram.Histogram
	// prom, if set, also receives every recorded value (in seconds).
	prom prometheus.Observer
}

func (h *Histogram) observe(o prometheus.Observer) {
	h.mu.Lock()
	h.prom = o
	h.mu.Unlock()
}

func NewHistogram() *Histogram {
	return &Histogram{h: hdrhistogram.New(1, (5 * time.Minute).Microseconds(), 3)}
}

func (h *Histogram) Record(d time.Duration) {
	us := d.Microseconds()
	if us < 1 {
		us = 1
	}
	h.mu.Lock()
	// Values above the trackable range are clamped rather than dropped so
	// they still show up in max and the tail percentiles.
	if err := h.h.RecordValue(us); err != nil {
		_ = h.h.RecordValue(h.h.HighestTrackableValue())
	}
	prom := h.prom
	h.mu.Unlock()
	if prom != nil {
		prom.Observe(d.Seconds())
	}
}

// Summary is a histogram's distribution in milliseconds.
type Summary struct {
	Count  int64   `json:"count"`
	MeanMs float64 `json:"mean_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P90Ms  float64 `json:"p90_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	P999Ms float64 `json:"p999_ms"`
	MaxMs  float64 `json:"max_ms"`
	MinMs  float64 `json:"min_ms"`
}

func (h *Histogram) Summary() Summary {
	h.mu.Lock()
	defer h.mu.Unlock()
	ms := func(us int64) float64 { return float64(us) / 1000 }
	if h.h.TotalCount() == 0 {
		return Summary{}
	}
	return Summary{
		Count:  h.h.TotalCount(),
		MeanMs: h.h.Mean() / 1000,
		P50Ms:  ms(h.h.ValueAtQuantile(50)),
		P90Ms:  ms(h.h.ValueAtQuantile(90)),
		P95Ms:  ms(h.h.ValueAtQuantile(95)),
		P99Ms:  ms(h.h.ValueAtQuantile(99)),
		P999Ms: ms(h.h.ValueAtQuantile(99.9)),
		MaxMs:  ms(h.h.Max()),
		MinMs:  ms(h.h.Min()),
	}
}

func (h *Histogram) Reset() {
	h.mu.Lock()
	h.h.Reset()
	h.mu.Unlock()
}

// Registry is the coordinator's full set of metrics.
type Registry struct {
	// SchedulingLatency is, per assignment, the time the job was handed to
	// a worker minus max(time the job became eligible, time that worker's
	// AcquireJob request arrived). See the coordinator for how each span
	// is measured on a single clock.
	SchedulingLatency *Histogram
	// QueueWait is leased_at - enqueued_at per assignment, on store time.
	QueueWait *Histogram
	// AcquireScript is the round-trip time of one store Acquire call (the
	// acquire Lua script, or a Raft proposal).
	AcquireScript *Histogram
	// RaftCommit is, on a Raft leader, the time from proposing a command to
	// its being committed and applied.
	RaftCommit *Histogram

	Submitted       atomic.Int64
	Duplicates      atomic.Int64
	Leased          atomic.Int64
	Succeeded       atomic.Int64
	Retried         atomic.Int64
	Failed          atomic.Int64
	Dead            atomic.Int64
	StaleRejections atomic.Int64
	Requeued        atomic.Int64
	Released        atomic.Int64
	WorkersExpired  atomic.Int64
	// LeaderChanges counts leadership transitions seen by this node
	// (replicated stores only).
	LeaderChanges atomic.Int64
	// LeaseExpirations counts leases the reaper revoked (requeued or
	// dead-lettered) because their deadline passed.
	LeaseExpirations atomic.Int64

	mu    sync.Mutex
	since time.Time
}

func NewRegistry() *Registry {
	return &Registry{
		SchedulingLatency: NewHistogram(),
		QueueWait:         NewHistogram(),
		AcquireScript:     NewHistogram(),
		RaftCommit:        NewHistogram(),
		since:             time.Now(),
	}
}

type Counters struct {
	Submitted       int64 `json:"submitted"`
	Duplicates      int64 `json:"duplicates"`
	Leased          int64 `json:"leased"`
	Succeeded       int64 `json:"succeeded"`
	Retried         int64 `json:"retried"`
	Failed          int64 `json:"failed"`
	Dead            int64 `json:"dead"`
	StaleRejections int64 `json:"stale_rejections"`
	Requeued        int64 `json:"requeued"`
	Released        int64 `json:"released"`
	WorkersExpired  int64 `json:"workers_expired"`
	LeaderChanges   int64 `json:"leader_changes"`
	// LeaseExpirations is omitted from older result files.
	LeaseExpirations int64 `json:"lease_expirations"`
}

// Snapshot is the JSON document served at /metrics.json.
type Snapshot struct {
	Since             time.Time `json:"since"`
	SchedulingLatency Summary   `json:"scheduling_latency"`
	QueueWait         Summary   `json:"queue_wait"`
	AcquireScript     Summary   `json:"acquire_script"`
	RaftCommit        Summary   `json:"raft_commit"`
	Counters          Counters  `json:"counters"`
}

func (r *Registry) Snapshot() Snapshot {
	r.mu.Lock()
	since := r.since
	r.mu.Unlock()
	return Snapshot{
		Since:             since,
		SchedulingLatency: r.SchedulingLatency.Summary(),
		QueueWait:         r.QueueWait.Summary(),
		AcquireScript:     r.AcquireScript.Summary(),
		RaftCommit:        r.RaftCommit.Summary(),
		Counters: Counters{
			Submitted:        r.Submitted.Load(),
			Duplicates:       r.Duplicates.Load(),
			Leased:           r.Leased.Load(),
			Succeeded:        r.Succeeded.Load(),
			Retried:          r.Retried.Load(),
			Failed:           r.Failed.Load(),
			Dead:             r.Dead.Load(),
			StaleRejections:  r.StaleRejections.Load(),
			Requeued:         r.Requeued.Load(),
			Released:         r.Released.Load(),
			WorkersExpired:   r.WorkersExpired.Load(),
			LeaderChanges:    r.LeaderChanges.Load(),
			LeaseExpirations: r.LeaseExpirations.Load(),
		},
	}
}

// Reset zeroes every histogram and counter, so a benchmark measures only
// its own run.
func (r *Registry) Reset() {
	r.SchedulingLatency.Reset()
	r.QueueWait.Reset()
	r.AcquireScript.Reset()
	r.RaftCommit.Reset()
	for _, c := range []*atomic.Int64{
		&r.Submitted, &r.Duplicates, &r.Leased, &r.Succeeded, &r.Retried, &r.Failed,
		&r.Dead, &r.StaleRejections, &r.Requeued, &r.Released, &r.WorkersExpired, &r.LeaderChanges,
		&r.LeaseExpirations,
	} {
		c.Store(0)
	}
	r.mu.Lock()
	r.since = time.Now()
	r.mu.Unlock()
}

// Handler serves GET /metrics (Prometheus text format, from prom; omitted
// when prom is nil), GET /metrics.json (the HdrHistogram snapshot that
// relay-bench reads), POST /metrics/reset and /debug/pprof/.
func (r *Registry) Handler(prom *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	if prom != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(prom, promhttp.HandlerOpts{}))
	}
	mux.HandleFunc("GET /metrics.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(r.Snapshot())
	})
	mux.HandleFunc("POST /metrics/reset", func(w http.ResponseWriter, _ *http.Request) {
		r.Reset()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
