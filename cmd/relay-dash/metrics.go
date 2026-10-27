package main

import (
	"time"
)

// counters maps the names the UI uses to the coordinators' Prometheus
// counters.
var counters = map[string]string{
	"submitted":       "relay_jobs_submitted_total",
	"duplicates":      "relay_submit_duplicates_total",
	"leased":          "relay_leases_granted_total",
	"succeeded":       "relay_jobs_succeeded_total",
	"retried":         "relay_jobs_retried_total",
	"failed":          "relay_jobs_failed_total",
	"dead":            "relay_jobs_dead_total",
	"stale":           "relay_stale_completion_rejections_total",
	"requeued":        "relay_reassignments_total",
	"expired":         "relay_lease_expirations_total",
	"released":        "relay_leases_released_total",
	"workers_expired": "relay_workers_expired_total",
	"leader_changes":  "relay_leader_changes_total",
}

var histograms = map[string]string{
	"dispatch":    "relay_dispatch_latency_seconds",
	"queue_wait":  "relay_queue_wait_seconds",
	"acquire":     "relay_store_acquire_seconds",
	"raft_commit": "relay_raft_commit_seconds",
}

const (
	// latencyWindow is how far back the windowed percentiles look.
	latencyWindow = 10 * time.Second
	// rateWindow is how far back transition rates look.
	rateWindow = 2 * time.Second
)

// metricState turns per-coordinator scrapes into cluster-wide numbers.
//
// Each coordinator counts only what it served, and on Raft that moves with
// leadership, so cluster totals are the sum of every node's increments. A
// counter that goes down means its process restarted; the new value is
// then all increment. The first scrape of a node only sets its baseline,
// so attaching to a busy cluster does not register its whole history as
// one burst.
type metricState struct {
	prev map[string]*scrape // last good scrape per coordinator service

	// totals are increments since the dashboard started watching this
	// cluster; tick holds the latest tick's share of them.
	totals map[string]float64
	tick   map[string]float64
	marks  []totalsMark

	// windows holds, per histogram, the cluster-wide observations of each
	// recent tick.
	windows map[string][]histMark
}

type totalsMark struct {
	at     time.Time
	totals map[string]float64
}

type histMark struct {
	at time.Time
	h  histogram
}

func newMetricState() *metricState {
	return &metricState{prev: map[string]*scrape{}, totals: map[string]float64{}, tick: map[string]float64{},
		windows: map[string][]histMark{}}
}

// observe folds in one round of scrapes, keyed by coordinator service;
// unreachable coordinators are absent.
func (m *metricState) observe(at time.Time, scrapes map[string]*scrape) {
	m.tick = map[string]float64{}
	ticked := map[string]histogram{}
	for node, s := range scrapes {
		prev := m.prev[node]
		m.prev[node] = s
		if prev == nil {
			continue
		}
		for key, name := range counters {
			cur, ok := s.values[name]
			if !ok {
				continue
			}
			d := cur - prev.values[name]
			if d < 0 {
				d = cur
			}
			m.tick[key] += d
			m.totals[key] += d
		}
		for key, name := range histograms {
			h, ok := s.hists[name]
			if !ok {
				continue
			}
			ticked[key] = ticked[key].plus(h.since(prev.hists[name]))
		}
	}

	snap := make(map[string]float64, len(m.totals))
	for k, v := range m.totals {
		snap[k] = v
	}
	m.marks = append(m.marks, totalsMark{at, snap})
	for len(m.marks) > 2 && at.Sub(m.marks[1].at) >= rateWindow {
		m.marks = m.marks[1:]
	}
	for key := range histograms {
		w := append(m.windows[key], histMark{at, ticked[key]})
		for len(w) > 0 && at.Sub(w[0].at) > latencyWindow {
			w = w[1:]
		}
		m.windows[key] = w
	}
}

// rates returns each counter's per-second rate over about rateWindow.
func (m *metricState) rates() map[string]float64 {
	out := map[string]float64{}
	if len(m.marks) < 2 {
		return out
	}
	first, last := m.marks[0], m.marks[len(m.marks)-1]
	dt := last.at.Sub(first.at).Seconds()
	if dt <= 0 {
		return out
	}
	for key := range counters {
		out[key] = (last.totals[key] - first.totals[key]) / dt
	}
	return out
}

// latency summarizes one histogram's last latencyWindow, in milliseconds.
type latency struct {
	Count float64  `json:"count"`
	P50   *float64 `json:"p50"`
	P95   *float64 `json:"p95"`
	P99   *float64 `json:"p99"`
}

func (m *metricState) latency(key string) latency {
	var sum histogram
	for _, hm := range m.windows[key] {
		sum = sum.plus(hm.h)
	}
	ms := func(q float64) *float64 {
		v, ok := sum.quantile(q)
		if !ok {
			return nil
		}
		v *= 1000
		return &v
	}
	return latency{Count: sum.total(), P50: ms(0.50), P95: ms(0.95), P99: ms(0.99)}
}
