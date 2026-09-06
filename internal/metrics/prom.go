package metrics

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// PromSources supplies the values Prometheus reads at scrape time.
type PromSources struct {
	// QueueDepth returns pending and leased job counts.
	QueueDepth func(ctx context.Context) (pending, leased int64, err error)
	// IsLeader reports leadership for a replicated store; nil means this
	// node always serves (Redis).
	IsLeader func() bool
}

// Prometheus builds the registry served at /metrics. Counters read this
// Registry's atomics when scraped, so nothing is counted twice; they drop
// to zero after POST /metrics/reset, which Prometheus treats as a counter
// reset. The latency histograms are fed by the same Record calls as the
// HdrHistograms.
func (r *Registry) Prometheus(src PromSources) *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	hist := func(h *Histogram, name, help string, buckets []float64) {
		ph := prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets})
		h.observe(ph)
		reg.MustRegister(ph)
	}
	fine := prometheus.ExponentialBucketsRange(50e-6, 5, 24)
	hist(r.SchedulingLatency, "relay_dispatch_latency_seconds",
		"Per assignment: handoff to the worker minus max(job eligible, worker request arrival).", fine)
	hist(r.QueueWait, "relay_queue_wait_seconds",
		"Per assignment: leased_at minus enqueued_at.", prometheus.ExponentialBucketsRange(1e-3, 300, 24))
	hist(r.AcquireScript, "relay_store_acquire_seconds",
		"Round-trip time of one store Acquire call.", fine)
	hist(r.RaftCommit, "relay_raft_commit_seconds",
		"Raft: time from proposing a command until it is committed and applied on the leader.", fine)

	counter := func(name, help string, v func() int64) {
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: name, Help: help},
			func() float64 { return float64(v()) }))
	}
	counter("relay_jobs_submitted_total", "Jobs created by SubmitJob.", r.Submitted.Load)
	counter("relay_submit_duplicates_total", "Submits answered from an existing idempotency key.", r.Duplicates.Load)
	counter("relay_leases_granted_total", "Leases handed to workers.", r.Leased.Load)
	counter("relay_jobs_succeeded_total", "Jobs completed successfully.", r.Succeeded.Load)
	counter("relay_jobs_retried_total", "Failed attempts scheduled for retry.", r.Retried.Load)
	counter("relay_jobs_failed_total", "Jobs failed permanently.", r.Failed.Load)
	counter("relay_jobs_dead_total", "Jobs dead-lettered after exhausting attempts.", r.Dead.Load)
	counter("relay_stale_completion_rejections_total", "Complete/Fail reports rejected because the lease token was no longer current.", r.StaleRejections.Load)
	counter("relay_reassignments_total", "Leases revoked (worker death or expiry) with the job requeued for another worker.", r.Requeued.Load)
	counter("relay_lease_expirations_total", "Leases revoked by the reaper because their deadline passed.", r.LeaseExpirations.Load)
	counter("relay_workers_expired_total", "Workers declared dead by the failure detector.", r.WorkersExpired.Load)
	counter("relay_leases_released_total", "Leases handed back because the acquire reply could not be delivered.", r.Released.Load)
	counter("relay_leader_changes_total", "Leadership transitions observed by this node.", r.LeaderChanges.Load)

	if src.QueueDepth != nil {
		depth := func(pick func(p, l int64) int64) func() float64 {
			return func() float64 {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				p, l, err := src.QueueDepth(ctx)
				if err != nil {
					return -1
				}
				return float64(pick(p, l))
			}
		}
		reg.MustRegister(
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "relay_queue_depth", Help: "Jobs waiting to run (PENDING, including those in retry backoff).",
				ConstLabels: prometheus.Labels{"state": "pending"}}, depth(func(p, _ int64) int64 { return p })),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "relay_leases_active", Help: "Jobs currently LEASED to a worker."},
				depth(func(_, l int64) int64 { return l })),
		)
	}
	isLeader := src.IsLeader
	if isLeader == nil {
		isLeader = func() bool { return true }
	}
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "relay_is_leader", Help: "1 if this coordinator serves (is the Raft leader, or uses Redis)."},
		func() float64 {
			if isLeader() {
				return 1
			}
			return 0
		}))
	return reg
}
