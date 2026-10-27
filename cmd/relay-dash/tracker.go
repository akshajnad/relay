package main

import (
	"context"
	"sort"
	"sync"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/store"
)

const (
	// trackRate caps how many newly submitted jobs per second the tracker
	// follows. Each followed job costs a GetJob per poll until it
	// finishes, which on Raft is a leader read with a quorum check, so
	// following everything at high load would distort what it shows.
	trackRate = 40
	// maxInFlight caps followed jobs that have not finished.
	maxInFlight = 400
	// keepFinished is how long a finished job stays on the timeline.
	keepFinished = 75 * time.Second
	pollEvery    = 400 * time.Millisecond
	pollParallel = 8
)

// tracker follows the jobs the dashboard submitted, polling GetJob until
// each one finishes. Their histories give the lease timeline: who held
// which lease, under which fencing token, and how it ended.
type tracker struct {
	mu        sync.Mutex
	jobs      map[string]*trackedJob
	untracked int64
	budget    float64
	refilled  time.Time
	clock     clockOffset
}

type trackedJob struct {
	ID       string
	Type     string
	Origin   string // load, manual or scenario
	Sent     time.Time
	Job      *relayv1.Job
	Events   []*relayv1.JobEvent
	Terminal bool
	Done     time.Time // when the dashboard saw it finish
	errors   int
}

func newTracker() *tracker {
	return &tracker{jobs: map[string]*trackedJob{}, budget: trackRate}
}

func (t *tracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.jobs = map[string]*trackedJob{}
	t.untracked = 0
	t.clock = clockOffset{}
}

// add follows a job unless the tracking budget is spent; force skips the
// budget (for jobs a person submitted by hand).
func (t *tracker) add(id, typ, origin string, sent, recv time.Time, force bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.jobs[id]; ok {
		return
	}
	now := time.Now()
	if !t.refilled.IsZero() {
		t.budget = min(trackRate, t.budget+now.Sub(t.refilled).Seconds()*trackRate)
	}
	t.refilled = now
	inFlight := 0
	for _, j := range t.jobs {
		if !j.Terminal {
			inFlight++
		}
	}
	if !force && (t.budget < 1 || inFlight >= maxInFlight) {
		t.untracked++
		return
	}
	t.budget--
	// The submit's midpoint stands in for the store's SUBMITTED time when
	// estimating the clock offset; see clockOffset.
	t.jobs[id] = &trackedJob{ID: id, Type: typ, Origin: origin, Sent: sent.Add(recv.Sub(sent) / 2)}
}

// run polls unfinished jobs until ctx ends. client returns the current
// cluster's JobService, or nil when there is none.
func (t *tracker) run(ctx context.Context, client func() relayv1.JobServiceClient) {
	tick := time.NewTicker(pollEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if c := client(); c != nil {
			t.pollOnce(ctx, c)
		}
	}
}

func (t *tracker) pollOnce(ctx context.Context, c relayv1.JobServiceClient) {
	t.mu.Lock()
	var ids []string
	for id, j := range t.jobs {
		if !j.Terminal {
			ids = append(ids, id)
		} else if time.Since(j.Done) > keepFinished {
			delete(t.jobs, id)
		}
	}
	t.mu.Unlock()

	sem := make(chan struct{}, pollParallel)
	var wg sync.WaitGroup
	for _, id := range ids {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			rctx, cancel := context.WithTimeout(ctx, time.Second)
			resp, err := c.GetJob(rctx, &relayv1.GetJobRequest{JobId: id})
			cancel()
			t.update(id, resp, err)
		}()
	}
	wg.Wait()
}

func (t *tracker) update(id string, resp *relayv1.GetJobResponse, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	j := t.jobs[id]
	if j == nil {
		return
	}
	if err != nil {
		// A job that keeps failing to load (say, the cluster was replaced
		// under it) is dropped rather than polled forever.
		if j.errors++; j.errors > 50 {
			delete(t.jobs, id)
		}
		return
	}
	j.errors = 0
	if j.Job == nil && len(resp.GetHistory()) > 0 && resp.GetHistory()[0].GetKind() == "SUBMITTED" {
		t.clock.sample(resp.GetHistory()[0].GetAtMs(), j.Sent)
	}
	j.Job, j.Events = resp.GetJob(), resp.GetHistory()
	if s := j.Job.GetStatus(); s == relayv1.JobStatus_JOB_STATUS_SUCCEEDED || s == relayv1.JobStatus_JOB_STATUS_FAILED || s == relayv1.JobStatus_JOB_STATUS_DEAD {
		if !j.Terminal {
			j.Terminal, j.Done = true, time.Now()
		}
	}
}

// clockOffset estimates store clock minus dashboard clock, so store
// timestamps (event times, heartbeats) can be placed on the dashboard's
// timeline. On one machine with Docker's VM the two are close but not
// equal. Each sample compares a job's SUBMITTED time with the midpoint of
// the dashboard's submit call; the estimate is a moving average.
type clockOffset struct {
	ms float64
	n  int
}

func (c *clockOffset) sample(storeMs int64, local time.Time) {
	s := float64(storeMs - local.UnixMilli())
	if c.n == 0 {
		c.ms = s
	} else {
		c.ms += (s - c.ms) / float64(min(c.n+1, 50))
	}
	c.n++
}

// local converts a store timestamp to dashboard milliseconds.
func (c *clockOffset) local(storeMs int64) int64 {
	if storeMs == 0 {
		return 0
	}
	return storeMs - int64(c.ms)
}

// span is one lease: a job held by a worker under a fencing token.
type span struct {
	Job     string `json:"job"`
	Type    string `json:"type"`
	Worker  string `json:"worker"`
	Token   int64  `json:"token"`
	Lease   int    `json:"lease"` // 1 for the job's first lease
	Start   int64  `json:"start"` // dashboard ms
	End     int64  `json:"end"`   // 0 while held
	Outcome string `json:"outcome"`
}

// leaseSpans derives a job's leases from its history. A job holds at most
// one lease at a time: LEASED opens one, and the next event about the job
// closes it with that event's kind (SUCCEEDED, RETRY, FAILED, DEAD,
// REQUEUED, RELEASED). A LEASED while one is open should not happen; it
// closes the open span as SUPERSEDED rather than losing it.
func leaseSpans(id, typ string, events []*relayv1.JobEvent, local func(int64) int64) []span {
	var out []span
	open := -1
	for _, e := range events {
		switch e.GetKind() {
		case "SUBMITTED":
			continue
		case store.StatusLeased:
			if open >= 0 {
				out[open].End, out[open].Outcome = local(e.GetAtMs()), "SUPERSEDED"
			}
			out = append(out, span{Job: id, Type: typ, Worker: e.GetWorkerId(), Token: e.GetLeaseToken(),
				Lease: len(out) + 1, Start: local(e.GetAtMs())})
			open = len(out) - 1
		default:
			if open >= 0 {
				out[open].End, out[open].Outcome = local(e.GetAtMs()), e.GetKind()
				open = -1
			}
		}
	}
	return out
}

// jobSummary is one row of the recent-jobs table.
type jobSummary struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Origin      string `json:"origin"`
	Status      string `json:"status"`
	Attempts    int32  `json:"attempts"`
	MaxAttempts int32  `json:"max_attempts"`
	Worker      string `json:"worker"`
	Token       int64  `json:"token"`
	Submitted   int64  `json:"submitted"`
	Finished    int64  `json:"finished"`
	Leases      int    `json:"leases"`
	LastError   string `json:"last_error,omitempty"`
}

type trackerView struct {
	Spans     []span       `json:"spans"`
	Recent    []jobSummary `json:"recent"`
	InFlight  int          `json:"in_flight"`
	Followed  int          `json:"followed"`
	Untracked int64        `json:"untracked"`
	// Held maps worker id to the followed jobs it holds a lease on.
	Held map[string][]heldJob `json:"held"`
}

type heldJob struct {
	Job   string `json:"job"`
	Type  string `json:"type"`
	Token int64  `json:"token"`
}

// view summarizes the followed jobs: lease spans that ended within window,
// and the most recent jobs.
func (t *tracker) view(now time.Time, window time.Duration, maxSpans, maxRecent int) trackerView {
	t.mu.Lock()
	defer t.mu.Unlock()
	cutoff := now.Add(-window).UnixMilli()
	v := trackerView{Untracked: t.untracked, Held: map[string][]heldJob{}, Followed: len(t.jobs)}
	jobs := make([]*trackedJob, 0, len(t.jobs))
	for _, j := range t.jobs {
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(a, b int) bool { return jobs[a].Sent.After(jobs[b].Sent) })
	for _, j := range jobs {
		if !j.Terminal {
			v.InFlight++
		}
		spans := leaseSpans(j.ID, j.Type, j.Events, t.clock.local)
		for _, s := range spans {
			if s.End == 0 {
				v.Held[s.Worker] = append(v.Held[s.Worker], heldJob{Job: j.ID, Type: j.Type, Token: s.Token})
			}
			if (s.End == 0 || s.End >= cutoff) && len(v.Spans) < maxSpans {
				v.Spans = append(v.Spans, s)
			}
		}
		if len(v.Recent) < maxRecent {
			v.Recent = append(v.Recent, t.summary(j, len(spans)))
		}
	}
	return v
}

func (t *tracker) summary(j *trackedJob, leases int) jobSummary {
	s := jobSummary{ID: j.ID, Type: j.Type, Origin: j.Origin, Status: "SUBMITTED", Submitted: j.Sent.UnixMilli(), Leases: leases}
	if p := j.Job; p != nil {
		s.Status = statusName(p.GetStatus())
		s.Attempts, s.MaxAttempts = p.GetAttempts(), p.GetMaxAttempts()
		s.Worker, s.Token = p.GetLeaseOwner(), p.GetLeaseToken()
		s.Finished = t.clock.local(p.GetFinishedAtMs())
		s.LastError = p.GetLastError()
	}
	return s
}

func (t *tracker) offset() *clockOffset {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.clock
	return &c
}

func statusName(s relayv1.JobStatus) string {
	switch s {
	case relayv1.JobStatus_JOB_STATUS_PENDING:
		return store.StatusPending
	case relayv1.JobStatus_JOB_STATUS_LEASED:
		return store.StatusLeased
	case relayv1.JobStatus_JOB_STATUS_SUCCEEDED:
		return store.StatusSucceeded
	case relayv1.JobStatus_JOB_STATUS_FAILED:
		return store.StatusFailed
	case relayv1.JobStatus_JOB_STATUS_DEAD:
		return store.StatusDead
	}
	return "UNKNOWN"
}
