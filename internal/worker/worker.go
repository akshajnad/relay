// Package worker implements the Relay worker loop: register, heartbeat,
// long-poll for jobs, run them through a handler, and report the outcome.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	Concurrency int
	Hostname    string
	// ShutdownTimeout is how long in-flight jobs may run after shutdown
	// begins. Jobs still running then are abandoned; their leases lapse and
	// the coordinator requeues them.
	ShutdownTimeout time.Duration
	// ReportTimeout bounds how long the worker keeps retrying a Complete or
	// Fail call that fails with a transient error (e.g. the coordinator is
	// restarting). Past it the result is dropped and the lease lapses.
	ReportTimeout time.Duration
	// RPCTimeout bounds each unary RPC other than AcquireJob.
	RPCTimeout time.Duration
	// AcquireTimeout is the client-side deadline for one AcquireJob
	// long-poll. It must exceed the coordinator's poll timeout.
	AcquireTimeout time.Duration
}

func DefaultConfig() Config {
	return Config{
		Concurrency:     1,
		Hostname:        "worker",
		ShutdownTimeout: 30 * time.Second,
		ReportTimeout:   time.Minute,
		RPCTimeout:      5 * time.Second,
		AcquireTimeout:  30 * time.Second,
	}
}

// heldLease is a lease this process is running a job under.
type heldLease struct {
	owner string // the worker id the lease was granted to
	token int64
	lost  bool // the coordinator said it no longer holds this lease
}

type Worker struct {
	client   relayv1.WorkerServiceClient
	handlers Registry
	cfg      Config
	log      *slog.Logger

	regMu sync.Mutex // serializes (re-)registration

	mu        sync.Mutex
	id        string
	heartbeat time.Duration
	held      map[string]*heldLease
}

func New(client relayv1.WorkerServiceClient, handlers Registry, cfg Config, log *slog.Logger) *Worker {
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	return &Worker{client: client, handlers: handlers, cfg: cfg, log: log, held: map[string]*heldLease{}}
}

// ID returns the worker's current id ("" before registration).
func (w *Worker) ID() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.id
}

// InFlight returns the number of jobs currently running.
func (w *Worker) InFlight() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.held)
}

// Run works until ctx is cancelled, then shuts down gracefully: it stops
// acquiring, keeps heartbeating while in-flight jobs finish and report, and
// returns once they have (or once ShutdownTimeout passes).
func (w *Worker) Run(ctx context.Context) error {
	if err := w.register(ctx, ""); err != nil {
		return err
	}

	// hard is cancelled only when the shutdown timeout expires; it governs
	// running handlers and result reporting. Heartbeats run until every slot
	// has exited, so draining jobs keep their leases.
	hard, cancelHard := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelHard()
	hbCtx, stopHB := context.WithCancel(context.WithoutCancel(ctx))
	hbDone := make(chan struct{})
	go func() { w.heartbeatLoop(hbCtx); close(hbDone) }()

	var slots sync.WaitGroup
	for i := 0; i < w.cfg.Concurrency; i++ {
		slots.Add(1)
		go func() {
			defer slots.Done()
			w.slot(ctx, hard)
		}()
	}
	allDone := make(chan struct{})
	go func() { slots.Wait(); close(allDone) }()

	select {
	case <-allDone:
	case <-ctx.Done():
		w.log.Info("shutting down: no longer acquiring, draining in-flight jobs",
			"in_flight", w.InFlight(), "timeout", w.cfg.ShutdownTimeout)
		select {
		case <-allDone:
			w.log.Info("drained cleanly")
		case <-time.After(w.cfg.ShutdownTimeout):
			w.log.Warn("shutdown timeout: abandoning in-flight jobs; their leases will lapse",
				"in_flight", w.InFlight())
			cancelHard()
			<-allDone
		}
	}
	stopHB()
	<-hbDone
	return nil
}

// register obtains a new worker id. stale is the id the caller saw rejected
// ("" on first registration); if another goroutine has already replaced it,
// register does nothing.
func (w *Worker) register(ctx context.Context, stale string) error {
	w.regMu.Lock()
	defer w.regMu.Unlock()
	if w.ID() != stale {
		return nil
	}
	backoff := newBackoff()
	for {
		rctx, cancel := context.WithTimeout(ctx, w.cfg.RPCTimeout)
		resp, err := w.client.RegisterWorker(rctx, &relayv1.RegisterWorkerRequest{
			Hostname: w.cfg.Hostname, Concurrency: int32(w.cfg.Concurrency),
		}, grpc.WaitForReady(true))
		cancel()
		if err == nil {
			if stale != "" {
				w.log.Warn("re-registered after being declared dead", "old_worker_id", stale, "worker_id", resp.WorkerId)
			} else {
				w.log.Info("registered", "worker_id", resp.WorkerId, "concurrency", w.cfg.Concurrency)
			}
			hb := time.Duration(resp.HeartbeatIntervalMs) * time.Millisecond
			if hb <= 0 {
				hb = time.Second
			}
			w.mu.Lock()
			w.id, w.heartbeat = resp.WorkerId, hb
			w.mu.Unlock()
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.log.Warn("register failed, retrying", "err", err)
		if !backoff.wait(ctx) {
			return ctx.Err()
		}
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	for {
		w.mu.Lock()
		interval := w.heartbeat
		w.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
		w.sendHeartbeat(ctx)
	}
}

func (w *Worker) sendHeartbeat(ctx context.Context) {
	w.mu.Lock()
	id := w.id
	req := &relayv1.HeartbeatRequest{WorkerId: id}
	for jobID, l := range w.held {
		if l.owner == id && !l.lost {
			req.Held = append(req.Held, &relayv1.HeldLease{JobId: jobID, LeaseToken: l.token})
		}
	}
	w.mu.Unlock()

	hctx, cancel := context.WithTimeout(ctx, w.cfg.RPCTimeout)
	defer cancel()
	resp, err := w.client.Heartbeat(hctx, req)
	switch {
	case err == nil:
	case status.Code(err) == codes.FailedPrecondition:
		// We were declared dead (e.g. we were paused or partitioned). Any
		// lease we still hold has been revoked; results for those will be
		// rejected by fencing. Re-register to keep taking work.
		if err := w.register(ctx, id); err != nil && ctx.Err() == nil {
			w.log.Error("re-register failed", "err", err)
		}
		return
	default:
		if ctx.Err() == nil {
			w.log.Warn("heartbeat failed", "err", err)
		}
		return
	}
	if len(resp.Lost) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, l := range resp.Lost {
		if h := w.held[l.JobId]; h != nil && h.token == l.LeaseToken && !h.lost {
			h.lost = true
			w.log.Warn("lease lost; result will be rejected", "job_id", l.JobId, "lease_token", l.LeaseToken)
		}
	}
}

// slot is one unit of concurrency: acquire, run, report, repeat.
func (w *Worker) slot(ctx, hard context.Context) {
	backoff := newBackoff()
	for ctx.Err() == nil {
		id := w.ID()
		actx, cancel := context.WithTimeout(ctx, w.cfg.AcquireTimeout)
		resp, err := w.client.AcquireJob(actx, &relayv1.AcquireJobRequest{WorkerId: id}, grpc.WaitForReady(true))
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if status.Code(err) == codes.FailedPrecondition {
				if err := w.register(ctx, id); err != nil {
					return
				}
				continue
			}
			w.log.Warn("acquire failed", "err", err)
			backoff.wait(ctx)
			continue
		}
		backoff.reset()
		if !resp.HasJob {
			continue
		}
		w.process(hard, resp.Job)
	}
}

func (w *Worker) process(ctx context.Context, pj *relayv1.Job) {
	w.mu.Lock()
	w.held[pj.Id] = &heldLease{owner: pj.LeaseOwner, token: pj.LeaseToken}
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		delete(w.held, pj.Id)
		w.mu.Unlock()
	}()

	job := Job{
		ID: pj.Id, Type: pj.Type, Payload: pj.Payload,
		Attempt: int(pj.Attempts), MaxAttempts: int(pj.MaxAttempts), WorkerID: pj.LeaseOwner,
	}
	log := w.log.With("job_id", job.ID, "type", job.Type, "attempt", job.Attempt, "lease_token", pj.LeaseToken)
	log.Debug("running job")
	result, err := w.handlers.run(ctx, job)
	if ctx.Err() != nil {
		// Shutdown timeout: don't report, let the lease lapse.
		log.Warn("abandoned job at shutdown")
		return
	}
	if err != nil {
		w.reportFailure(ctx, log, pj, err)
		return
	}
	w.reportSuccess(ctx, log, pj, result)
}

func (w *Worker) reportSuccess(ctx context.Context, log *slog.Logger, pj *relayv1.Job, result []byte) {
	req := &relayv1.CompleteJobRequest{JobId: pj.Id, WorkerId: pj.LeaseOwner, LeaseToken: pj.LeaseToken, Result: result}
	err := w.report(ctx, log, func(rctx context.Context) error {
		resp, err := w.client.CompleteJob(rctx, req)
		if err == nil && resp.AlreadyCompleted {
			log.Info("completion was already recorded (retried call)")
		}
		return err
	})
	if err == nil {
		log.Debug("job succeeded")
	}
}

func (w *Worker) reportFailure(ctx context.Context, log *slog.Logger, pj *relayv1.Job, jobErr error) {
	retryable := !IsPermanent(jobErr)
	req := &relayv1.FailJobRequest{JobId: pj.Id, WorkerId: pj.LeaseOwner, LeaseToken: pj.LeaseToken,
		Error: jobErr.Error(), Retryable: retryable}
	_ = w.report(ctx, log, func(rctx context.Context) error {
		resp, err := w.client.FailJob(rctx, req)
		if err == nil {
			log.Info("job failed", "err", jobErr, "retryable", retryable,
				"status", resp.Status.String(), "retry_in_ms", resp.RetryInMs)
		}
		return err
	})
}

// report delivers an outcome, retrying transient errors. A fencing
// rejection is final: the job has moved on, so the result is dropped.
func (w *Worker) report(ctx context.Context, log *slog.Logger, call func(context.Context) error) error {
	deadline := time.Now().Add(w.cfg.ReportTimeout)
	backoff := newBackoff()
	for {
		rctx, cancel := context.WithTimeout(ctx, w.cfg.RPCTimeout)
		err := call(rctx)
		cancel()
		switch status.Code(err) {
		case codes.OK:
			return nil
		case codes.Aborted:
			log.Warn("stale lease, dropping result", "err", status.Convert(err).Message())
			return err
		case codes.NotFound, codes.InvalidArgument:
			log.Error("result rejected, dropping", "err", err)
			return err
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			log.Error("giving up reporting result; lease will lapse", "err", err)
			return err
		}
		log.Warn("report failed, retrying", "err", err)
		backoff.wait(ctx)
	}
}

// backoff is capped exponential backoff with jitter for retry loops.
type backoff struct{ next time.Duration }

func newBackoff() *backoff { return &backoff{next: 50 * time.Millisecond} }

func (b *backoff) reset() { b.next = 50 * time.Millisecond }

// wait sleeps for the next interval; it returns false if ctx ended first.
func (b *backoff) wait(ctx context.Context) bool {
	d := b.next/2 + time.Duration(rand.Int64N(int64(b.next/2)+1))
	b.next = min(2*b.next, 2*time.Second)
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

var errNoHandler = errors.New("no handler registered for job type")

func noHandler(typ string) error { return Permanent(fmt.Errorf("%w %q", errNoHandler, typ)) }
