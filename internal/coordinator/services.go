package coordinator

import (
	"context"
	"errors"
	"fmt"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type jobService struct {
	relayv1.UnimplementedJobServiceServer
	c *Coordinator
}

func (s *jobService) SubmitJob(ctx context.Context, req *relayv1.SubmitJobRequest) (*relayv1.SubmitJobResponse, error) {
	c := s.c
	if req.GetType() == "" {
		return nil, status.Error(codes.InvalidArgument, "type is required")
	}
	if len(req.GetPayload()) > c.cfg.MaxPayloadBytes {
		return nil, status.Errorf(codes.InvalidArgument, "payload is %d bytes, limit is %d", len(req.GetPayload()), c.cfg.MaxPayloadBytes)
	}
	if req.GetMaxAttempts() < 0 {
		return nil, status.Error(codes.InvalidArgument, "max_attempts must be >= 0")
	}
	sctx, cancel := c.storeCtx(ctx)
	defer cancel()
	id, dup, err := c.store.Submit(sctx, store.SubmitParams{
		Type:           req.GetType(),
		Payload:        req.GetPayload(),
		IdempotencyKey: req.GetIdempotencyKey(),
		MaxAttempts:    int(req.GetMaxAttempts()),
	})
	if err != nil {
		return nil, storeError(err)
	}
	if dup {
		c.metrics.Duplicates.Add(1)
	} else {
		c.metrics.Submitted.Add(1)
		c.notifier.Signal(1)
	}
	return &relayv1.SubmitJobResponse{JobId: id, Duplicate: dup}, nil
}

func (s *jobService) GetJob(ctx context.Context, req *relayv1.GetJobRequest) (*relayv1.GetJobResponse, error) {
	j, events, err := s.c.store.Get(ctx, req.GetJobId())
	if err != nil {
		return nil, storeError(err)
	}
	resp := &relayv1.GetJobResponse{Job: jobToProto(j)}
	for _, e := range events {
		resp.History = append(resp.History, &relayv1.JobEvent{
			AtMs: e.AtMs, Kind: e.Kind, WorkerId: e.Worker, LeaseToken: e.Token, Detail: e.Detail,
		})
	}
	return resp, nil
}

type workerService struct {
	relayv1.UnimplementedWorkerServiceServer
	c *Coordinator
}

func (s *workerService) RegisterWorker(ctx context.Context, req *relayv1.RegisterWorkerRequest) (*relayv1.RegisterWorkerResponse, error) {
	host := req.GetHostname()
	if host == "" {
		host = "unknown"
	}
	sctx, cancel := s.c.storeCtx(ctx)
	defer cancel()
	id, err := s.c.store.RegisterWorker(sctx, host, int(req.GetConcurrency()))
	if err != nil {
		return nil, storeError(err)
	}
	s.c.log.Info("worker registered", "worker_id", id, "hostname", host, "concurrency", req.GetConcurrency())
	return &relayv1.RegisterWorkerResponse{
		WorkerId:            id,
		HeartbeatIntervalMs: s.c.cfg.HeartbeatInterval.Milliseconds(),
	}, nil
}

func (s *workerService) Heartbeat(ctx context.Context, req *relayv1.HeartbeatRequest) (*relayv1.HeartbeatResponse, error) {
	held := make([]store.Lease, len(req.GetHeld()))
	for i, h := range req.GetHeld() {
		held[i] = store.Lease{JobID: h.GetJobId(), Token: h.GetLeaseToken()}
	}
	sctx, cancel := s.c.storeCtx(ctx)
	defer cancel()
	lost, err := s.c.store.Renew(sctx, req.GetWorkerId(), held)
	if err != nil {
		return nil, storeError(err)
	}
	resp := &relayv1.HeartbeatResponse{}
	for _, l := range lost {
		resp.Lost = append(resp.Lost, &relayv1.HeldLease{JobId: l.JobID, LeaseToken: l.Token})
	}
	return resp, nil
}

// AcquireJob long-polls for a job.
//
// It also records scheduling latency for every assignment:
//
//	latency = handoff - max(eligible, requestArrival)
//	        = min(handoff - requestArrival, handoff - eligible)
//
// handoff and requestArrival are on this process's monotonic clock. The job's
// eligible time is on the store clock, so handoff - eligible is measured as
// (handoff - scriptCall) + (storeNow - eligibleAt): each term spans a single
// clock, and clock skew between coordinator and store cannot leak in.
// Starting the first term at scriptCall rather than at the moment the store
// ran the acquire adds the request's network leg, so the result errs high.
func (s *workerService) AcquireJob(ctx context.Context, req *relayv1.AcquireJobRequest) (*relayv1.AcquireJobResponse, error) {
	c := s.c
	arrival := time.Now()
	wid := req.GetWorkerId()
	deadline := time.NewTimer(c.cfg.PollTimeout)
	defer deadline.Stop()

	for {
		if c.notifier.Closed() {
			return nil, status.Error(codes.Unavailable, "coordinator shutting down")
		}
		w := c.notifier.Subscribe()

		sctx, cancel := c.storeCtx(ctx)
		scriptCall := time.Now()
		res, err := c.store.Acquire(sctx, wid)
		cancel()
		c.metrics.AcquireScript.Record(time.Since(scriptCall))
		if err != nil {
			w.Cancel()
			return nil, storeError(err)
		}

		if j := res.Job; j != nil {
			w.Cancel()
			if res.More {
				c.notifier.Signal(1)
			}
			if ctx.Err() != nil {
				// The worker went away while we leased it a job. Hand the
				// lease straight back instead of letting it sit until expiry.
				c.release(ctx, j)
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			handoff := time.Now()
			sinceArrival := handoff.Sub(arrival)
			sinceEligible := handoff.Sub(scriptCall) + time.Duration(res.StoreNowMs-res.EligibleAtMs)*time.Millisecond
			c.metrics.SchedulingLatency.Record(min(sinceArrival, sinceEligible))
			c.metrics.QueueWait.Record(time.Duration(res.StoreNowMs-j.EnqueuedAt) * time.Millisecond)
			c.metrics.Leased.Add(1)
			return &relayv1.AcquireJobResponse{HasJob: true, Job: jobToProto(j)}, nil
		}

		if res.NextEligibleIn > 0 {
			c.notifier.SignalAfter(res.NextEligibleIn)
		}
		select {
		case <-w.C():
			// Possibly work; loop and try again.
		case <-deadline.C:
			w.Cancel()
			return &relayv1.AcquireJobResponse{}, nil
		case <-ctx.Done():
			w.Cancel()
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
}

func (c *Coordinator) release(ctx context.Context, j *store.Job) {
	sctx, cancel := c.storeCtx(ctx)
	defer cancel()
	if err := c.store.Release(sctx, j.ID, j.LeaseOwner, j.LeaseToken); err != nil {
		c.log.Warn("release of undeliverable lease failed; it will expire instead",
			"job_id", j.ID, "worker_id", j.LeaseOwner, "err", err)
		return
	}
	c.metrics.Released.Add(1)
	c.notifier.Signal(1)
}

func (s *workerService) CompleteJob(ctx context.Context, req *relayv1.CompleteJobRequest) (*relayv1.CompleteJobResponse, error) {
	c := s.c
	sctx, cancel := c.storeCtx(ctx)
	defer cancel()
	already, err := c.store.Complete(sctx, req.GetJobId(), req.GetWorkerId(), req.GetLeaseToken(), req.GetResult())
	if err != nil {
		c.noteRejection("completion", req.GetJobId(), req.GetWorkerId(), req.GetLeaseToken(), err)
		return nil, storeError(err)
	}
	if !already {
		c.metrics.Succeeded.Add(1)
	}
	return &relayv1.CompleteJobResponse{AlreadyCompleted: already}, nil
}

func (s *workerService) FailJob(ctx context.Context, req *relayv1.FailJobRequest) (*relayv1.FailJobResponse, error) {
	c := s.c
	sctx, cancel := c.storeCtx(ctx)
	defer cancel()
	out, err := c.store.Fail(sctx, req.GetJobId(), req.GetWorkerId(), req.GetLeaseToken(), req.GetError(), req.GetRetryable())
	if err != nil {
		c.noteRejection("failure", req.GetJobId(), req.GetWorkerId(), req.GetLeaseToken(), err)
		return nil, storeError(err)
	}
	if !out.Already {
		switch out.Status {
		case store.StatusPending:
			c.metrics.Retried.Add(1)
			c.notifier.SignalAfter(out.RetryIn)
		case store.StatusFailed:
			c.metrics.Failed.Add(1)
		case store.StatusDead:
			c.metrics.Dead.Add(1)
		}
	}
	return &relayv1.FailJobResponse{Status: statusToProto[out.Status], RetryInMs: out.RetryIn.Milliseconds()}, nil
}

func (c *Coordinator) noteRejection(what, jobID, workerID string, token int64, err error) {
	var stale *store.StaleLeaseError
	if errors.As(err, &stale) {
		c.metrics.StaleRejections.Add(1)
		c.log.Warn(fmt.Sprintf("rejected stale %s", what), "job_id", jobID, "worker_id", workerID,
			"lease_token", token, "current_status", stale.Status, "current_owner", stale.CurrentOwner,
			"current_token", stale.CurrentToken)
	}
}

func (s *workerService) ListWorkers(ctx context.Context, _ *relayv1.ListWorkersRequest) (*relayv1.ListWorkersResponse, error) {
	ws, err := s.c.store.ListWorkers(ctx)
	if err != nil {
		return nil, storeError(err)
	}
	resp := &relayv1.ListWorkersResponse{}
	for _, w := range ws {
		resp.Workers = append(resp.Workers, &relayv1.Worker{
			Id:              w.ID,
			Hostname:        w.Hostname,
			Concurrency:     int32(w.Concurrency),
			State:           w.State,
			RegisteredAtMs:  w.RegisteredAt,
			LastHeartbeatMs: w.LastHeartbeat,
			ActiveJobs:      int32(w.ActiveJobs),
		})
	}
	return resp, nil
}
