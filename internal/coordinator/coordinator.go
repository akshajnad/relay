// Package coordinator implements Relay's gRPC services on top of a
// store.Store, the long-poll dispatch path, and the liveness loops (failure
// detector and lease reaper).
package coordinator

import (
	"context"
	"errors"
	"log/slog"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/metrics"
	"github.com/akshajnad/relay/internal/rpc"
	"github.com/akshajnad/relay/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	// PollTimeout bounds how long AcquireJob blocks when no job is ready.
	PollTimeout time.Duration
	// HeartbeatInterval is what workers are told to heartbeat at, and how
	// often the failure detector runs.
	HeartbeatInterval time.Duration
	// DeadAfter is how long a worker may go without heartbeating before it
	// is declared dead and its leases are revoked.
	DeadAfter time.Duration
	// ReapInterval is how often expired leases are swept.
	ReapInterval time.Duration
	// StartupGrace delays the failure detector and reaper after startup so
	// workers can heartbeat again after a coordinator restart, instead of
	// being declared dead from timestamps that went stale while the
	// coordinator was down. Safety never depends on it, only efficiency.
	StartupGrace time.Duration
	// MaxPayloadBytes caps SubmitJob payloads.
	MaxPayloadBytes int
	// StoreTimeout bounds each store call.
	StoreTimeout time.Duration
}

func DefaultConfig() Config {
	return Config{
		PollTimeout:       5 * time.Second,
		HeartbeatInterval: time.Second,
		DeadAfter:         3 * time.Second,
		ReapInterval:      500 * time.Millisecond,
		StartupGrace:      4 * time.Second,
		MaxPayloadBytes:   1 << 20,
		StoreTimeout:      5 * time.Second,
	}
}

// Coordinator owns the dispatch state shared by both gRPC services.
type Coordinator struct {
	store    store.Store
	cfg      Config
	notifier *Notifier
	metrics  *metrics.Registry
	log      *slog.Logger
}

func New(st store.Store, cfg Config, m *metrics.Registry, log *slog.Logger) *Coordinator {
	return &Coordinator{store: st, cfg: cfg, notifier: NewNotifier(), metrics: m, log: log}
}

// Register attaches JobService and WorkerService to s.
func (c *Coordinator) Register(s *grpc.Server) {
	relayv1.RegisterJobServiceServer(s, &jobService{c: c})
	relayv1.RegisterWorkerServiceServer(s, &workerService{c: c})
}

// Shutdown wakes every long-poll so in-flight AcquireJob calls return and
// the gRPC server can stop gracefully.
func (c *Coordinator) Shutdown() { c.notifier.Close() }

// storeCtx detaches a store call from the RPC's cancellation. A transition
// that has been sent (a Lua script, a Raft proposal) runs to completion in
// the store regardless, so abandoning the reply on client cancel would only
// hide what happened (for acquire, a lease nobody knows about). The call
// still has its own timeout.
func (c *Coordinator) storeCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), c.cfg.StoreTimeout)
}

func storeError(err error) error {
	var stale *store.StaleLeaseError
	var notLeader *store.NotLeaderError
	switch {
	case err == nil:
		return nil
	case errors.As(err, &notLeader):
		return rpc.NotLeaderError(notLeader.LeaderID, notLeader.LeaderAddr)
	case errors.As(err, &stale):
		return status.Error(codes.Aborted, stale.Error())
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, store.ErrWorkerNotAlive):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Unavailable, "store: "+err.Error())
}

var statusToProto = map[string]relayv1.JobStatus{
	store.StatusPending:   relayv1.JobStatus_JOB_STATUS_PENDING,
	store.StatusLeased:    relayv1.JobStatus_JOB_STATUS_LEASED,
	store.StatusSucceeded: relayv1.JobStatus_JOB_STATUS_SUCCEEDED,
	store.StatusFailed:    relayv1.JobStatus_JOB_STATUS_FAILED,
	store.StatusDead:      relayv1.JobStatus_JOB_STATUS_DEAD,
}

func jobToProto(j *store.Job) *relayv1.Job {
	return &relayv1.Job{
		Id:               j.ID,
		Type:             j.Type,
		Payload:          j.Payload,
		IdempotencyKey:   j.IdempotencyKey,
		Status:           statusToProto[j.Status],
		Attempts:         int32(j.Attempts),
		MaxAttempts:      int32(j.MaxAttempts),
		LeaseOwner:       j.LeaseOwner,
		LeaseExpiresAtMs: j.LeaseExpiresAt,
		LeaseToken:       j.LeaseToken,
		CreatedAtMs:      j.CreatedAt,
		EnqueuedAtMs:     j.EnqueuedAt,
		LeasedAtMs:       j.LeasedAt,
		FinishedAtMs:     j.FinishedAt,
		LastError:        j.LastError,
		Result:           j.Result,
	}
}
