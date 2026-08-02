// Package store defines the Store interface every Relay backend implements,
// and the types its state transitions use. The coordinator depends only on
// this package; backends live in store/redis and store/raft, and
// store/storetest is the conformance suite both must pass.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Job statuses.
const (
	StatusPending   = "PENDING"
	StatusLeased    = "LEASED"
	StatusSucceeded = "SUCCEEDED"
	StatusFailed    = "FAILED"
	StatusDead      = "DEAD"
)

// Worker states.
const (
	WorkerAlive = "ALIVE"
	WorkerDead  = "DEAD"
)

var (
	ErrNotFound       = errors.New("job not found")
	ErrWorkerNotAlive = errors.New("worker not registered or declared dead")
)

// StaleLeaseError is returned when a lease holder reports on a lease it no
// longer owns: the job was reassigned, requeued, or already finished.
type StaleLeaseError struct {
	JobID        string
	Status       string
	CurrentOwner string
	CurrentToken int64
}

func (e *StaleLeaseError) Error() string {
	return fmt.Sprintf("stale lease on job %s: now %s, owner %q, token %d",
		e.JobID, e.Status, e.CurrentOwner, e.CurrentToken)
}

// NotLeaderError is returned by a replicated store when this node cannot
// serve the call because it is not the leader. The call was not executed,
// so the caller may retry it against the leader. LeaderID and LeaderAddr
// are empty when no leader is known.
type NotLeaderError struct {
	LeaderID   string
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "not the leader; no leader known"
	}
	return fmt.Sprintf("not the leader; leader is %s at %s", e.LeaderID, e.LeaderAddr)
}

// Job is one job's state. Times are store-clock milliseconds since the
// Unix epoch.
type Job struct {
	ID             string
	Type           string
	Payload        []byte
	IdempotencyKey string
	Status         string
	Attempts       int
	MaxAttempts    int
	LeaseOwner     string
	LeaseExpiresAt int64
	LeaseToken     int64
	CreatedAt      int64
	// EnqueuedAt is when the job last became eligible to run.
	EnqueuedAt int64
	LeasedAt   int64
	FinishedAt int64
	LastError  string
	Result     []byte
}

// Event is one entry of a job's transition history.
type Event struct {
	AtMs   int64  `json:"at"`
	Kind   string `json:"kind"`
	Worker string `json:"worker"`
	Token  int64  `json:"token"`
	Detail string `json:"detail"`
}

// Terminal reports whether an event kind ends a job.
func Terminal(kind string) bool {
	return kind == StatusSucceeded || kind == StatusFailed || kind == StatusDead
}

// Lease identifies one lease on one job.
type Lease struct {
	JobID string
	Token int64
}

// Worker is one registered worker.
type Worker struct {
	ID            string
	Hostname      string
	Concurrency   int
	State         string
	RegisteredAt  int64
	LastHeartbeat int64
	ActiveJobs    int
}

type Config struct {
	LeaseDuration      time.Duration
	BackoffBase        time.Duration
	BackoffMax         time.Duration
	DefaultMaxAttempts int
	// ScanBatch bounds how many leases or workers one reaper or detector
	// pass processes per store round trip.
	ScanBatch int
}

func DefaultConfig() Config {
	return Config{
		LeaseDuration:      10 * time.Second,
		BackoffBase:        200 * time.Millisecond,
		BackoffMax:         30 * time.Second,
		DefaultMaxAttempts: 3,
		ScanBatch:          256,
	}
}

type SubmitParams struct {
	Type           string
	Payload        []byte
	IdempotencyKey string
	MaxAttempts    int
}

// Acquired is the outcome of one Acquire call.
type Acquired struct {
	// Job is nil when nothing was eligible.
	Job *Job
	// StoreNowMs is the store's clock when the acquire ran.
	StoreNowMs int64
	// EligibleAtMs is when the leased job became eligible (store clock).
	EligibleAtMs int64
	// More is true when another job was already eligible after this one
	// was leased.
	More bool
	// NextEligibleIn is how long until the earliest not-yet-eligible job
	// becomes eligible, or -1 if the ready queue is empty. Only set when
	// Job is nil.
	NextEligibleIn time.Duration
}

// FailOutcome describes where a failed attempt left the job.
type FailOutcome struct {
	Status  string // PENDING (will retry), FAILED or DEAD
	RetryIn time.Duration
	// Already is true when this lease had reported its failure before.
	Already bool
}

// RevokeResult counts what happened to revoked leases.
type RevokeResult struct {
	Requeued int
	Dead     int
}

// Depth is a point-in-time count of non-terminal jobs.
type Depth struct {
	// Pending counts PENDING jobs, including ones waiting out a backoff.
	Pending int64
	Leased  int64
}

// Store is every state transition Relay makes. Each method is atomic: it
// either happens entirely or not at all, and transitions on one job are
// totally ordered.
type Store interface {
	// Submit creates a job, or returns the existing job's id with
	// duplicate=true if the idempotency key was seen before.
	Submit(ctx context.Context, p SubmitParams) (id string, duplicate bool, err error)
	// RegisterWorker adds a live worker and returns its id.
	RegisterWorker(ctx context.Context, hostname string, concurrency int) (string, error)
	// Acquire leases the oldest eligible job to workerID. Every lease gets a
	// fencing token greater than any earlier token on the same job.
	Acquire(ctx context.Context, workerID string) (Acquired, error)
	// Renew records liveness for workerID and renews the leases it reports
	// holding. It returns the reported leases that could not be renewed.
	Renew(ctx context.Context, workerID string, held []Lease) (lost []Lease, err error)
	// Complete marks the job SUCCEEDED if workerID still holds lease token.
	// already is true when this lease had completed the job before (a
	// retried call); the stored result is not rewritten.
	Complete(ctx context.Context, jobID, workerID string, token int64, result []byte) (already bool, err error)
	// Fail records a failed attempt if workerID still holds lease token.
	Fail(ctx context.Context, jobID, workerID string, token int64, errMsg string, retryable bool) (FailOutcome, error)
	// Release returns a just-acquired lease that could not be delivered to
	// the worker. The attempt does not count.
	Release(ctx context.Context, jobID, workerID string, token int64) error
	// ReapExpired requeues (or dead-letters) every job whose lease expired.
	ReapExpired(ctx context.Context) (RevokeResult, error)
	// ExpireWorkers declares dead every worker that has not renewed within
	// deadAfter and revokes all of its leases. It returns the expired ids.
	ExpireWorkers(ctx context.Context, deadAfter time.Duration) (RevokeResult, []string, error)
	// Get returns a job and its transition history.
	Get(ctx context.Context, id string) (*Job, []Event, error)
	// ListWorkers returns every worker ever registered, live or dead.
	ListWorkers(ctx context.Context) ([]Worker, error)
	// QueueDepth counts pending and leased jobs.
	QueueDepth(ctx context.Context) (Depth, error)
	Config() Config
}

// Leadership is implemented by replicated stores. Only the leader serves
// calls; the others return *NotLeaderError.
type Leadership interface {
	// LeaderChanges delivers true when this node becomes leader (and is
	// ready to serve) and false when it stops being leader.
	LeaderChanges() <-chan bool
	// IsLeader reports whether this node currently serves calls.
	IsLeader() bool
}
