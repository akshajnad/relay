package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Job is what a handler sees of the job it is running.
type Job struct {
	ID          string
	Type        string
	Payload     []byte
	Attempt     int // 1-based: the first run is attempt 1
	MaxAttempts int
	WorkerID    string
}

// Handler runs one job. A nil error completes the job with the returned
// result. An error fails the attempt: it is retried with backoff unless it
// is wrapped with Permanent.
//
// Handlers must tolerate running more than once for the same job, because
// execution is at-least-once: a worker that crashes or stalls has its job
// run again elsewhere.
type Handler func(ctx context.Context, job Job) ([]byte, error)

// Registry maps job types to handlers.
type Registry map[string]Handler

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks err as non-retryable: the job goes straight to FAILED.
func Permanent(err error) error { return permanentError{err} }

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

func (r Registry) run(ctx context.Context, job Job) (result []byte, err error) {
	h, ok := r[job.Type]
	if !ok {
		return nil, noHandler(job.Type)
	}
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("handler panicked: %v", p)
		}
	}()
	return h(ctx, job)
}

// DefaultHandlers returns the built-in handlers:
//
//	noop          completes immediately
//	sleep         sleeps for payload {"duration_ms": N}, or defaultSleep
//	fail-n-times  fails attempts 1..N of payload {"n": N}, then succeeds
//
// Each result records which worker produced it and on which attempt, so a
// result overwritten by a late duplicate would be detectable.
func DefaultHandlers(defaultSleep time.Duration) Registry {
	return Registry{
		"noop": func(_ context.Context, job Job) ([]byte, error) {
			return resultFor(job, nil)
		},
		"sleep": func(ctx context.Context, job Job) ([]byte, error) {
			var p struct {
				DurationMs *int64 `json:"duration_ms"`
			}
			if err := decode(job.Payload, &p); err != nil {
				return nil, err
			}
			d := defaultSleep
			if p.DurationMs != nil {
				d = time.Duration(*p.DurationMs) * time.Millisecond
			}
			t := time.NewTimer(d)
			defer t.Stop()
			select {
			case <-t.C:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return resultFor(job, map[string]any{"slept_ms": d.Milliseconds()})
		},
		"fail-n-times": func(_ context.Context, job Job) ([]byte, error) {
			var p struct {
				N int `json:"n"`
			}
			if err := decode(job.Payload, &p); err != nil {
				return nil, err
			}
			if job.Attempt <= p.N {
				return nil, fmt.Errorf("planned failure %d of %d", job.Attempt, p.N)
			}
			return resultFor(job, nil)
		},
	}
}

func decode(payload []byte, v any) error {
	if len(payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(payload, v); err != nil {
		return Permanent(fmt.Errorf("invalid payload: %w", err))
	}
	return nil
}

func resultFor(job Job, extra map[string]any) ([]byte, error) {
	out := map[string]any{"worker_id": job.WorkerID, "attempt": job.Attempt}
	for k, v := range extra {
		out[k] = v
	}
	return json.Marshal(out)
}
