package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/history"
	"github.com/akshajnad/relay/internal/rpc"
	"github.com/hibiken/asynq"
)

// Job states reported by jobClient.State.
const (
	statePending   = "pending"
	stateActive    = "active"
	stateSucceeded = "succeeded"
	stateFailed    = "failed"
	stateDead      = "dead"
)

// jobClient submits jobs to one backend and reads their state. key is a
// caller-chosen unique id: Relay uses it as the idempotency key, asynq as
// the TaskID (asynq's own deduplication).
type jobClient interface {
	Submit(ctx context.Context, key, typ string, payload []byte, maxAttempts int) (id string, err error)
	// State returns the job's state and, where the backend records it, the
	// number of terminal transitions in its history (-1 if not observable).
	State(ctx context.Context, id string) (state string, terminal int, err error)
	// LiveWorkers counts worker processes the backend considers alive.
	LiveWorkers(ctx context.Context) (int, error)
	Close() error
}

// newJobClient connects to a backend. retain makes asynq keep completed
// tasks inspectable (only the fault phase needs it; asynq's default, used
// everywhere else, deletes them on completion).
func newJobClient(kind, target string, retain bool) (jobClient, error) {
	switch kind {
	case "relay":
		conn, err := rpc.DialCluster(target)
		if err != nil {
			return nil, err
		}
		return &relayClient{conn: conn, jobs: relayv1.NewJobServiceClient(conn), workers: relayv1.NewWorkerServiceClient(conn)}, nil
	case "asynq":
		opt := asynq.RedisClientOpt{Addr: target}
		return &asynqClient{client: asynq.NewClient(opt), insp: asynq.NewInspector(opt), retain: retain}, nil
	}
	return nil, fmt.Errorf("unknown client kind %q", kind)
}

type relayClient struct {
	conn    *rpc.ClusterConn
	jobs    relayv1.JobServiceClient
	workers relayv1.WorkerServiceClient
}

func (c *relayClient) LiveWorkers(ctx context.Context) (int, error) {
	resp, err := c.workers.ListWorkers(ctx, &relayv1.ListWorkersRequest{})
	if err != nil {
		return 0, err
	}
	n := 0
	for _, w := range resp.Workers {
		if w.State == "ALIVE" {
			n++
		}
	}
	return n, nil
}

func (c *relayClient) Submit(ctx context.Context, key, typ string, payload []byte, maxAttempts int) (string, error) {
	resp, err := c.jobs.SubmitJob(ctx, &relayv1.SubmitJobRequest{Type: typ, Payload: payload, IdempotencyKey: key, MaxAttempts: int32(maxAttempts)})
	if err != nil {
		return "", err
	}
	return resp.JobId, nil
}

func (c *relayClient) State(ctx context.Context, id string) (string, int, error) {
	resp, err := c.jobs.GetJob(ctx, &relayv1.GetJobRequest{JobId: id})
	if err != nil {
		return "", 0, err
	}
	terminal := 0
	for _, e := range resp.History {
		switch e.Kind {
		case "SUCCEEDED", "FAILED", "DEAD":
			terminal++
		}
	}
	return strings.ToLower(history.StatusName(resp.Job.Status)), terminal, nil
}

func (c *relayClient) Close() error { return c.conn.Close() }

// asynqClient uses asynq's client and inspector the documented way.
type asynqClient struct {
	client *asynq.Client
	insp   *asynq.Inspector
	retain bool
}

func (c *asynqClient) Submit(ctx context.Context, key, typ string, payload []byte, maxAttempts int) (string, error) {
	opts := []asynq.Option{asynq.TaskID(key), asynq.MaxRetry(max(maxAttempts-1, 0)), asynq.Queue("default")}
	if c.retain {
		opts = append(opts, asynq.Retention(time.Hour))
	}
	_, err := c.client.EnqueueContext(ctx, asynq.NewTask(typ, payload), opts...)
	if errors.Is(err, asynq.ErrTaskIDConflict) {
		return key, nil // already enqueued: the same task
	}
	if err != nil {
		return "", err
	}
	return key, nil
}

func (c *asynqClient) State(_ context.Context, id string) (string, int, error) {
	info, err := c.insp.GetTaskInfo("default", id)
	if err != nil {
		return "", 0, err
	}
	state := statePending
	switch info.State {
	case asynq.TaskStateActive:
		state = stateActive
	case asynq.TaskStateCompleted:
		state = stateSucceeded
	case asynq.TaskStateArchived:
		state = stateDead
	}
	// asynq does not record a transition history, so duplicate terminal
	// transitions cannot be observed.
	return state, -1, nil
}

func (c *asynqClient) LiveWorkers(context.Context) (int, error) {
	servers, err := c.insp.Servers()
	if err != nil {
		return 0, err
	}
	n := 0
	for _, s := range servers {
		if s.Status == "active" {
			n++
		}
	}
	return n, nil
}

func (c *asynqClient) Close() error {
	c.insp.Close()
	return c.client.Close()
}
