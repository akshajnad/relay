// Command asynq-worker is the benchmark's external baseline: an
// idiomatic github.com/hibiken/asynq server running the same noop and
// sleep handlers as Relay's worker, and recording the same handler
// start/end notes (internal/history) so both are timed identically.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akshajnad/relay/internal/envflag"
	"github.com/akshajnad/relay/internal/history"
	"github.com/hibiken/asynq"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "asynq-worker:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("asynq-worker", flag.ExitOnError)
	redisAddr := fs.String("redis-addr", "redis:6379", "Redis address")
	concurrency := fs.Int("concurrency", 10, "tasks processed in parallel")
	checkInterval := fs.Duration("task-check-interval", 0, "asynq TaskCheckInterval (0 = asynq's default, 1s)")
	historyDir := fs.String("history-dir", "", "record handler start/end notes in this directory")
	if err := envflag.Parse(fs, "ASYNQ", os.Args[1:]); err != nil {
		return err
	}

	var rec *history.Recorder
	if *historyDir != "" {
		r, err := history.Open(*historyDir, "asynq-worker")
		if err != nil {
			return err
		}
		defer r.Close()
		rec = r
	}
	host, _ := os.Hostname()
	note := func(m string, ctx context.Context, t *asynq.Task) {
		if rec == nil {
			return
		}
		id, _ := asynq.GetTaskID(ctx)
		retried, _ := asynq.GetRetryCount(ctx)
		rec.Note(history.Record{Method: m, JobID: id, Worker: host, Attempts: retried + 1})
	}

	mux := asynq.NewServeMux()
	mux.HandleFunc("noop", func(ctx context.Context, t *asynq.Task) error {
		note(history.ExecStart, ctx, t)
		note(history.ExecEnd, ctx, t)
		return nil
	})
	mux.HandleFunc("sleep", func(ctx context.Context, t *asynq.Task) error {
		note(history.ExecStart, ctx, t)
		defer note(history.ExecEnd, ctx, t)
		var p struct {
			DurationMs int64 `json:"duration_ms"`
		}
		if err := json.Unmarshal(t.Payload(), &p); err != nil {
			return fmt.Errorf("bad payload: %v: %w", err, asynq.SkipRetry)
		}
		select {
		case <-time.After(time.Duration(p.DurationMs) * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	srv := asynq.NewServer(asynq.RedisClientOpt{Addr: *redisAddr}, asynq.Config{
		Concurrency:       *concurrency,
		TaskCheckInterval: *checkInterval,
		Queues:            map[string]int{"default": 1},
	})
	if err := srv.Start(mux); err != nil {
		return err
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Shutdown()
	return nil
}
