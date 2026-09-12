// Command worker connects to a Relay coordinator, registers, heartbeats,
// and runs jobs with the built-in handlers (noop, sleep, fail-n-times).
// SIGTERM stops acquiring and drains in-flight jobs.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/envflag"
	"github.com/akshajnad/relay/internal/history"
	"github.com/akshajnad/relay/internal/logx"
	"github.com/akshajnad/relay/internal/rpc"
	"github.com/akshajnad/relay/internal/tracing"
	"github.com/akshajnad/relay/internal/worker"
	"google.golang.org/grpc"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := worker.DefaultConfig()
	host, _ := os.Hostname()
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	addr := fs.String("coordinator", "localhost:50051", "coordinator gRPC address, or a list id=addr,id=addr (follows the leader)")
	sleep := fs.Duration("sleep-duration", 20*time.Millisecond, "default duration for the sleep handler")
	logLevel := fs.String("log-level", "info", "debug|info|warn|error")
	logFormat := fs.String("log-format", "json", "json|text")
	historyDir := fs.String("history-dir", "", "record every RPC to a JSONL history file in this directory (chaos testing)")
	otlp := fs.String("otlp-endpoint", "", "OTLP gRPC endpoint (host:port) for traces; empty disables tracing")
	historyRPCs := fs.Bool("history-rpcs", true, "with --history-dir, record every RPC (false: only handler start/end, for benchmarks)")
	fs.IntVar(&cfg.Concurrency, "concurrency", 1, "jobs to run in parallel")
	fs.StringVar(&cfg.Hostname, "hostname", host, "hostname reported to the coordinator")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", cfg.ShutdownTimeout, "how long to drain in-flight jobs on SIGTERM")
	fs.DurationVar(&cfg.ReportTimeout, "report-timeout", cfg.ReportTimeout, "how long to retry reporting a result through transient errors")
	if err := envflag.Parse(fs, "RELAY", os.Args[1:]); err != nil {
		return err
	}
	log := logx.New(os.Stderr, *logLevel, *logFormat)

	shutdownTracing, err := tracing.Setup(context.Background(), "relay-worker", *otlp, 1)
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = shutdownTracing(ctx)
	}()
	dialOpts := []grpc.DialOption{tracing.DialOption()}
	if *historyDir != "" {
		rec, err := history.Open(*historyDir, "worker")
		if err != nil {
			return err
		}
		defer rec.Close()
		if *historyRPCs {
			dialOpts = append(dialOpts, grpc.WithChainUnaryInterceptor(history.UnaryClientInterceptor(rec)))
		}
		// Handler start and end, for benchmark timing.
		cfg.OnExecute = func(job worker.Job, token int64, start bool) {
			m := history.ExecEnd
			if start {
				m = history.ExecStart
			}
			rec.Note(history.Record{Method: m, JobID: job.ID, Worker: job.WorkerID, Token: token, Attempts: job.Attempt})
		}
		log.Info("recording history", "file", rec.Process())
	}
	conn, err := rpc.DialCluster(*addr, dialOpts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	w := worker.New(relayv1.NewWorkerServiceClient(conn), worker.DefaultHandlers(*sleep), cfg, log)
	log.Info("worker starting", "coordinator", *addr, "concurrency", cfg.Concurrency, "hostname", cfg.Hostname)
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		return err
	}
	log.Info("worker stopped")
	return nil
}
