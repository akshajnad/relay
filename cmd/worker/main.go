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
	"github.com/akshajnad/relay/internal/logx"
	"github.com/akshajnad/relay/internal/rpc"
	"github.com/akshajnad/relay/internal/worker"
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
	addr := fs.String("coordinator", "localhost:50051", "coordinator gRPC address")
	sleep := fs.Duration("sleep-duration", 20*time.Millisecond, "default duration for the sleep handler")
	logLevel := fs.String("log-level", "info", "debug|info|warn|error")
	logFormat := fs.String("log-format", "json", "json|text")
	fs.IntVar(&cfg.Concurrency, "concurrency", 1, "jobs to run in parallel")
	fs.StringVar(&cfg.Hostname, "hostname", host, "hostname reported to the coordinator")
	fs.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", cfg.ShutdownTimeout, "how long to drain in-flight jobs on SIGTERM")
	fs.DurationVar(&cfg.ReportTimeout, "report-timeout", cfg.ReportTimeout, "how long to retry reporting a result through transient errors")
	if err := envflag.Parse(fs, "RELAY", os.Args[1:]); err != nil {
		return err
	}
	log := logx.New(os.Stderr, *logLevel, *logFormat)

	conn, err := rpc.Dial(*addr)
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
