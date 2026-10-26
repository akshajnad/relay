// Command coordinator runs the Relay gRPC server: job submission, the
// long-poll dispatch path, worker liveness, and the lease reaper. All
// durable state lives in Redis, so a restarted coordinator loses nothing.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akshajnad/relay/internal/coordinator"
	"github.com/akshajnad/relay/internal/envflag"
	"github.com/akshajnad/relay/internal/logx"
	"github.com/akshajnad/relay/internal/metrics"
	"github.com/akshajnad/relay/internal/store"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "coordinator:", err)
		os.Exit(1)
	}
}

func run() error {
	ccfg := coordinator.DefaultConfig()
	scfg := store.DefaultConfig()
	fs := flag.NewFlagSet("coordinator", flag.ExitOnError)
	listen := fs.String("listen", ":50051", "gRPC listen address")
	metricsListen := fs.String("metrics-listen", ":9090", "HTTP address for /metrics and /debug/pprof")
	redisAddr := fs.String("redis-addr", "localhost:6379", "Redis address")
	redisPool := fs.Int("redis-pool-size", 256, "Redis connection pool size (needs headroom for concurrent long-polls)")
	missed := fs.Int("missed-heartbeats", 3, "heartbeats a worker may miss before it is declared dead")
	grace := fs.Duration("startup-grace", 0, "delay before failure detection after startup (0 = dead-after + one heartbeat interval)")
	shutdownTimeout := fs.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown limit")
	logLevel := fs.String("log-level", "info", "debug|info|warn|error")
	logFormat := fs.String("log-format", "json", "json|text")
	fs.DurationVar(&ccfg.PollTimeout, "poll-timeout", ccfg.PollTimeout, "AcquireJob long-poll timeout")
	fs.DurationVar(&ccfg.HeartbeatInterval, "heartbeat-interval", ccfg.HeartbeatInterval, "worker heartbeat interval")
	fs.DurationVar(&ccfg.ReapInterval, "reap-interval", ccfg.ReapInterval, "expired-lease sweep interval")
	fs.DurationVar(&scfg.LeaseDuration, "lease-duration", scfg.LeaseDuration, "lease length; renewed by every heartbeat")
	fs.DurationVar(&scfg.BackoffBase, "backoff-base", scfg.BackoffBase, "retry backoff base")
	fs.DurationVar(&scfg.BackoffMax, "backoff-max", scfg.BackoffMax, "retry backoff cap")
	fs.IntVar(&scfg.DefaultMaxAttempts, "default-max-attempts", scfg.DefaultMaxAttempts, "max_attempts for jobs submitted without one")
	if err := envflag.Parse(fs, "RELAY", os.Args[1:]); err != nil {
		return err
	}
	ccfg.DeadAfter = time.Duration(*missed) * ccfg.HeartbeatInterval
	ccfg.StartupGrace = *grace
	if ccfg.StartupGrace == 0 {
		ccfg.StartupGrace = ccfg.DeadAfter + ccfg.HeartbeatInterval
	}
	if scfg.LeaseDuration <= ccfg.DeadAfter {
		return fmt.Errorf("lease-duration (%v) must exceed the failure-detection window (%v)", scfg.LeaseDuration, ccfg.DeadAfter)
	}

	log := logx.New(os.Stderr, *logLevel, *logFormat)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, PoolSize: *redisPool})
	defer rdb.Close()
	st := store.New(rdb, scfg)
	if err := waitForRedis(ctx, st, log); err != nil {
		return err
	}

	m := metrics.NewRegistry()
	c := coordinator.New(st, ccfg, m, log)

	srv := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
	)
	c.Register(srv)
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	httpSrv := &http.Server{Addr: *metricsListen, Handler: m.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()

	livenessDone := make(chan struct{})
	go func() { c.RunLiveness(ctx); close(livenessDone) }()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	log.Info("coordinator listening", "grpc", *listen, "metrics", *metricsListen, "redis", *redisAddr,
		"lease", scfg.LeaseDuration, "heartbeat_interval", ccfg.HeartbeatInterval, "dead_after", ccfg.DeadAfter)

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	hs.Shutdown()
	c.Shutdown() // release long-polls so GracefulStop can finish
	stopped := make(chan struct{})
	go func() { srv.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(*shutdownTimeout):
		srv.Stop()
	}
	<-livenessDone
	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	return nil
}

func waitForRedis(ctx context.Context, st *store.Store, log *slog.Logger) error {
	backoff := 100 * time.Millisecond
	for {
		err := st.LoadScripts(ctx)
		if err == nil {
			return nil
		}
		log.Warn("waiting for redis", "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 2*time.Second)
	}
}
