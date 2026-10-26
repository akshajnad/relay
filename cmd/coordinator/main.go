// Command coordinator runs the Relay gRPC server: job submission, the
// long-poll dispatch path, worker liveness, and the lease reaper.
//
// With --store redis (the default) all durable state lives in Redis, so a
// restarted coordinator loses nothing. With --store raft, 3 or 5
// coordinators replicate the state among themselves with Raft; only the
// leader serves, and the others redirect clients to it.
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
	"strings"
	"syscall"
	"time"

	"github.com/akshajnad/relay/internal/coordinator"
	"github.com/akshajnad/relay/internal/envflag"
	"github.com/akshajnad/relay/internal/logx"
	"github.com/akshajnad/relay/internal/metrics"
	"github.com/akshajnad/relay/internal/store"
	raftstore "github.com/akshajnad/relay/internal/store/raft"
	redisstore "github.com/akshajnad/relay/internal/store/redis"
	"github.com/akshajnad/relay/internal/tracing"
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
	metricsListen := fs.String("metrics-listen", ":9090", "HTTP address for /metrics (Prometheus), /metrics.json and /debug/pprof")
	storeKind := fs.String("store", "redis", "state backend: redis or raft")
	redisAddr := fs.String("redis-addr", "localhost:6379", "Redis address")
	rcfg := raftstore.DefaultConfig()
	peers := fs.String("peers", "", "raft: cluster members as id=raft_addr=grpc_addr,... (used to bootstrap and for leader hints)")
	fs.StringVar(&rcfg.NodeID, "node-id", "", "raft: this node's server id")
	fs.StringVar(&rcfg.RaftBind, "raft-bind", ":7000", "raft: address the Raft transport listens on")
	fs.StringVar(&rcfg.RaftAdvertise, "raft-advertise", "", "raft: address peers reach this node's Raft transport on")
	fs.StringVar(&rcfg.GRPCAdvertise, "grpc-advertise", "", "raft: address clients reach this node's gRPC server on")
	fs.StringVar(&rcfg.DataDir, "data-dir", "/data", "raft: directory for raft.db and snapshots")
	fs.BoolVar(&rcfg.Bootstrap, "bootstrap", false, "raft: bootstrap the cluster from --peers if this node has no state")
	fs.DurationVar(&rcfg.HeartbeatTimeout, "raft-heartbeat-timeout", rcfg.HeartbeatTimeout, "raft: follower heartbeat timeout")
	fs.DurationVar(&rcfg.ElectionTimeout, "raft-election-timeout", rcfg.ElectionTimeout, "raft: candidate election timeout")
	fs.DurationVar(&rcfg.LeaderLeaseTimeout, "raft-leader-lease-timeout", rcfg.LeaderLeaseTimeout, "raft: how long a leader serves without quorum contact before stepping down")
	fs.Uint64Var(&rcfg.SnapshotThreshold, "raft-snapshot-threshold", rcfg.SnapshotThreshold, "raft: log entries between snapshots")
	fs.Uint64Var(&rcfg.TrailingLogs, "raft-trailing-logs", rcfg.TrailingLogs, "raft: log entries kept after a snapshot")
	redisPool := fs.Int("redis-pool-size", 256, "Redis connection pool size (needs headroom for concurrent long-polls)")
	missed := fs.Int("missed-heartbeats", 3, "heartbeats a worker may miss before it is declared dead")
	grace := fs.Duration("startup-grace", 0, "delay before failure detection after startup (0 = dead-after + one heartbeat interval)")
	shutdownTimeout := fs.Duration("shutdown-timeout", 10*time.Second, "graceful shutdown limit")
	logLevel := fs.String("log-level", "info", "debug|info|warn|error")
	logFormat := fs.String("log-format", "json", "json|text")
	otlp := fs.String("otlp-endpoint", "", "OTLP gRPC endpoint (host:port) for traces, e.g. jaeger:4317; empty disables tracing")
	sample := fs.Float64("trace-sample-ratio", 1, "fraction of new traces to sample")
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

	var st store.Store
	switch *storeKind {
	case "redis":
		rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, PoolSize: *redisPool})
		defer rdb.Close()
		rs := redisstore.New(rdb, scfg)
		if err := waitForRedis(ctx, rs, log); err != nil {
			return err
		}
		st = rs
	case "raft":
		rcfg.Store = scfg
		ps, err := parsePeers(*peers)
		if err != nil {
			return err
		}
		rcfg.Peers = ps
		if rcfg.NodeID == "" {
			return errors.New("--node-id is required with --store raft")
		}
		rs, err := raftstore.Open(rcfg, log)
		if err != nil {
			return err
		}
		defer func() {
			if err := rs.Close(); err != nil {
				log.Error("raft shutdown", "err", err)
			}
		}()
		st = rs
	default:
		return fmt.Errorf("unknown --store %q (want redis or raft)", *storeKind)
	}
	if !store.FencingEnabled {
		log.Error("THIS BINARY WAS BUILT WITH relay_nofence: FENCING IS DISABLED. It exists only to test the chaos checker.")
	}

	shutdownTracing, err := tracing.Setup(ctx, "relay-coordinator", *otlp, *sample)
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = shutdownTracing(sctx)
	}()

	m := metrics.NewRegistry()
	src := metrics.PromSources{QueueDepth: func(ctx context.Context) (int64, int64, error) {
		d, err := st.QueueDepth(ctx)
		return d.Pending, d.Leased, err
	}}
	if l, ok := st.(store.Leadership); ok {
		src.IsLeader = l.IsLeader
	}
	prom := m.Prometheus(src)
	if rs, ok := st.(*raftstore.Store); ok {
		rs.SetCommitObserver(m.RaftCommit.Record)
	}
	c := coordinator.New(st, ccfg, m, log)

	srv := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
		tracing.ServerOption(),
	)
	c.Register(srv)
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	httpSrv := &http.Server{Addr: *metricsListen, Handler: m.Handler(prom), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics server", "err", err)
		}
	}()

	livenessDone := make(chan struct{})
	go func() { c.RunLiveness(ctx); close(livenessDone) }()

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	log.Info("coordinator listening", "grpc", *listen, "metrics", *metricsListen, "store", *storeKind, "node", rcfg.NodeID,
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

// parsePeers parses id=raft_addr=grpc_addr,...
func parsePeers(spec string) ([]raftstore.Peer, error) {
	var out []raftstore.Peer
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		f := strings.Split(part, "=")
		if len(f) != 3 {
			return nil, fmt.Errorf("bad peer %q: want id=raft_addr=grpc_addr", part)
		}
		out = append(out, raftstore.Peer{ID: f[0], RaftAddr: f[1], GRPCAddr: f[2]})
	}
	return out, nil
}

func waitForRedis(ctx context.Context, st *redisstore.Store, log *slog.Logger) error {
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
