// Command relay-dash is a live dashboard for a local Relay cluster.
//
// It watches the cluster through Relay's public interfaces only: every
// coordinator's ClusterStatus and /metrics, ListWorkers on the leader, and
// GetJob for the jobs the dashboard submitted itself. It streams what it
// sees to a web UI: the coordinators and their Raft state, each worker's
// slots and heartbeat age against the failure detector's deadline, the job
// state machine with live transition rates, a per-worker timeline of lease
// spans with their fencing tokens, and windowed latency percentiles.
//
// It can also drive the cluster through docker compose: launch it with a
// configuration entered in the UI, scale workers, generate load, inject
// faults (kill, pause, network isolation), and run guided failure
// scenarios that narrate what the engine does in response.
//
//	go run ./cmd/relay-dash          # then open http://127.0.0.1:8080
//
// Run it from the repository root (it reads the compose files in deploy/).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/akshajnad/relay/internal/envflag"
	"github.com/akshajnad/relay/internal/logx"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "relay-dash:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("relay-dash", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:8080", "HTTP address the dashboard serves on")
	composeDir := fs.String("compose-dir", "deploy", "directory holding Relay's compose files")
	attach := fs.String("attach", "auto", "cluster to watch at startup: auto (whichever compose project is running), raft, redis or none")
	coordinator := fs.String("coordinator", "", "watch an external cluster instead: gRPC address, or id=addr,... (turns off docker controls)")
	metricsSpec := fs.String("metrics", "", "with --coordinator: the coordinators' metrics addresses as id=host:port,... (or one host:port)")
	logLevel := fs.String("log-level", "info", "debug|info|warn|error")
	if err := envflag.Parse(fs, "RELAY_DASH", os.Args[1:]); err != nil {
		return err
	}
	log := logx.New(os.Stderr, *logLevel, "text")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	d := newDash(ctx, log)
	if *coordinator != "" {
		l, err := externalLayout(*coordinator, *metricsSpec)
		if err != nil {
			return err
		}
		if err := d.attach(l, nil); err != nil {
			return err
		}
	} else {
		dk := &docker{dir: *composeDir}
		if _, err := os.Stat(dk.path(layouts["raft"].File)); err != nil {
			return fmt.Errorf("compose files not found in %q (run from the repository root, or set --compose-dir): %w", *composeDir, err)
		}
		if err := dk.available(ctx); err != nil {
			log.Warn("docker is not available; cluster launch and fault injection are off", "err", err)
		} else {
			d.docker = dk
		}
		if err := d.attachAtStartup(ctx, *attach); err != nil {
			return err
		}
	}

	go d.runPoller(ctx)
	go d.tracker.run(ctx, d.jobClient)
	if d.docker != nil {
		go d.runContainerWatch(ctx)
	}

	srv := &http.Server{Handler: d.routes(*listen), ReadHeaderTimeout: 5 * time.Second}
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	if host, _, _ := net.SplitHostPort(*listen); !isLoopback(host) {
		log.Warn("listening beyond loopback: anyone who can reach this address can launch, stop and fault the cluster", "listen", *listen)
	}
	log.Info("relay-dash serving", "url", "http://"+lis.Addr().String())
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(lis) }()
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	d.shutdown()
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}
