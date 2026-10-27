package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/rpc"
	"google.golang.org/grpc"
)

// coordSpec is one coordinator as the dashboard reaches it.
type coordSpec struct {
	ID      string `json:"id"`      // Raft server id; "" for a lone Redis-backed coordinator
	Service string `json:"service"` // compose service name (or the id, for an external cluster)
	GRPC    string `json:"grpc"`    // host address of its gRPC port
	Metrics string `json:"metrics"` // host address of its metrics port; "" if unknown
}

// layout is how one cluster is laid out: which compose project and file it
// comes from, and where its coordinators are published on the host.
type layout struct {
	Backend      string // raft, redis, or external
	Project      string // compose project; "" for an external cluster
	File         string // compose file, relative to --compose-dir
	Coordinators []coordSpec
	StoreService string // the Redis service, if the layout has one
}

// layouts mirror deploy/docker-compose{,.raft}.yml and their default ports.
var layouts = map[string]layout{
	"raft": {Backend: "raft", Project: "relay-raft", File: "docker-compose.raft.yml", Coordinators: []coordSpec{
		{ID: "n1", Service: "coord1", GRPC: "127.0.0.1:50051", Metrics: "127.0.0.1:9091"},
		{ID: "n2", Service: "coord2", GRPC: "127.0.0.1:50052", Metrics: "127.0.0.1:9092"},
		{ID: "n3", Service: "coord3", GRPC: "127.0.0.1:50053", Metrics: "127.0.0.1:9093"},
	}},
	"redis": {Backend: "redis", Project: "relay", File: "docker-compose.yml", StoreService: "redis",
		Coordinators: []coordSpec{{Service: "coordinator", GRPC: "127.0.0.1:50051", Metrics: "127.0.0.1:9090"}}},
}

const observabilityFile = "docker-compose.observability.yml"

// grpcSpec is the endpoint list rpc.DialCluster takes.
func (l layout) grpcSpec() string {
	parts := make([]string, len(l.Coordinators))
	for i, c := range l.Coordinators {
		parts[i] = c.GRPC
		if c.ID != "" {
			parts[i] = c.ID + "=" + c.GRPC
		}
	}
	return strings.Join(parts, ",")
}

// externalLayout describes a cluster the dashboard did not start.
func externalLayout(coordinators, metrics string) (layout, error) {
	eps, err := rpc.ParseEndpoints(coordinators)
	if err != nil {
		return layout{}, err
	}
	var ms []rpc.Endpoint
	if metrics != "" {
		if ms, err = rpc.ParseEndpoints(metrics); err != nil {
			return layout{}, err
		}
	}
	l := layout{Backend: "external"}
	for i, ep := range eps {
		c := coordSpec{ID: ep.ID, Service: ep.ID, GRPC: ep.Addr}
		if c.Service == "" {
			c.Service = fmt.Sprintf("coordinator-%d", i+1)
		}
		for j, m := range ms {
			if (m.ID != "" && m.ID == ep.ID) || (m.ID == "" && j == i) {
				c.Metrics = m.Addr
			}
		}
		l.Coordinators = append(l.Coordinators, c)
	}
	return l, nil
}

// ClusterConfig is what the launch form edits. Durations are Go duration
// strings; each setting reaches the containers through the RELAY_* variable
// the compose files read.
type ClusterConfig struct {
	Backend                string `json:"backend"`
	Workers                int    `json:"workers"`
	Concurrency            int    `json:"concurrency"`
	SleepDuration          string `json:"sleep_duration"`
	LeaseDuration          string `json:"lease_duration"`
	HeartbeatInterval      string `json:"heartbeat_interval"`
	MissedHeartbeats       int    `json:"missed_heartbeats"`
	PollTimeout            string `json:"poll_timeout"`
	BackoffBase            string `json:"backoff_base"`
	RaftHeartbeatTimeout   string `json:"raft_heartbeat_timeout"`
	RaftElectionTimeout    string `json:"raft_election_timeout"`
	RaftLeaderLeaseTimeout string `json:"raft_leader_lease_timeout"`
	// Observability adds Jaeger, Prometheus and Grafana, as `make up` does.
	Observability bool `json:"observability"`
	// Rebuild rebuilds the relay:dev image before launching.
	Rebuild bool `json:"rebuild"`
}

// defaultClusterConfig matches the compose files' own defaults.
func defaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		Backend: "raft", Workers: 3, Concurrency: 4, SleepDuration: "20ms",
		LeaseDuration: "10s", HeartbeatInterval: "1s", MissedHeartbeats: 3, PollTimeout: "5s", BackoffBase: "200ms",
		RaftHeartbeatTimeout: "1s", RaftElectionTimeout: "1s", RaftLeaderLeaseTimeout: "500ms",
	}
}

func (c ClusterConfig) dur(s string) time.Duration {
	d, _ := time.ParseDuration(s)
	return d
}

// DeadAfter is how long a silent worker lives before the failure detector
// declares it dead.
func (c ClusterConfig) DeadAfter() time.Duration {
	return time.Duration(c.MissedHeartbeats) * c.dur(c.HeartbeatInterval)
}

// Validate rejects what the coordinator or hashicorp/raft would refuse at
// startup, so a bad value fails in the form instead of in a crash loop.
func (c ClusterConfig) Validate() error {
	if _, ok := layouts[c.Backend]; !ok {
		return fmt.Errorf("backend must be raft or redis, not %q", c.Backend)
	}
	if c.Workers < 0 || c.Workers > 32 {
		return errors.New("workers must be between 0 and 32")
	}
	if c.Concurrency < 1 || c.Concurrency > 64 {
		return errors.New("slots per worker must be between 1 and 64")
	}
	if c.MissedHeartbeats < 1 || c.MissedHeartbeats > 100 {
		return errors.New("missed heartbeats must be between 1 and 100")
	}
	durs := []struct {
		name, v string
		min     time.Duration
	}{
		{"default sleep", c.SleepDuration, 0},
		{"lease duration", c.LeaseDuration, 100 * time.Millisecond},
		{"heartbeat interval", c.HeartbeatInterval, 10 * time.Millisecond},
		{"poll timeout", c.PollTimeout, 100 * time.Millisecond},
		{"retry backoff base", c.BackoffBase, time.Millisecond},
		{"raft heartbeat timeout", c.RaftHeartbeatTimeout, 5 * time.Millisecond},
		{"raft election timeout", c.RaftElectionTimeout, 5 * time.Millisecond},
		{"raft leader lease timeout", c.RaftLeaderLeaseTimeout, 5 * time.Millisecond},
	}
	for _, d := range durs {
		v, err := time.ParseDuration(d.v)
		if err != nil {
			return fmt.Errorf("%s: %q is not a duration (try 500ms, 2s)", d.name, d.v)
		}
		if v < d.min {
			return fmt.Errorf("%s must be at least %v", d.name, d.min)
		}
	}
	if lease, window := c.dur(c.LeaseDuration), c.DeadAfter(); lease <= window {
		return fmt.Errorf("lease duration (%v) must exceed the failure-detection window, missed heartbeats × interval (%v)", lease, window)
	}
	if c.Backend == "raft" {
		hb := c.dur(c.RaftHeartbeatTimeout)
		if c.dur(c.RaftLeaderLeaseTimeout) > hb {
			return errors.New("raft leader lease timeout cannot exceed the raft heartbeat timeout")
		}
		if c.dur(c.RaftElectionTimeout) < hb {
			return errors.New("raft election timeout must be at least the raft heartbeat timeout")
		}
	}
	return nil
}

// env is the compose environment that applies the configuration.
func (c ClusterConfig) env() []string {
	otlp := ""
	if c.Observability {
		otlp = "jaeger:4317"
	}
	return []string{
		"RELAY_WORKER_CONCURRENCY=" + strconv.Itoa(c.Concurrency),
		"RELAY_SLEEP_DURATION=" + c.SleepDuration,
		"RELAY_LEASE_DURATION=" + c.LeaseDuration,
		"RELAY_HEARTBEAT_INTERVAL=" + c.HeartbeatInterval,
		"RELAY_MISSED_HEARTBEATS=" + strconv.Itoa(c.MissedHeartbeats),
		"RELAY_POLL_TIMEOUT=" + c.PollTimeout,
		"RELAY_BACKOFF_BASE=" + c.BackoffBase,
		"RELAY_RAFT_HEARTBEAT_TIMEOUT=" + c.RaftHeartbeatTimeout,
		"RELAY_RAFT_ELECTION_TIMEOUT=" + c.RaftElectionTimeout,
		"RELAY_RAFT_LEADER_LEASE_TIMEOUT=" + c.RaftLeaderLeaseTimeout,
		"RELAY_OTLP_ENDPOINT=" + otlp,
	}
}

// configFromEnv recovers the configuration of a cluster the dashboard did
// not launch from its containers' environments. Inside a container the
// variables carry flag names, so the worker's concurrency arrives as
// RELAY_CONCURRENCY rather than compose's RELAY_WORKER_CONCURRENCY.
func configFromEnv(backend string, coord, worker map[string]string, workers int) ClusterConfig {
	c := defaultClusterConfig()
	c.Backend, c.Workers = backend, workers
	str := func(dst *string, env map[string]string, key string) {
		if v, ok := env[key]; ok && v != "" {
			*dst = v
		}
	}
	num := func(dst *int, env map[string]string, key string) {
		if n, err := strconv.Atoi(env[key]); err == nil {
			*dst = n
		}
	}
	str(&c.LeaseDuration, coord, "RELAY_LEASE_DURATION")
	str(&c.HeartbeatInterval, coord, "RELAY_HEARTBEAT_INTERVAL")
	num(&c.MissedHeartbeats, coord, "RELAY_MISSED_HEARTBEATS")
	str(&c.PollTimeout, coord, "RELAY_POLL_TIMEOUT")
	str(&c.BackoffBase, coord, "RELAY_BACKOFF_BASE")
	str(&c.RaftHeartbeatTimeout, coord, "RELAY_RAFT_HEARTBEAT_TIMEOUT")
	str(&c.RaftElectionTimeout, coord, "RELAY_RAFT_ELECTION_TIMEOUT")
	str(&c.RaftLeaderLeaseTimeout, coord, "RELAY_RAFT_LEADER_LEASE_TIMEOUT")
	num(&c.Concurrency, worker, "RELAY_CONCURRENCY")
	str(&c.SleepDuration, worker, "RELAY_SLEEP_DURATION")
	c.Observability = coord["RELAY_OTLP_ENDPOINT"] != ""
	return c
}

// target is the dashboard's set of connections to one cluster.
type target struct {
	l       layout
	conn    *rpc.ClusterConn   // follows the leader; for job and worker calls
	nodes   []*grpc.ClientConn // one per coordinator, for per-node ClusterStatus
	jobs    relayv1.JobServiceClient
	workers relayv1.WorkerServiceClient
}

func dialTarget(l layout) (*target, error) {
	conn, err := rpc.DialCluster(l.grpcSpec())
	if err != nil {
		return nil, err
	}
	t := &target{l: l, conn: conn, jobs: relayv1.NewJobServiceClient(conn), workers: relayv1.NewWorkerServiceClient(conn)}
	for _, c := range l.Coordinators {
		nc, err := rpc.Dial(c.GRPC)
		if err != nil {
			t.close()
			return nil, err
		}
		t.nodes = append(t.nodes, nc)
	}
	return t, nil
}

func (t *target) close() {
	if t.conn != nil {
		_ = t.conn.Close()
	}
	for _, c := range t.nodes {
		_ = c.Close()
	}
}
