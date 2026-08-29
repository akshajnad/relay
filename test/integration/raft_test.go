//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/rpc"
)

var raftComposeFile = func() string {
	_, here, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(here), "..", "..", "deploy", "docker-compose.raft.yml")
}()

// RaftCluster is a three-node Raft compose project, torn down when the test
// ends.
type RaftCluster struct {
	t       *testing.T
	project string
	env     []string
	// Endpoints maps node id to its published gRPC address.
	Endpoints map[string]string
	conn      *rpc.ClusterConn
	Jobs      relayv1.JobServiceClient
	Workers   relayv1.WorkerServiceClient
}

func NewRaftCluster(t *testing.T, workers int) *RaftCluster {
	t.Helper()
	c := &RaftCluster{t: t, project: "relayit-" + strings.Trim(nonAlnum.ReplaceAllString(strings.ToLower(t.Name()), "-"), "-"),
		Endpoints: map[string]string{}}
	c.env = os.Environ()
	var spec []string
	for i := 1; i <= 4; i++ {
		port := freePort(t)
		id := fmt.Sprintf("n%d", i)
		c.Endpoints[id] = fmt.Sprintf("127.0.0.1:%d", port)
		c.env = append(c.env, fmt.Sprintf("RELAY_N%d_GRPC_PUBLISH=127.0.0.1:%d:50051", i, port),
			fmt.Sprintf("RELAY_N%d_METRICS_PUBLISH=127.0.0.1:%d:9090", i, freePort(t)))
		if i <= 3 {
			spec = append(spec, id+"="+c.Endpoints[id])
		}
	}
	c.compose("down", "-v", "--remove-orphans")
	t.Cleanup(func() {
		if t.Failed() {
			out, _ := c.compose("logs", "--no-color", "--tail", "200")
			t.Logf("compose logs:\n%s", out)
		}
		if c.conn != nil {
			c.conn.Close()
		}
		c.compose("--profile", "extra", "down", "-v", "--remove-orphans", "--timeout", "5")
	})
	c.mustCompose("up", "-d", "--wait", "--no-build", "--scale", fmt.Sprintf("worker=%d", workers))
	conn, err := rpc.DialCluster(strings.Join(spec, ","))
	if err != nil {
		t.Fatal(err)
	}
	c.conn = conn
	c.Jobs = relayv1.NewJobServiceClient(conn)
	c.Workers = relayv1.NewWorkerServiceClient(conn)
	Eventually(t, 60*time.Second, fmt.Sprintf("%d live workers", workers), func() bool {
		resp, err := c.Workers.ListWorkers(ctx(t), &relayv1.ListWorkersRequest{})
		alive := 0
		if err == nil {
			for _, w := range resp.Workers {
				if w.State == "ALIVE" {
					alive++
				}
			}
		}
		return alive == workers
	})
	return c
}

func (c *RaftCluster) compose(args ...string) (string, error) {
	full := append([]string{"compose", "-p", c.project, "-f", raftComposeFile}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Env = c.env
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return out.String(), err
}

func (c *RaftCluster) mustCompose(args ...string) string {
	c.t.Helper()
	out, err := c.compose(args...)
	if err != nil {
		c.t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// status asks one node for its view of the cluster.
func (c *RaftCluster) status(node string) (*relayv1.ClusterStatusResponse, error) {
	conn, err := rpc.Dial(c.Endpoints[node])
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return relayv1.NewClusterServiceClient(conn).ClusterStatus(ctx(c.t), &relayv1.ClusterStatusRequest{})
}

// leader returns the node id that reports being leader, waiting for one.
func (c *RaftCluster) leader(exclude string) string {
	c.t.Helper()
	var leader string
	Eventually(c.t, 30*time.Second, "a leader", func() bool {
		for _, n := range []string{"n1", "n2", "n3"} {
			if n == exclude {
				continue
			}
			if st, err := c.status(n); err == nil && st.State == "Leader" {
				leader = n
				return true
			}
		}
		return false
	})
	return leader
}

func (c *RaftCluster) container(node string) string {
	c.t.Helper()
	svc := "coord" + strings.TrimPrefix(node, "n")
	return strings.TrimSpace(c.mustCompose("ps", "-a", "-q", svc))
}

func (c *RaftCluster) relayctl(args ...string) string {
	c.t.Helper()
	var spec []string
	for _, n := range []string{"n1", "n2", "n3"} {
		spec = append(spec, n+"="+c.Endpoints[n])
	}
	full := append([]string{"run", "./cmd/relayctl", "--coordinator", strings.Join(spec, ",")}, args...)
	cmd := exec.Command("go", full...)
	cmd.Dir = filepath.Join(filepath.Dir(raftComposeFile), "..")
	out, err := cmd.CombinedOutput()
	if err != nil {
		c.t.Fatalf("relayctl %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func (c *RaftCluster) submitRetrying(req *relayv1.SubmitJobRequest) string {
	c.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := c.Jobs.SubmitJob(cctx, req)
		cancel()
		if err == nil {
			return resp.JobId
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("submit kept failing: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *RaftCluster) getRetrying(id string) *relayv1.GetJobResponse {
	c.t.Helper()
	var resp *relayv1.GetJobResponse
	Eventually(c.t, 30*time.Second, "GetJob "+id, func() bool {
		var err error
		resp, err = c.Jobs.GetJob(ctx(c.t), &relayv1.GetJobRequest{JobId: id})
		return err == nil
	})
	return resp
}

// docker kill the Raft leader in the middle of a workload: a new leader is
// elected, clients and workers follow it, every acknowledged job finishes
// exactly once, and the old leader rejoins as a follower and catches up.
func TestRaftLeaderKilled(t *testing.T) {
	c := NewRaftCluster(t, 3)
	var ids []string
	for i := 0; i < 60; i++ {
		ids = append(ids, c.submitRetrying(&relayv1.SubmitJobRequest{Type: "sleep", Payload: sleepPayload(300),
			IdempotencyKey: fmt.Sprintf("before-%d", i), MaxAttempts: 5}))
	}
	old := c.leader("")
	docker(t, "kill", c.container(old))
	killed := time.Now()
	newLeader := c.leader(old)
	t.Logf("leader %s killed; %s elected after %v", old, newLeader, time.Since(killed).Round(time.Millisecond))

	for i := 0; i < 60; i++ {
		ids = append(ids, c.submitRetrying(&relayv1.SubmitJobRequest{Type: "sleep", Payload: sleepPayload(50),
			IdempotencyKey: fmt.Sprintf("after-%d", i), MaxAttempts: 5}))
	}
	for _, id := range ids {
		Eventually(t, 90*time.Second, "job "+id+" to succeed", func() bool {
			return c.getRetrying(id).Job.Status == succeeded
		})
		got := c.getRetrying(id)
		terminal := 0
		for _, e := range got.History {
			if e.Kind == "SUCCEEDED" || e.Kind == "FAILED" || e.Kind == "DEAD" {
				terminal++
			}
		}
		if terminal != 1 {
			t.Fatalf("job %s has %d terminal transitions", id, terminal)
		}
	}

	// The old leader comes back as a follower and catches up.
	docker(t, "start", c.container(old))
	Eventually(t, 60*time.Second, old+" to rejoin and catch up", func() bool {
		st, err := c.status(old)
		if err != nil || st.State != "Follower" {
			return false
		}
		lead, err := c.status(c.leader(""))
		return err == nil && st.AppliedIndex >= lead.CommitIndex-5
	})
}

// Membership is managed through relayctl: a fourth node joins and catches
// up, then is removed.
func TestRaftMembershipViaRelayctl(t *testing.T) {
	c := NewRaftCluster(t, 1)
	for i := 0; i < 20; i++ {
		c.submitRetrying(&relayv1.SubmitJobRequest{Type: "noop", IdempotencyKey: fmt.Sprintf("m-%d", i)})
	}
	c.mustCompose("--profile", "extra", "up", "-d", "--wait", "--no-build", "--no-recreate", "coord4")
	out := c.relayctl("cluster", "add", "--id", "n4", "--raft-addr", "coord4:7000", "--grpc-addr", "coord4:50051")
	if !strings.Contains(out, "added n4") {
		t.Fatalf("add output: %s", out)
	}
	Eventually(t, 30*time.Second, "n4 to catch up", func() bool {
		st, err := c.status("n4")
		return err == nil && st.State == "Follower" && len(st.Servers) == 4 && st.AppliedIndex > 20
	})
	status := c.relayctl("cluster", "status")
	if !strings.Contains(status, "n4") || !strings.Contains(status, "coord4:50051") {
		t.Fatalf("status does not list n4:\n%s", status)
	}
	c.relayctl("cluster", "remove", "--id", "n4")
	Eventually(t, 30*time.Second, "n4 to be removed", func() bool {
		st, err := c.status(c.leader(""))
		return err == nil && len(st.Servers) == 3
	})
}
