package rpc

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeNode answers GetJob as leader, or with NOT_LEADER naming the leader.
type fakeNode struct {
	relayv1.UnimplementedJobServiceServer
	id     string
	leader *atomic.Value // id of the current leader
	calls  atomic.Int64
	// unavailable makes the node fail every call with a plain UNAVAILABLE.
	unavailable atomic.Bool
}

func (n *fakeNode) GetJob(context.Context, *relayv1.GetJobRequest) (*relayv1.GetJobResponse, error) {
	n.calls.Add(1)
	if n.unavailable.Load() {
		return nil, status.Error(codes.Unavailable, "store: raft apply: leadership lost")
	}
	if l := n.leader.Load().(string); l != n.id {
		return nil, NotLeaderError(l, "ignored-addr:1")
	}
	return &relayv1.GetJobResponse{Job: &relayv1.Job{Id: n.id}}, nil
}

func startNodes(t *testing.T, ids ...string) ([]*fakeNode, string, *atomic.Value) {
	t.Helper()
	leader := &atomic.Value{}
	leader.Store(ids[0])
	var nodes []*fakeNode
	var spec []string
	for _, id := range ids {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		n := &fakeNode{id: id, leader: leader}
		srv := grpc.NewServer()
		relayv1.RegisterJobServiceServer(srv, n)
		go srv.Serve(lis)
		t.Cleanup(srv.Stop)
		nodes = append(nodes, n)
		spec = append(spec, id+"="+lis.Addr().String())
	}
	return nodes, strings.Join(spec, ","), leader
}

func getLeader(t *testing.T, conn *ClusterConn) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := relayv1.NewJobServiceClient(conn).GetJob(ctx, &relayv1.GetJobRequest{})
	if err != nil {
		return "", err
	}
	return resp.Job.Id, nil
}

func TestClusterConnFollowsLeaderHints(t *testing.T) {
	nodes, spec, leader := startNodes(t, "n1", "n2", "n3")
	leader.Store("n3")
	conn, err := DialCluster(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// First call lands on n1, which redirects straight to n3 by id.
	if got, err := getLeader(t, conn); err != nil || got != "n3" {
		t.Fatalf("got %q, %v", got, err)
	}
	if nodes[1].calls.Load() != 0 {
		t.Fatal("redirect did not use the leader hint")
	}
	// Leadership moves: the next call is redirected again.
	leader.Store("n2")
	if got, err := getLeader(t, conn); err != nil || got != "n2" {
		t.Fatalf("after failover got %q, %v", got, err)
	}
	// No leader known yet: the client walks the nodes until one is.
	leader.Store("")
	go func() { time.Sleep(100 * time.Millisecond); leader.Store("n1") }()
	if got, err := getLeader(t, conn); err != nil || got != "n1" {
		t.Fatalf("leaderless period got %q, %v", got, err)
	}
}

func TestClusterConnDoesNotRetryUnknownOutcomes(t *testing.T) {
	nodes, spec, _ := startNodes(t, "n1", "n2")
	conn, err := DialCluster(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	nodes[0].unavailable.Store(true)
	// The call may have executed on n1, so it is returned, not replayed...
	if _, err := getLeader(t, conn); status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, want UNAVAILABLE", err)
	}
	if nodes[0].calls.Load() != 1 || nodes[1].calls.Load() != 0 {
		t.Fatalf("calls = %d, %d: an unknown outcome was retried", nodes[0].calls.Load(), nodes[1].calls.Load())
	}
	// ...but the next call goes elsewhere (n2, which redirects back to n1
	// as leader; n1 is still unavailable).
	if _, err := getLeader(t, conn); status.Code(err) != codes.Unavailable {
		t.Fatalf("second call err = %v", err)
	}
	if nodes[1].calls.Load() != 1 {
		t.Fatal("client did not move off the unavailable node")
	}
}

func TestParseEndpoints(t *testing.T) {
	eps, err := ParseEndpoints("n1=a:1, n2=b:2,c:3")
	if err != nil || len(eps) != 3 || eps[0] != (Endpoint{"n1", "a:1"}) || eps[2] != (Endpoint{"", "c:3"}) {
		t.Fatalf("eps = %+v, %v", eps, err)
	}
	if _, err := ParseEndpoints(" , "); err == nil {
		t.Fatal("empty spec accepted")
	}
}
