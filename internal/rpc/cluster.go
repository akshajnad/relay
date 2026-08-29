package rpc

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Endpoint is one coordinator. ID is the node's Raft server id, used to
// resolve NOT_LEADER hints; it may be empty for a single coordinator.
type Endpoint struct {
	ID   string
	Addr string
}

// ParseEndpoints parses "addr" or "id=addr,id=addr,...".
func ParseEndpoints(spec string) ([]Endpoint, error) {
	var eps []Endpoint
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		ep := Endpoint{Addr: part}
		if id, addr, ok := strings.Cut(part, "="); ok {
			ep = Endpoint{ID: id, Addr: addr}
		}
		eps = append(eps, ep)
	}
	if len(eps) == 0 {
		return nil, fmt.Errorf("no coordinator endpoints in %q", spec)
	}
	return eps, nil
}

// ClusterConn is a grpc.ClientConnInterface over a set of coordinators. It
// sends each call to the node it believes is leader. A NOT_LEADER reply
// means the call did not execute, so it follows the hint (or tries the
// next node) and retries, with backoff, until the call's deadline. Any
// other unavailability is returned to the caller, because the call may
// have executed, but the next call goes to another node.
type ClusterConn struct {
	eps   []Endpoint
	conns []*grpc.ClientConn

	mu  sync.Mutex
	cur int
}

var _ grpc.ClientConnInterface = (*ClusterConn)(nil)

// DialCluster connects to every endpoint in spec (see ParseEndpoints).
func DialCluster(spec string, extra ...grpc.DialOption) (*ClusterConn, error) {
	eps, err := ParseEndpoints(spec)
	if err != nil {
		return nil, err
	}
	c := &ClusterConn{eps: eps}
	for _, ep := range eps {
		conn, err := Dial(ep.Addr, extra...)
		if err != nil {
			c.Close()
			return nil, err
		}
		c.conns = append(c.conns, conn)
	}
	return c, nil
}

func (c *ClusterConn) Close() error {
	var errs []error
	for _, conn := range c.conns {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}

func (c *ClusterConn) current() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur
}

// moveTo switches to the hinted leader, or to the next endpoint after
// from when the hint does not resolve. It returns the new index.
func (c *ClusterConn) moveTo(from int, leaderID, leaderAddr string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cur != from {
		return c.cur // someone else already moved on
	}
	next := -1
	for i, ep := range c.eps {
		if (leaderID != "" && ep.ID == leaderID) || (leaderAddr != "" && ep.Addr == leaderAddr) {
			next = i
			break
		}
	}
	if next < 0 || next == from {
		next = (from + 1) % len(c.eps)
	}
	c.cur = next
	return next
}

func (c *ClusterConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	if len(c.conns) == 1 {
		return c.conns[0].Invoke(ctx, method, args, reply, opts...)
	}
	// With several nodes, fail fast on a dead connection instead of
	// waiting for it, so the caller can move on to a live node.
	opts = append(opts, grpc.WaitForReady(false))
	backoff := 20 * time.Millisecond
	for {
		i := c.current()
		err := c.conns[i].Invoke(ctx, method, args, reply, opts...)
		if err == nil {
			return nil
		}
		if id, addr, ok := ParseNotLeader(err); ok {
			// Not executed: safe to retry elsewhere.
			c.moveTo(i, id, addr)
			if !sleep(ctx, backoff) {
				return err
			}
			backoff = min(2*backoff, 500*time.Millisecond)
			continue
		}
		switch status.Code(err) {
		case codes.Unavailable, codes.DeadlineExceeded:
			// Possibly executed: hand the error back, but steer the next
			// call away from a node that may be down or partitioned.
			c.moveTo(i, "", "")
		}
		return err
	}
}

func (c *ClusterConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return c.conns[c.current()].NewStream(ctx, desc, method, opts...)
}

func sleep(ctx context.Context, d time.Duration) bool {
	d = d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
