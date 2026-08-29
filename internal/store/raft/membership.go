package raftstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/akshajnad/relay/internal/store"
	"github.com/hashicorp/raft"
)

var _ store.Membership = (*Store)(nil)

// ClusterStatus reports this node's view; any node answers.
func (s *Store) ClusterStatus() (store.ClusterStatus, error) {
	f := s.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return store.ClusterStatus{}, err
	}
	stats := s.raft.Stats()
	num := func(k string) uint64 { n, _ := strconv.ParseUint(stats[k], 10, 64); return n }
	_, leaderID := s.raft.LeaderWithID()
	st := store.ClusterStatus{
		NodeID: s.cfg.NodeID, State: s.raft.State().String(), LeaderID: string(leaderID),
		Term: num("term"), LastIndex: s.raft.LastIndex(), AppliedIndex: s.raft.AppliedIndex(), CommitIndex: num("commit_index"),
	}
	if nl, ok := s.notLeader().(*store.NotLeaderError); ok {
		st.LeaderAddr = nl.LeaderAddr
	}
	s.fsm.mu.RLock()
	defer s.fsm.mu.RUnlock()
	for _, srv := range f.Configuration().Servers {
		id := string(srv.ID)
		addr := s.fsm.nodeAddrs[id]
		if addr == "" {
			for _, p := range s.cfg.Peers {
				if p.ID == id {
					addr = p.GRPCAddr
				}
			}
		}
		st.Servers = append(st.Servers, store.ClusterServer{ID: id, RaftAddr: string(srv.Address), GRPCAddr: addr,
			Voter: srv.Suffrage == raft.Voter, Leader: srv.ID == leaderID})
	}
	return st, nil
}

// AddVoter adds a node to the cluster and records its client address. The
// new node must already be running (without --bootstrap); it catches up
// from the leader, by snapshot if the log has been compacted.
func (s *Store) AddVoter(_ context.Context, id, raftAddr, grpcAddr string) error {
	if !s.IsLeader() {
		return s.notLeader()
	}
	if err := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(raftAddr), 0, 30*time.Second).Error(); err != nil {
		return s.membershipErr("add voter", err)
	}
	if grpcAddr != "" {
		if _, err := s.propose(&command{Op: opSetNodeAddr, NowMs: time.Now().UnixMilli(), NodeID: id, NodeAddr: grpcAddr}); err != nil {
			return err
		}
	}
	return nil
}

// RemoveServer removes a node. Removing the leader itself makes it step
// down once the change commits.
func (s *Store) RemoveServer(_ context.Context, id string) error {
	if !s.IsLeader() {
		return s.notLeader()
	}
	if err := s.raft.RemoveServer(raft.ServerID(id), 0, 30*time.Second).Error(); err != nil {
		return s.membershipErr("remove server", err)
	}
	return nil
}

func (s *Store) membershipErr(what string, err error) error {
	if errors.Is(err, raft.ErrNotLeader) {
		return s.notLeader()
	}
	return fmt.Errorf("%s: %w", what, err)
}
