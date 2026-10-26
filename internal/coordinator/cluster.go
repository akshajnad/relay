package coordinator

import (
	"context"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// clusterService exposes a replicated store's membership. On a store
// without one (Redis) every call is UNIMPLEMENTED.
type clusterService struct {
	relayv1.UnimplementedClusterServiceServer
	c *Coordinator
}

func (s *clusterService) membership() (store.Membership, error) {
	m, ok := s.c.store.(store.Membership)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "this coordinator's store is not replicated")
	}
	return m, nil
}

func (s *clusterService) ClusterStatus(context.Context, *relayv1.ClusterStatusRequest) (*relayv1.ClusterStatusResponse, error) {
	m, err := s.membership()
	if err != nil {
		return nil, err
	}
	st, err := m.ClusterStatus()
	if err != nil {
		return nil, storeError(err)
	}
	resp := &relayv1.ClusterStatusResponse{
		NodeId: st.NodeID, State: st.State, LeaderId: st.LeaderID, LeaderAddress: st.LeaderAddr,
		Term: st.Term, LastIndex: st.LastIndex, AppliedIndex: st.AppliedIndex, CommitIndex: st.CommitIndex,
	}
	for _, srv := range st.Servers {
		resp.Servers = append(resp.Servers, &relayv1.ClusterServer{Id: srv.ID, RaftAddress: srv.RaftAddr,
			GrpcAddress: srv.GRPCAddr, Voter: srv.Voter, Leader: srv.Leader})
	}
	return resp, nil
}

func (s *clusterService) AddVoter(ctx context.Context, req *relayv1.AddVoterRequest) (*relayv1.AddVoterResponse, error) {
	m, err := s.membership()
	if err != nil {
		return nil, err
	}
	if req.GetId() == "" || req.GetRaftAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "id and raft_address are required")
	}
	if err := m.AddVoter(ctx, req.GetId(), req.GetRaftAddress(), req.GetGrpcAddress()); err != nil {
		return nil, storeError(err)
	}
	s.c.log.Info("added voter", "id", req.GetId(), "raft", req.GetRaftAddress(), "grpc", req.GetGrpcAddress())
	return &relayv1.AddVoterResponse{}, nil
}

func (s *clusterService) RemoveServer(ctx context.Context, req *relayv1.RemoveServerRequest) (*relayv1.RemoveServerResponse, error) {
	m, err := s.membership()
	if err != nil {
		return nil, err
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}
	if err := m.RemoveServer(ctx, req.GetId()); err != nil {
		return nil, storeError(err)
	}
	s.c.log.Info("removed server", "id", req.GetId())
	return &relayv1.RemoveServerResponse{}, nil
}
