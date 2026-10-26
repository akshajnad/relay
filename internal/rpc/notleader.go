package rpc

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NotLeaderReason marks an UNAVAILABLE status whose call was rejected
// before it executed because the node was not the leader. Such a call is
// always safe to retry elsewhere. An UNAVAILABLE without it (for example
// leadership lost after a proposal was sent) has an unknown outcome.
const NotLeaderReason = "NOT_LEADER"

const errorDomain = "relay.v1"

// NotLeaderError builds the NOT_LEADER status. leaderID and leaderAddr may
// be empty when no leader is known.
func NotLeaderError(leaderID, leaderAddr string) error {
	st := status.New(codes.Unavailable, "not the leader")
	st, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason:   NotLeaderReason,
		Domain:   errorDomain,
		Metadata: map[string]string{"leader_id": leaderID, "leader_addr": leaderAddr},
	})
	if err != nil {
		return status.Error(codes.Unavailable, "not the leader")
	}
	return st.Err()
}

// ParseNotLeader reports whether err is a NOT_LEADER status, and the leader
// it names (possibly empty).
func ParseNotLeader(err error) (leaderID, leaderAddr string, ok bool) {
	st, isStatus := status.FromError(err)
	if !isStatus || st.Code() != codes.Unavailable {
		return "", "", false
	}
	for _, d := range st.Details() {
		if info, isInfo := d.(*errdetails.ErrorInfo); isInfo && info.GetReason() == NotLeaderReason && info.GetDomain() == errorDomain {
			return info.GetMetadata()["leader_id"], info.GetMetadata()["leader_addr"], true
		}
	}
	return "", "", false
}
