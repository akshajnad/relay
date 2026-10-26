package rpc

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNotLeaderRoundTrip(t *testing.T) {
	err := NotLeaderError("n2", "10.0.0.2:50051")
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v", status.Code(err))
	}
	id, addr, ok := ParseNotLeader(err)
	if !ok || id != "n2" || addr != "10.0.0.2:50051" {
		t.Fatalf("parsed (%q, %q, %v)", id, addr, ok)
	}
	if _, _, ok := ParseNotLeader(NotLeaderError("", "")); !ok {
		t.Fatal("leaderless NOT_LEADER not recognized")
	}
	// A plain UNAVAILABLE has an unknown outcome and must not parse.
	for _, e := range []error{status.Error(codes.Unavailable, "leadership lost"), errors.New("x"), nil} {
		if _, _, ok := ParseNotLeader(e); ok {
			t.Fatalf("%v parsed as NOT_LEADER", e)
		}
	}
}
