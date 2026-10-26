//go:build relay_nofence

package redisstore

import (
	"context"
	"testing"

	"github.com/akshajnad/relay/internal/store"
)

// TestNoFenceAcceptsStaleCompletion confirms the relay_nofence build really
// disables fencing, so the chaos checker's deliberate-bug test exercises a
// real bug.
func TestNoFenceAcceptsStaleCompletion(t *testing.T) {
	s, _ := newStore(t, nil)
	ctx := context.Background()
	a, b := mustRegister(t, s, "host-a"), mustRegister(t, s, "host-b")
	id := mustSubmit(t, s, store.SubmitParams{})
	j := mustAcquire(t, s, a)
	if _, err := s.Complete(ctx, id, b, j.LeaseToken+7, []byte("zombie")); err != nil {
		t.Fatalf("nofence build rejected a stale completion: %v", err)
	}
	got, _ := mustGet(t, s, id)
	if got.Status != store.StatusSucceeded || string(got.Result) != "zombie" {
		t.Fatalf("job = %+v", got)
	}
}
