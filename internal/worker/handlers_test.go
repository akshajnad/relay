package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestHandlers(t *testing.T) {
	r := DefaultHandlers(time.Millisecond)
	ctx := context.Background()
	job := func(typ, payload string, attempt int) Job {
		return Job{ID: "j", Type: typ, Payload: []byte(payload), Attempt: attempt, MaxAttempts: 5, WorkerID: "w-1"}
	}

	out, err := r.run(ctx, job("noop", "", 1))
	if err != nil {
		t.Fatal(err)
	}
	var res map[string]any
	if err := json.Unmarshal(out, &res); err != nil || res["worker_id"] != "w-1" {
		t.Fatalf("noop result = %s, %v", out, err)
	}

	start := time.Now()
	if _, err := r.run(ctx, job("sleep", `{"duration_ms":30}`, 1)); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("sleep returned early")
	}
	if _, err := r.run(ctx, job("sleep", "", 1)); err != nil {
		t.Fatalf("sleep with default duration: %v", err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := r.run(cctx, job("sleep", `{"duration_ms":10000}`, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled sleep = %v", err)
	}

	for attempt := 1; attempt <= 2; attempt++ {
		_, err := r.run(ctx, job("fail-n-times", `{"n":2}`, attempt))
		if err == nil || IsPermanent(err) {
			t.Fatalf("fail-n-times attempt %d = %v, want retryable error", attempt, err)
		}
	}
	if _, err := r.run(ctx, job("fail-n-times", `{"n":2}`, 3)); err != nil {
		t.Fatalf("fail-n-times attempt 3 = %v", err)
	}

	if _, err := r.run(ctx, job("sleep", `{not json`, 1)); !IsPermanent(err) {
		t.Fatalf("bad payload = %v, want permanent", err)
	}
	if _, err := r.run(ctx, job("nope", "", 1)); !IsPermanent(err) || !errors.Is(err, errNoHandler) {
		t.Fatalf("unknown type = %v, want permanent no-handler", err)
	}

	r["boom"] = func(context.Context, Job) ([]byte, error) { panic("kaboom") }
	if _, err := r.run(ctx, job("boom", "", 1)); err == nil || IsPermanent(err) {
		t.Fatalf("panic = %v, want retryable error", err)
	}
}
