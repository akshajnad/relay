package check

import (
	"strings"
	"testing"

	"github.com/akshajnad/relay/internal/history"
	"github.com/akshajnad/relay/internal/store"
)

// builder assembles a synthetic history. Each op takes 10 time units and
// ops are laid out in call order unless given explicit times.
type builder struct {
	h   history.History
	now int64
	seq uint64
}

func (b *builder) op(proc string, in history.Record, typ string, out history.Record) {
	b.now += 10
	b.at(proc, b.now, b.now+5, in, typ, out)
}

func (b *builder) at(proc string, call, ret int64, in history.Record, typ string, out history.Record) {
	b.seq++
	in.Process, in.Op, in.Type, in.TimeNs = proc, b.seq, history.Invoke, call
	op := history.Op{Invoke: in}
	if typ != history.Info {
		out.Process, out.Op, out.Type, out.TimeNs, out.Method = proc, b.seq, typ, ret, in.Method
		op.Done = &out
	}
	b.h.Ops = append(b.h.Ops, op)
}

func (b *builder) submit(proc, key, id string, dup bool) {
	b.op(proc, history.Record{Method: history.Submit, Key: key, MaxAttempts: 3}, history.OK,
		history.Record{JobID: id, Duplicate: dup})
}

func (b *builder) acquire(w, id string, tok int64, attempt int) {
	b.op(w, history.Record{Method: history.Acquire, Worker: w}, history.OK,
		history.Record{HasJob: true, JobID: id, Token: tok, Attempts: attempt, MaxAttempts: 3,
			Owner: w, Status: store.StatusLeased})
}

func (b *builder) complete(w, id string, tok int64, code string) {
	b.op(w, history.Record{Method: history.Complete, Worker: w, JobID: id, Token: tok}, history.OK,
		history.Record{Code: code})
}

func ev(kind, w string, tok int64) store.Event { return store.Event{Kind: kind, Worker: w, Token: tok} }

func job(id, status string, attempts int, owner string, tok int64, events ...store.Event) FinalJob {
	return FinalJob{
		Job: store.Job{ID: id, Status: status, Attempts: attempts, MaxAttempts: 3,
			LeaseOwner: owner, LeaseToken: tok},
		Events: events,
	}
}

func mustPass(t *testing.T, rep *Report) {
	t.Helper()
	if !rep.OK() {
		t.Fatalf("expected PASS:\n%s", rep)
	}
}

func mustFlag(t *testing.T, rep *Report, invariants ...string) {
	t.Helper()
	if rep.OK() {
		t.Fatalf("expected FAIL, got:\n%s", rep)
	}
	for _, name := range invariants {
		if rep.Result(name).OK() {
			t.Fatalf("expected %s to be violated:\n%s", name, rep)
		}
	}
}

func TestCleanHistoryPasses(t *testing.T) {
	var b builder
	b.submit("c1", "k1", "j1", false)
	b.submit("c2", "k1", "j1", true) // duplicate key, same job
	b.acquire("w1", "j1", 1, 1)
	b.complete("w1", "j1", 1, "")
	b.complete("w1", "j1", 1, "") // retried report
	b.h.Ops[len(b.h.Ops)-1].Done.Already = true
	b.op("c1", history.Record{Method: history.Get, JobID: "j1"}, history.OK,
		history.Record{JobID: "j1", Status: store.StatusSucceeded, Token: 1, Attempts: 1, Owner: "w1", Key: "k1"})

	final := Final{"j1": job("j1", store.StatusSucceeded, 1, "w1", 1,
		ev("SUBMITTED", "", 0), ev("LEASED", "w1", 1), ev("SUCCEEDED", "w1", 1))}
	final["j1"] = withKey(final["j1"], "k1")
	mustPass(t, Check(&b.h, final, Options{}))
}

func TestFencedZombiePasses(t *testing.T) {
	// w1's lease is revoked (unobserved), w2 gets the job under a higher
	// token and completes; w1's late completion is rejected.
	var b builder
	b.submit("c1", "", "j1", false)
	b.acquire("w1", "j1", 1, 1)
	b.acquire("w2", "j1", 2, 2)
	b.complete("w2", "j1", 2, "")
	b.complete("w1", "j1", 1, "Aborted")
	final := Final{"j1": job("j1", store.StatusSucceeded, 2, "w2", 2,
		ev("SUBMITTED", "", 0), ev("LEASED", "w1", 1), ev("REQUEUED", "w1", 1),
		ev("LEASED", "w2", 2), ev("SUCCEEDED", "w2", 2))}
	mustPass(t, Check(&b.h, final, Options{}))
}

func TestHiddenLeaseAndIndeterminateOpsPass(t *testing.T) {
	// An acquire whose reply was lost leased the job to w1 (attempt 1,
	// token 5), it expired, and w2 got attempt 2 under token 9. w2's
	// completion timed out but applied.
	var b builder
	b.submit("c1", "", "j1", false)
	b.op("w1", history.Record{Method: history.Acquire, Worker: "w1"}, history.Info, history.Record{})
	b.acquire("w2", "j1", 9, 2)
	b.op("w2", history.Record{Method: history.Complete, Worker: "w2", JobID: "j1", Token: 9}, history.Info, history.Record{})
	b.op("c1", history.Record{Method: history.Get, JobID: "j1"}, history.OK,
		history.Record{JobID: "j1", Status: store.StatusSucceeded, Token: 9, Attempts: 2, Owner: "w2"})
	final := Final{"j1": job("j1", store.StatusSucceeded, 2, "w2", 9,
		ev("SUBMITTED", "", 0), ev("LEASED", "w1", 5), ev("REQUEUED", "w1", 5),
		ev("LEASED", "w2", 9), ev("SUCCEEDED", "w2", 9))}
	mustPass(t, Check(&b.h, final, Options{}))
}

func TestRetryThenDeadPasses(t *testing.T) {
	var b builder
	b.submit("c1", "", "j1", false)
	for attempt := 1; attempt <= 3; attempt++ {
		tok := int64(attempt * 10)
		b.acquire("w1", "j1", tok, attempt)
		status := store.StatusPending
		if attempt == 3 {
			status = store.StatusDead
		}
		b.op("w1", history.Record{Method: history.FailJob, Worker: "w1", JobID: "j1", Token: tok, Retryable: true},
			history.OK, history.Record{Status: status})
	}
	final := Final{"j1": job("j1", store.StatusDead, 3, "w1", 30,
		ev("SUBMITTED", "", 0), ev("LEASED", "w1", 10), ev("RETRY", "w1", 10), ev("LEASED", "w1", 20),
		ev("RETRY", "w1", 20), ev("LEASED", "w1", 30), ev("DEAD", "w1", 30))}
	mustPass(t, Check(&b.h, final, Options{}))
}

// TestZombieAcceptanceFlagged is the fencing bug: a completion carrying a
// superseded token is accepted.
func TestZombieAcceptanceFlagged(t *testing.T) {
	var b builder
	b.submit("c1", "", "j1", false)
	b.acquire("w1", "j1", 1, 1)
	b.acquire("w2", "j1", 2, 2)
	b.complete("w1", "j1", 1, "") // accepted with a stale token
	b.complete("w2", "j1", 2, "Aborted")
	final := Final{"j1": job("j1", store.StatusSucceeded, 2, "w2", 2,
		ev("SUBMITTED", "", 0), ev("LEASED", "w1", 1), ev("REQUEUED", "w1", 1),
		ev("LEASED", "w2", 2), ev("SUCCEEDED", "w1", 1))}
	rep := Check(&b.h, final, Options{})
	mustFlag(t, rep, Fencing, Linearizable)
	if len(rep.NonLinearizable) != 1 || rep.NonLinearizable[0] != "j1" {
		t.Fatalf("non-linearizable = %v", rep.NonLinearizable)
	}
	if paths, err := rep.Visualize(t.TempDir(), 5); err != nil || len(paths) != 1 {
		t.Fatalf("visualize = %v, %v", paths, err)
	}
}

func TestLateReportAfterRevocationFlagged(t *testing.T) {
	// The lease was revoked and not re-granted, yet the old holder's
	// completion landed.
	var b builder
	b.submit("c1", "", "j1", false)
	b.acquire("w1", "j1", 1, 1)
	b.complete("w1", "j1", 1, "")
	final := Final{"j1": job("j1", store.StatusSucceeded, 1, "w1", 1,
		ev("SUBMITTED", "", 0), ev("LEASED", "w1", 1), ev("REQUEUED", "w1", 1), ev("SUCCEEDED", "w1", 1))}
	mustFlag(t, Check(&b.h, final, Options{}), Fencing)
}

func TestDoubleTerminalFlagged(t *testing.T) {
	var b builder
	b.submit("c1", "", "j1", false)
	b.acquire("w1", "j1", 1, 1)
	b.acquire("w2", "j1", 2, 2)
	b.complete("w2", "j1", 2, "")
	b.complete("w1", "j1", 1, "")
	final := Final{"j1": job("j1", store.StatusSucceeded, 2, "w1", 2,
		ev("SUBMITTED", "", 0), ev("LEASED", "w1", 1), ev("REQUEUED", "w1", 1),
		ev("LEASED", "w2", 2), ev("SUCCEEDED", "w2", 2), ev("SUCCEEDED", "w1", 1))}
	mustFlag(t, Check(&b.h, final, Options{}), TerminalOnce, Fencing, Linearizable)
}

func TestLostAckFlagged(t *testing.T) {
	var b builder
	b.submit("c1", "", "j1", false)
	final := Final{"j1": {ReadErr: "NotFound: job not found"}}
	mustFlag(t, Check(&b.h, final, Options{}), NoLostAcks)
	mustFlag(t, Check(&b.h, Final{}, Options{}), NoLostAcks)
}

func TestStuckJobFlagged(t *testing.T) {
	var b builder
	b.submit("c1", "", "j1", false)
	final := Final{"j1": job("j1", store.StatusPending, 0, "", 0, ev("SUBMITTED", "", 0))}
	mustFlag(t, Check(&b.h, final, Options{}), Quiesced)
}

func TestIdempotencyViolationFlagged(t *testing.T) {
	var b builder
	b.submit("c1", "k1", "j1", false)
	b.submit("c2", "k1", "j2", false)
	final := Final{
		"j1": withKey(job("j1", store.StatusSucceeded, 1, "w1", 1, ev("SUBMITTED", "", 0), ev("LEASED", "w1", 1), ev("SUCCEEDED", "w1", 1)), "k1"),
		"j2": withKey(job("j2", store.StatusSucceeded, 1, "w1", 1, ev("SUBMITTED", "", 0), ev("LEASED", "w1", 1), ev("SUCCEEDED", "w1", 1)), "k1"),
	}
	rep := Check(&b.h, final, Options{})
	mustFlag(t, rep, Idempotency)
	if !strings.Contains(strings.Join(rep.Result(Idempotency).Examples, "\n"), "2 different jobs") {
		t.Fatalf("examples = %v", rep.Result(Idempotency).Examples)
	}
}

func TestEarlyDeadFlagged(t *testing.T) {
	var b builder
	b.submit("c1", "", "j1", false)
	final := Final{"j1": job("j1", store.StatusDead, 1, "w1", 1,
		ev("SUBMITTED", "", 0), ev("LEASED", "w1", 1), ev("DEAD", "w1", 1))}
	mustFlag(t, Check(&b.h, final, Options{}), Attempts)
}

func TestStaleReadFlagged(t *testing.T) {
	// A read that returns the state before a completion that had already
	// been acknowledged is not linearizable.
	var b builder
	b.submit("c1", "", "j1", false)
	b.acquire("w1", "j1", 1, 1)
	b.complete("w1", "j1", 1, "")
	b.op("c1", history.Record{Method: history.Get, JobID: "j1"}, history.OK,
		history.Record{JobID: "j1", Status: store.StatusLeased, Token: 1, Attempts: 1, Owner: "w1"})
	final := Final{"j1": job("j1", store.StatusSucceeded, 1, "w1", 1,
		ev("SUBMITTED", "", 0), ev("LEASED", "w1", 1), ev("SUCCEEDED", "w1", 1))}
	mustFlag(t, Check(&b.h, final, Options{}), Linearizable)
}

func withKey(f FinalJob, key string) FinalJob {
	f.Job.IdempotencyKey = key
	return f
}
