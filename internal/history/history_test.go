package history

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecordAndRead(t *testing.T) {
	dir := t.TempDir()
	r, err := Open(dir, "client")
	if err != nil {
		t.Fatal(err)
	}
	a := r.Begin(Record{Method: Submit, Key: "k"})
	b := r.Begin(Record{Method: Complete, JobID: "j", Token: 3})
	r.End(a, OK, Record{JobID: "j"})
	r.Note(Record{Method: ExecStart, JobID: "j"})
	// b never completes: the process "dies" mid-call.
	_ = b
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	// A second incarnation that was killed mid-write.
	path := filepath.Join(dir, "worker-x.jsonl")
	if err := os.WriteFile(path, []byte(`{"p":"worker-x","op":1,"type":"invoke","t":1,"m":"acquire"}`+"\n"+`{"p":"wor`), 0o644); err != nil {
		t.Fatal(err)
	}

	h, err := ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Ops) != 3 || len(h.Notes) != 1 {
		t.Fatalf("ops=%d notes=%d", len(h.Ops), len(h.Notes))
	}
	byMethod := map[string]Op{}
	for _, op := range h.Ops {
		byMethod[op.Invoke.Method] = op
	}
	if op := byMethod[Submit]; op.Type() != OK || op.Done.JobID != "j" || op.Invoke.Key != "k" {
		t.Fatalf("submit = %+v", op)
	}
	if op := byMethod[Complete]; op.Type() != Info || op.Invoke.Token != 3 {
		t.Fatalf("unfinished complete = %+v", op)
	}
	if op := byMethod[Acquire]; op.Type() != Info {
		t.Fatalf("killed acquire = %+v", op)
	}
}

func TestMalformedMiddleLineIsAnError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "x.jsonl"), []byte("garbage\n{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadDir(dir); err == nil {
		t.Fatal("expected an error")
	}
}

func TestReturnBeforeInvokeIsRejected(t *testing.T) {
	dir := t.TempDir()
	lines := `{"p":"x","op":1,"type":"invoke","t":100,"m":"get"}` + "\n" + `{"p":"x","op":1,"type":"ok","t":90,"m":"get"}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "x.jsonl"), []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadDir(dir); err == nil {
		t.Fatal("an op that returned before it was invoked was accepted")
	}
}

func TestClockIsMonotonic(t *testing.T) {
	prev, _ := clockNow()
	for i := 0; i < 10000; i++ {
		now, wall := clockNow()
		if now < prev || wall == 0 {
			t.Fatalf("clock went backwards: %d after %d", now, prev)
		}
		prev = now
	}
}
