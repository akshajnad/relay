// Package history records every client-visible Relay operation as JSONL, in
// the style of a Jepsen history: an invoke record when a call starts and an
// ok, fail or info record when it ends. internal/check reads these files.
//
// Each record is written with a single write(2) on an unbuffered file, so a
// process killed with SIGKILL loses nothing it had already recorded. An op
// whose invoke has no matching completion is indeterminate (info): it may
// or may not have taken effect.
//
// Ordering timestamps come from CLOCK_MONOTONIC, which on Linux is one
// system-wide clock that never steps backwards, so histories from every
// process on the same kernel are comparable. The chaos harness therefore
// runs every recorded process inside the same Docker VM. (The wall clock is
// not safe for this: a chaos run caught the VM's wall clock stepping back
// 3.4ms, which made a read "return" before it was invoked.) Each record
// also carries the wall clock, for correlating with external events.
package history

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// Record types.
const (
	Invoke = "invoke"
	OK     = "ok"
	// Fail means the op definitely did not take effect.
	Fail = "fail"
	// Info means the outcome is unknown.
	Info = "info"
	// Note is a point event that is not an operation (e.g. a handler
	// starting), used for timing.
	Note = "note"
)

// Methods recorded.
const (
	Submit    = "submit"
	Get       = "get"
	Register  = "register"
	Heartbeat = "heartbeat"
	Acquire   = "acquire"
	Complete  = "complete"
	FailJob   = "fail"
	// Notes.
	ExecStart = "exec_start"
	ExecEnd   = "exec_end"
)

// Record is one line of a history file. Invoke records carry the inputs;
// completion records carry the outputs and repeat Process and Op so the two
// can be paired.
type Record struct {
	Process string `json:"p"`
	Op      uint64 `json:"op"`
	Type    string `json:"type"`
	// TimeNs is CLOCK_MONOTONIC; WallNs is the wall clock at the same time.
	TimeNs int64  `json:"t"`
	WallNs int64  `json:"wall,omitempty"`
	Method string `json:"m"`

	// Inputs (and, for acquire/submit/get, outputs).
	Worker      string `json:"w,omitempty"`
	JobID       string `json:"job,omitempty"`
	Token       int64  `json:"tok,omitempty"`
	Key         string `json:"key,omitempty"`
	MaxAttempts int    `json:"max,omitempty"`
	Retryable   bool   `json:"retryable,omitempty"`

	// Outputs.
	Duplicate bool   `json:"dup,omitempty"`
	Already   bool   `json:"already,omitempty"`
	Status    string `json:"status,omitempty"`
	Attempts  int    `json:"attempts,omitempty"`
	Owner     string `json:"owner,omitempty"`
	HasJob    bool   `json:"has_job,omitempty"`
	Code      string `json:"code,omitempty"`
	Err       string `json:"err,omitempty"`
}

// Recorder appends records to one file. It is safe for concurrent use.
type Recorder struct {
	process string
	mu      sync.Mutex
	f       *os.File
	next    atomic.Uint64
	now     func() (mono, wall int64)
}

// Open creates a new history file in dir for this process incarnation. The
// name combines hostname, pid and start time, so a restarted container
// never appends to its previous incarnation's file.
func Open(dir, role string) (*Recorder, error) {
	host, _ := os.Hostname()
	proc := fmt.Sprintf("%s-%s-%d-%d", role, host, os.Getpid(), time.Now().UnixNano())
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, proc+".jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	return &Recorder{process: proc, f: f, now: clockNow}, nil
}

// Process returns this recorder's process name.
func (r *Recorder) Process() string { return r.process }

func (r *Recorder) write(rec *Record) {
	rec.Process = r.process
	line, err := json.Marshal(rec)
	if err != nil {
		panic(err) // Record has only plain fields
	}
	line = append(line, '\n')
	r.mu.Lock()
	defer r.mu.Unlock()
	// One write call per record; a short write would corrupt the line, and
	// on a regular file it only happens when the disk is full.
	if _, err := r.f.Write(line); err != nil {
		fmt.Fprintln(os.Stderr, "history: write failed:", err)
	}
}

// Begin records an invoke and returns the op id to finish it with.
func (r *Recorder) Begin(rec Record) uint64 {
	rec.Op = r.next.Add(1)
	rec.Type = Invoke
	rec.TimeNs, rec.WallNs = r.now()
	r.write(&rec)
	return rec.Op
}

// End records the completion of op. typ is OK, Fail or Info.
func (r *Recorder) End(op uint64, typ string, rec Record) {
	rec.Op = op
	rec.Type = typ
	rec.TimeNs, rec.WallNs = r.now()
	r.write(&rec)
}

// Note records a point event.
func (r *Recorder) Note(rec Record) {
	rec.Op = r.next.Add(1)
	rec.Type = Note
	rec.TimeNs, rec.WallNs = r.now()
	r.write(&rec)
}

func (r *Recorder) Close() error { return r.f.Close() }

// clockNow reads CLOCK_MONOTONIC and the wall clock.
func clockNow() (mono, wall int64) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		panic(fmt.Sprintf("history: CLOCK_MONOTONIC: %v", err))
	}
	return ts.Nano(), time.Now().UnixNano()
}
