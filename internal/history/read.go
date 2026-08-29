package history

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Op is an invoke paired with its completion.
type Op struct {
	Invoke Record
	// Done is nil when the process recorded no completion (it was killed,
	// or the run ended mid-call). Such an op is indeterminate.
	Done *Record
}

// Type is OK, Fail or Info. A missing completion counts as Info.
func (o Op) Type() string {
	if o.Done == nil {
		return Info
	}
	return o.Done.Type
}

// Code is the gRPC code of a completed op ("" on success).
func (o Op) Code() string {
	if o.Done == nil {
		return ""
	}
	return o.Done.Code
}

// History is every op and note recorded in one run.
type History struct {
	Ops   []Op
	Notes []Record
}

// ReadDir loads every *.jsonl file in dir. A truncated final line (a
// process killed mid-write) is ignored; any other malformed line, or an op
// that completed before it was invoked, is an error.
func ReadDir(dir string) (*History, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	h := &History{}
	for _, f := range files {
		if err := h.readFile(f); err != nil {
			return nil, err
		}
	}
	for _, op := range h.Ops {
		if op.Done != nil && op.Done.TimeNs < op.Invoke.TimeNs {
			return nil, fmt.Errorf("%s op %d (%s) completed %dns before it was invoked: history clock is not monotonic",
				op.Invoke.Process, op.Invoke.Op, op.Invoke.Method, op.Invoke.TimeNs-op.Done.TimeNs)
		}
	}
	sort.SliceStable(h.Ops, func(i, j int) bool { return h.Ops[i].Invoke.TimeNs < h.Ops[j].Invoke.TimeNs })
	sort.SliceStable(h.Notes, func(i, j int) bool { return h.Notes[i].TimeNs < h.Notes[j].TimeNs })
	return h, nil
}

func (h *History) readFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	var recs []Record
	var bad error
	for line := 1; sc.Scan(); line++ {
		if bad != nil {
			return bad // a malformed line that was not the last one
		}
		var r Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			bad = fmt.Errorf("%s:%d: %w", path, line, err)
			continue
		}
		recs = append(recs, r)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	h.add(recs)
	return nil
}

func (h *History) add(recs []Record) {
	type key struct {
		p  string
		op uint64
	}
	open := map[key]int{}
	for _, r := range recs {
		switch r.Type {
		case Note:
			h.Notes = append(h.Notes, r)
		case Invoke:
			open[key{r.Process, r.Op}] = len(h.Ops)
			h.Ops = append(h.Ops, Op{Invoke: r})
		default:
			k := key{r.Process, r.Op}
			if i, ok := open[k]; ok {
				done := r
				h.Ops[i].Done = &done
				delete(open, k)
			}
		}
	}
}
