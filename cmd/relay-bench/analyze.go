package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/akshajnad/relay/internal/history"
)

// Dist is a latency distribution in milliseconds.
type Dist struct {
	Count  int     `json:"count"`
	MeanMs float64 `json:"mean_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P90Ms  float64 `json:"p90_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MaxMs  float64 `json:"max_ms"`
	MinMs  float64 `json:"min_ms"`
}

// distOf computes exact percentiles (nearest rank).
func distOf(ds []time.Duration) Dist {
	if len(ds) == 0 {
		return Dist{}
	}
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	q := func(p float64) float64 {
		i := int(math.Ceil(p*float64(len(s)))) - 1
		return ms(s[max(0, min(i, len(s)-1))])
	}
	var sum time.Duration
	for _, d := range s {
		sum += d
	}
	return Dist{Count: len(s), MeanMs: ms(sum / time.Duration(len(s))), P50Ms: q(0.5), P90Ms: q(0.9),
		P95Ms: q(0.95), P99Ms: q(0.99), MaxMs: ms(s[len(s)-1]), MinMs: ms(s[0])}
}

// tailer incrementally reads every history file in a directory, so live
// trials can poll cheaply.
type tailer struct {
	dir string
	off map[string]int64
}

func newTailer(dir string) *tailer { return &tailer{dir: dir, off: map[string]int64{}} }

// poll returns the complete records appended since the last call.
func (t *tailer) poll() ([]history.Record, error) {
	files, err := filepath.Glob(filepath.Join(t.dir, "*.jsonl"))
	if err != nil {
		return nil, err
	}
	var out []history.Record
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			return nil, err
		}
		if _, err := fh.Seek(t.off[f], io.SeekStart); err != nil {
			fh.Close()
			return nil, err
		}
		r := bufio.NewReaderSize(fh, 1<<20)
		for {
			line, err := r.ReadBytes('\n')
			if err != nil {
				break // a partial last line is picked up next time
			}
			t.off[f] += int64(len(line))
			var rec history.Record
			if json.Unmarshal(bytes.TrimSpace(line), &rec) == nil {
				out = append(out, rec)
			}
		}
		fh.Close()
	}
	return out, nil
}

// phase is one benchmark phase's recorded history.
type phase struct {
	// submit is the first submit invocation per job (mono ns), for acked
	// submits whose key has the phase's prefix.
	submit map[string]int64
	// rtt is each acknowledged submit's round trip.
	rtt []time.Duration
	// notes are handler start/end notes, per worker process, in time order.
	notes map[string][]history.Record
}

func loadPhase(dir, keyPrefix string) (*phase, error) {
	h, err := history.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	p := &phase{submit: map[string]int64{}, notes: map[string][]history.Record{}}
	firstInvoke := map[string]int64{} // key -> first invoke
	for _, op := range h.Ops {
		if op.Invoke.Method != history.Submit || !strings.HasPrefix(op.Invoke.Key, keyPrefix+"-") {
			continue
		}
		if t, ok := firstInvoke[op.Invoke.Key]; !ok || op.Invoke.TimeNs < t {
			firstInvoke[op.Invoke.Key] = op.Invoke.TimeNs
		}
		if op.Type() == history.OK {
			p.rtt = append(p.rtt, time.Duration(op.Done.TimeNs-op.Invoke.TimeNs))
			if _, seen := p.submit[op.Done.JobID]; !seen {
				p.submit[op.Done.JobID] = firstInvoke[op.Invoke.Key]
			}
		}
	}
	for _, n := range h.Notes {
		p.notes[n.Process] = append(p.notes[n.Process], n)
	}
	return p, nil
}

// span returns the first handler start and last handler end among the
// phase's jobs, and how many distinct phase jobs finished.
func (p *phase) span() (first, last int64, done int) {
	finished := map[string]bool{}
	first = math.MaxInt64
	for _, ns := range p.notes {
		for _, n := range ns {
			if _, ok := p.submit[n.JobID]; !ok {
				continue
			}
			switch n.Method {
			case history.ExecStart:
				first = min(first, n.TimeNs)
			case history.ExecEnd:
				last = max(last, n.TimeNs)
				finished[n.JobID] = true
			}
		}
	}
	return first, last, len(finished)
}

// dispatch computes, for the first execution of each phase job, the
// externally observed dispatch latency
//
//	handler start - max(submit invoked, a slot on that worker became free)
//
// Free slots are simulated per worker process from its own start/end
// notes: concurrency slots start free, a handler start takes the slot
// that has been free longest (FIFO), a handler end frees one. This is the
// same for every backend. A slot counts as free from the moment the
// previous handler returned, so the latency includes reporting the
// previous result and requesting the next job. Queue wait is handler
// start - submit invoked.
func (p *phase) dispatch(concurrency int) (dispatch, queueWait []time.Duration) {
	seen := map[string]bool{}
	for _, ns := range p.notes {
		free := make([]int64, concurrency) // free-since times, FIFO
		for _, n := range ns {
			switch n.Method {
			case history.ExecStart:
				since := n.TimeNs
				if len(free) > 0 {
					since, free = free[0], free[1:]
				}
				sub, ok := p.submit[n.JobID]
				if !ok || seen[n.JobID] {
					continue
				}
				seen[n.JobID] = true
				dispatch = append(dispatch, time.Duration(n.TimeNs-max(sub, since)))
				queueWait = append(queueWait, time.Duration(n.TimeNs-sub))
			case history.ExecEnd:
				free = append(free, n.TimeNs)
			}
		}
	}
	return dispatch, queueWait
}

// executions counts, per phase job, handler starts and completed runs.
func (p *phase) executions() (starts, ends map[string]int) {
	starts, ends = map[string]int{}, map[string]int{}
	for _, ns := range p.notes {
		for _, n := range ns {
			if _, ok := p.submit[n.JobID]; !ok {
				continue
			}
			switch n.Method {
			case history.ExecStart:
				starts[n.JobID]++
			case history.ExecEnd:
				ends[n.JobID]++
			}
		}
	}
	return starts, ends
}

// countLines counts occurrences of a substring in every history file.
func countLines(dir, needle string) int {
	files, _ := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	n := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err == nil {
			n += bytes.Count(b, []byte(needle))
		}
	}
	return n
}
