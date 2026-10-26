// Package check verifies a chaos run. It takes the recorded client and
// worker history (internal/history) plus every job's final state and server
// event log, read after the cluster quiesced, and checks Relay's safety
// invariants and per-job linearizability.
package check

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/akshajnad/relay/internal/history"
	"github.com/akshajnad/relay/internal/store"
	"github.com/anishathalye/porcupine"
)

// FinalJob is one job as read after quiescing, with its server-side
// transition log.
type FinalJob struct {
	Job    store.Job     `json:"job"`
	Events []store.Event `json:"events"`
	// ReadErr is set when the final read failed (e.g. NOT_FOUND).
	ReadErr string `json:"read_err,omitempty"`
}

// Final is the post-quiesce snapshot, keyed by job id.
type Final map[string]FinalJob

// Invariant names, in report order.
const (
	NoLostAcks     = "no-lost-acks"
	Quiesced       = "all-jobs-terminal"
	TerminalOnce   = "terminal-at-most-once"
	Fencing        = "fencing"
	Idempotency    = "idempotency"
	Attempts       = "attempts-bounded"
	Linearizable   = "linearizable"
	maxExamples    = 10
	linearizeLimit = 5 * time.Minute
)

var invariantOrder = []string{NoLostAcks, Quiesced, TerminalOnce, Fencing, Idempotency, Attempts, Linearizable}

// Result is one invariant's outcome.
type Result struct {
	Name       string   `json:"name"`
	Checked    int      `json:"checked"`
	Violations int      `json:"violations"`
	Examples   []string `json:"examples,omitempty"`
}

func (r Result) OK() bool { return r.Violations == 0 }

// Report is the checker's verdict.
type Report struct {
	Jobs          int      `json:"jobs"`
	Ops           int      `json:"ops"`
	Indeterminate int      `json:"indeterminate_ops"`
	Results       []Result `json:"results"`
	// NonLinearizable lists job ids whose history porcupine rejected.
	NonLinearizable []string `json:"non_linearizable,omitempty"`

	lin   map[string]porcupine.LinearizationInfo
	model porcupine.Model
}

func (r *Report) OK() bool {
	for _, res := range r.Results {
		if !res.OK() {
			return false
		}
	}
	return true
}

// Result returns the named invariant's result.
func (r *Report) Result(name string) Result {
	for _, res := range r.Results {
		if res.Name == name {
			return res
		}
	}
	return Result{Name: name}
}

func (r *Report) String() string {
	var b strings.Builder
	verdict := "PASS"
	if !r.OK() {
		verdict = "FAIL"
	}
	fmt.Fprintf(&b, "%s: %d jobs, %d ops (%d indeterminate)\n", verdict, r.Jobs, r.Ops, r.Indeterminate)
	for _, res := range r.Results {
		mark := "ok  "
		if !res.OK() {
			mark = "FAIL"
		}
		fmt.Fprintf(&b, "  [%s] %-22s checked=%d violations=%d\n", mark, res.Name, res.Checked, res.Violations)
		for _, ex := range res.Examples {
			fmt.Fprintf(&b, "         - %s\n", ex)
		}
	}
	return b.String()
}

// Visualize writes a porcupine visualization for each non-linearizable job
// (at most limit) into dir and returns the paths.
func (r *Report) Visualize(dir string, limit int) ([]string, error) {
	var paths []string
	for i, id := range r.NonLinearizable {
		if i >= limit {
			break
		}
		p := filepath.Join(dir, "linearizability-"+id+".html")
		if err := porcupine.VisualizePath(r.model, r.lin[id], p); err != nil {
			return paths, err
		}
		paths = append(paths, p)
	}
	return paths, nil
}

type collector struct {
	res Result
}

func (c *collector) check() { c.res.Checked++ }
func (c *collector) fail(format string, a ...any) {
	c.res.Violations++
	if len(c.res.Examples) < maxExamples {
		c.res.Examples = append(c.res.Examples, fmt.Sprintf(format, a...))
	}
}

// Options tune the checker.
type Options struct {
	// DefaultMaxAttempts is the server default applied to submits with
	// max_attempts 0.
	DefaultMaxAttempts int
}

// Check runs every invariant.
func Check(h *history.History, final Final, opt Options) *Report {
	if opt.DefaultMaxAttempts == 0 {
		opt.DefaultMaxAttempts = store.DefaultConfig().DefaultMaxAttempts
	}
	idx := index(h)
	rep := &Report{Jobs: len(idx.acked), Ops: len(h.Ops)}
	for _, op := range h.Ops {
		if op.Type() == history.Info {
			rep.Indeterminate++
		}
	}
	results := map[string]Result{
		NoLostAcks:   checkNoLostAcks(idx, final),
		Quiesced:     checkQuiesced(idx, final),
		TerminalOnce: checkTerminalOnce(idx, final),
		Fencing:      checkFencing(idx, final),
		Idempotency:  checkIdempotency(idx, final),
		Attempts:     checkAttempts(idx, final),
	}
	results[Linearizable] = rep.checkLinearizable(idx, opt)
	for _, name := range invariantOrder {
		rep.Results = append(rep.Results, results[name])
	}
	return rep
}

// idx groups the history by job.
type idx struct {
	// acked maps every job id some submit was acknowledged with to the
	// first acknowledging op.
	acked map[string]history.Op
	// byJob holds every op attributable to a job.
	byJob map[string][]history.Op
	// keyJob maps idempotency key -> set of job ids it was acked with.
	keyJob map[string]map[string]bool
	// jobKey maps job id -> idempotency key seen on the job itself.
	jobKeys map[string]map[string]bool
	// keySubmits holds every submit op per key.
	keySubmits map[string][]history.Op
}

func index(h *history.History) *idx {
	x := &idx{
		acked:      map[string]history.Op{},
		byJob:      map[string][]history.Op{},
		keyJob:     map[string]map[string]bool{},
		jobKeys:    map[string]map[string]bool{},
		keySubmits: map[string][]history.Op{},
	}
	addSet := func(m map[string]map[string]bool, k, v string) {
		if m[k] == nil {
			m[k] = map[string]bool{}
		}
		m[k][v] = true
	}
	for _, op := range h.Ops {
		in := op.Invoke
		switch in.Method {
		case history.Submit:
			if in.Key != "" {
				x.keySubmits[in.Key] = append(x.keySubmits[in.Key], op)
			}
			if op.Type() == history.OK && op.Code() == "" {
				id := op.Done.JobID
				if _, seen := x.acked[id]; !seen {
					x.acked[id] = op
				}
				if in.Key != "" {
					addSet(x.keyJob, in.Key, id)
				}
			}
		case history.Acquire, history.Get:
			if op.Type() == history.OK && op.Done.JobID != "" && op.Done.Key != "" {
				addSet(x.jobKeys, op.Done.JobID, op.Done.Key)
			}
		}
	}
	for _, op := range h.Ops {
		if id := jobOf(op, x); id != "" {
			x.byJob[id] = append(x.byJob[id], op)
		}
	}
	return x
}

// jobOf attributes an op to a job, or returns "" when it cannot be (an
// acquire that returned nothing or whose reply was lost, a heartbeat), or
// when it certainly had no effect.
func jobOf(op history.Op, x *idx) string {
	in := op.Invoke
	if op.Type() == history.Fail {
		return ""
	}
	switch in.Method {
	case history.Submit:
		if op.Type() == history.OK && op.Code() == "" {
			return op.Done.JobID
		}
		// An unacknowledged submit belongs to the job its key resolved to,
		// if any submit with that key was ever acknowledged.
		if ids := x.keyJob[in.Key]; in.Key != "" && len(ids) == 1 {
			for id := range ids {
				return id
			}
		}
		return ""
	case history.Acquire:
		if op.Type() == history.OK && op.Code() == "" && op.Done.HasJob {
			return op.Done.JobID
		}
		return ""
	case history.Complete, history.FailJob, history.Get:
		return in.JobID
	}
	return ""
}

func checkNoLostAcks(x *idx, final Final) Result {
	c := collector{res: Result{Name: NoLostAcks}}
	for _, id := range sortedKeys(x.acked) {
		c.check()
		f, ok := final[id]
		if !ok || f.ReadErr != "" || f.Job.ID != id {
			c.fail("job %s was acknowledged at %s but is missing (read: %q)", id, ts(x.acked[id].Done.TimeNs), f.ReadErr)
		}
	}
	return c.res
}

func checkQuiesced(x *idx, final Final) Result {
	c := collector{res: Result{Name: Quiesced}}
	for _, id := range sortedKeys(x.acked) {
		f, ok := final[id]
		if !ok || f.ReadErr != "" {
			continue // reported as lost
		}
		c.check()
		if !store.Terminal(f.Job.Status) {
			c.fail("job %s still %s after quiescing (attempts %d/%d, owner %q, token %d)",
				id, f.Job.Status, f.Job.Attempts, f.Job.MaxAttempts, f.Job.LeaseOwner, f.Job.LeaseToken)
		}
	}
	return c.res
}

func checkTerminalOnce(x *idx, final Final) Result {
	c := collector{res: Result{Name: TerminalOnce}}
	for _, id := range sortedKeys(final) {
		f := final[id]
		if f.ReadErr != "" {
			continue
		}
		c.check()
		var terms []store.Event
		for _, e := range f.Events {
			if store.Terminal(e.Kind) {
				terms = append(terms, e)
			}
		}
		if len(terms) > 1 {
			c.fail("job %s has %d terminal transitions in its server history: %s", id, len(terms), describeEvents(terms))
		}
		if len(terms) == 1 {
			last := f.Events[len(f.Events)-1]
			if !store.Terminal(last.Kind) {
				c.fail("job %s transitioned after its terminal %s: %s", id, terms[0].Kind, describeEvents(f.Events))
			}
			if f.Job.Status != terms[0].Kind {
				c.fail("job %s status %s disagrees with its terminal event %s", id, f.Job.Status, terms[0].Kind)
			}
		}
		if store.Terminal(f.Job.Status) && len(terms) == 0 {
			c.fail("job %s is %s with no terminal event", id, f.Job.Status)
		}
	}
	// Client side: at most one lease may have its terminal report accepted.
	for _, id := range sortedKeys(x.byJob) {
		tokens := map[int64]string{}
		for _, op := range x.byJob[id] {
			if op.Type() != history.OK || op.Code() != "" {
				continue
			}
			in := op.Invoke
			switch {
			case in.Method == history.Complete && !op.Done.Already:
				tokens[in.Token] = "SUCCEEDED"
			case in.Method == history.FailJob && store.Terminal(op.Done.Status):
				tokens[in.Token] = op.Done.Status
			}
		}
		if len(tokens) > 1 {
			c.fail("job %s: terminal reports from %d different leases were accepted: %v", id, len(tokens), tokens)
		}
	}
	return c.res
}

func checkFencing(x *idx, final Final) Result {
	c := collector{res: Result{Name: Fencing}}
	// Server side: every transition a lease holder made must carry the
	// token of the job's current lease, which is the latest LEASED event.
	for _, id := range sortedKeys(final) {
		f := final[id]
		if f.ReadErr != "" {
			continue
		}
		var cur *store.Event
		var lastTok int64
		for i := range f.Events {
			e := &f.Events[i]
			switch e.Kind {
			case "LEASED":
				if e.Token <= lastTok {
					c.fail("job %s: lease token %d does not exceed previous %d", id, e.Token, lastTok)
				}
				lastTok = e.Token
				cur = e
			case "SUCCEEDED", "FAILED", "RETRY":
				// Holder-reported transitions (DEAD may also come from revocation).
				c.check()
				if cur == nil || cur.Token != e.Token || cur.Worker != e.Worker {
					c.fail("job %s: %s by %s with token %d accepted while the current lease was %s",
						id, e.Kind, e.Worker, e.Token, leaseDesc(cur))
				}
				cur = nil
			case "DEAD":
				c.check()
				if cur == nil || cur.Token != e.Token {
					c.fail("job %s: DEAD with token %d while the current lease was %s", id, e.Token, leaseDesc(cur))
				}
				cur = nil
			case "REQUEUED", "RELEASED":
				// The lease ended; nothing may report on it any more.
				cur = nil
			}
		}
	}
	// Client side: an accepted report must name the job's final current
	// lease at acceptance, so no newer lease may have been handed out
	// before the report was even sent.
	for _, id := range sortedKeys(x.byJob) {
		ops := x.byJob[id]
		for _, op := range ops {
			in := op.Invoke
			if (in.Method != history.Complete && in.Method != history.FailJob) || op.Type() != history.OK || op.Code() != "" {
				continue
			}
			if in.Method == history.Complete && op.Done.Already {
				continue
			}
			c.check()
			for _, other := range ops {
				if other.Invoke.Method == history.Acquire && other.Type() == history.OK && other.Done.Token > in.Token &&
					other.Done.TimeNs < in.TimeNs {
					c.fail("job %s: %s with stale token %d by %s accepted at %s; token %d had been granted to %s at %s",
						id, in.Method, in.Token, in.Worker, ts(op.Done.TimeNs), other.Done.Token, other.Done.Owner, ts(other.Done.TimeNs))
					break
				}
			}
			if in.Method == history.Complete {
				if f, ok := final[id]; ok && f.ReadErr == "" {
					if ev := lastOf(f.Events, "SUCCEEDED"); ev == nil || ev.Token != in.Token || ev.Worker != in.Worker {
						c.fail("job %s: completion by %s with token %d was acknowledged but the stored success is %s",
							id, in.Worker, in.Token, leaseDesc(ev))
					}
				}
			}
		}
	}
	return c.res
}

func checkIdempotency(x *idx, final Final) Result {
	c := collector{res: Result{Name: Idempotency}}
	keys := make([]string, 0, len(x.keySubmits))
	for k := range x.keySubmits {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		c.check()
		ids := x.keyJob[k]
		if len(ids) > 1 {
			c.fail("key %q was acknowledged with %d different jobs: %v", k, len(ids), setKeys(ids))
		}
		created := 0
		for _, op := range x.keySubmits[k] {
			if op.Type() == history.OK && op.Code() == "" && !op.Done.Duplicate {
				created++
			}
		}
		if created > 1 {
			c.fail("key %q: %d submits each reported creating a new job", k, created)
		}
		for id := range ids {
			if f, ok := final[id]; ok && f.ReadErr == "" && f.Job.IdempotencyKey != k {
				c.fail("job %s was returned for key %q but stores key %q", id, k, f.Job.IdempotencyKey)
			}
		}
	}
	// Every job the history saw carrying a key must be that key's job.
	for _, id := range sortedKeys(x.jobKeys) {
		for k := range x.jobKeys[id] {
			if ids := x.keyJob[k]; len(ids) > 0 && !ids[id] {
				c.fail("job %s carries key %q, which was acknowledged as job(s) %v", id, k, setKeys(ids))
			}
		}
	}
	return c.res
}

func checkAttempts(x *idx, final Final) Result {
	c := collector{res: Result{Name: Attempts}}
	for _, id := range sortedKeys(final) {
		f := final[id]
		if f.ReadErr != "" {
			continue
		}
		c.check()
		j := f.Job
		if j.Attempts > j.MaxAttempts {
			c.fail("job %s: %d attempts exceed max_attempts %d", id, j.Attempts, j.MaxAttempts)
		}
		if j.Status == store.StatusDead && j.Attempts != j.MaxAttempts {
			c.fail("job %s is DEAD after %d of %d attempts", id, j.Attempts, j.MaxAttempts)
		}
		leased := 0
		for _, e := range f.Events {
			switch e.Kind {
			case "LEASED":
				leased++
			case "RELEASED":
				leased--
			}
		}
		if leased != j.Attempts {
			c.fail("job %s: %d counted lease attempts in history but attempts=%d", id, leased, j.Attempts)
		}
	}
	for _, id := range sortedKeys(x.byJob) {
		for _, op := range x.byJob[id] {
			if op.Invoke.Method == history.Acquire && op.Type() == history.OK && op.Done.Attempts > op.Done.MaxAttempts {
				c.fail("job %s leased on attempt %d of max %d", id, op.Done.Attempts, op.Done.MaxAttempts)
			}
		}
	}
	return c.res
}

func (r *Report) checkLinearizable(x *idx, opt Options) Result {
	c := collector{res: Result{Name: Linearizable}}
	r.model = Model(opt.DefaultMaxAttempts)
	r.lin = map[string]porcupine.LinearizationInfo{}
	for _, id := range sortedKeys(x.byJob) {
		ops := toPorcupine(x.byJob[id])
		c.check()
		res, info := porcupine.CheckOperationsVerbose(r.model, ops, linearizeLimit)
		switch res {
		case porcupine.Illegal:
			r.NonLinearizable = append(r.NonLinearizable, id)
			r.lin[id] = info
			c.fail("job %s: history of %d ops is not linearizable", id, len(ops))
		case porcupine.Unknown:
			c.fail("job %s: linearizability check timed out after %v", id, linearizeLimit)
		}
	}
	return c.res
}

// toPorcupine converts one job's ops. Each process is a porcupine client.
func toPorcupine(ops []history.Op) []porcupine.Operation {
	clients := map[string]int{}
	out := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		in := op.Invoke
		cid, ok := clients[in.Process]
		if !ok {
			cid = len(clients)
			clients[in.Process] = cid
		}
		pi := opInput{Method: in.Method, Worker: in.Worker, Token: in.Token, Retryable: in.Retryable, Max: in.MaxAttempts}
		var po opOutput
		ret := int64(math.MaxInt64)
		if op.Type() == history.Info {
			po.Unknown = true
		} else {
			d := op.Done
			ret = d.TimeNs
			po = opOutput{Code: d.Code, Dup: d.Duplicate, Already: d.Already, Status: d.Status,
				Token: d.Token, Attempts: d.Attempts, Owner: d.Owner}
		}
		out = append(out, porcupine.Operation{ClientId: cid, Input: pi, Call: in.TimeNs, Output: po, Return: ret})
	}
	return out
}

// WriteJSON saves the report.
func (r *Report) WriteJSON(path string) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func lastOf(ev []store.Event, kind string) *store.Event {
	for i := len(ev) - 1; i >= 0; i-- {
		if ev[i].Kind == kind {
			return &ev[i]
		}
	}
	return nil
}

func leaseDesc(e *store.Event) string {
	if e == nil {
		return "none"
	}
	return fmt.Sprintf("token %d held by %s", e.Token, e.Worker)
}

func describeEvents(ev []store.Event) string {
	parts := make([]string, len(ev))
	for i, e := range ev {
		parts[i] = fmt.Sprintf("%s(tok=%d,%s)", e.Kind, e.Token, e.Worker)
	}
	return strings.Join(parts, " ")
}

func ts(ns int64) string { return time.Unix(0, ns).UTC().Format("15:04:05.000000") }

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func setKeys(m map[string]bool) []string { return sortedKeys(m) }
