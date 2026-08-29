package check

import (
	"fmt"

	"github.com/akshajnad/relay/internal/history"
	"github.com/akshajnad/relay/internal/store"
	"github.com/anishathalye/porcupine"
)

// jobState is the model of one job. It mirrors exactly the fields the
// stores' fencing logic reads (status, owner, token, attempts, reported),
// abstracting tokens to "the last one observed".
type jobState struct {
	Exists   bool
	Status   string
	Max      int
	Attempts int
	// Last is the highest lease token the history has observed for the job.
	Last int64
	// Hidden means the job's current or most recent lease is one no client
	// observed (its AcquireJob reply was lost): its token is unknown but
	// greater than Last, and its owner is unknown.
	Hidden bool
	// Owner is the observed owner of the lease with token Last.
	Owner string
	// Reported is the token whose holder reported the last outcome (the
	// stores' reported_token), so a retried report is recognized.
	Reported int64
}

// opInput and opOutput are porcupine's Input and Output for one op.
type opInput struct {
	Method    string
	Worker    string
	Token     int64
	Retryable bool
	Max       int
}

type opOutput struct {
	// Unknown marks an indeterminate op: it may or may not have applied.
	Unknown  bool
	Code     string // gRPC code name of a definite rejection, "" for success
	Dup      bool
	Already  bool
	Status   string
	Token    int64
	Attempts int
	Owner    string
}

// internalClosure returns every state reachable from s by transitions no
// client observes: the reaper or failure detector revoking a lease, a lease
// granted whose reply was lost, and the coordinator releasing such a lease.
func internalClosure(s jobState) []jobState {
	seen := map[jobState]bool{s: true}
	queue := []jobState{s}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, nxt := range internalSteps(cur) {
			if !seen[nxt] {
				seen[nxt] = true
				queue = append(queue, nxt)
			}
		}
	}
	out := make([]jobState, 0, len(seen))
	for st := range seen {
		out = append(out, st)
	}
	return out
}

func internalSteps(s jobState) []jobState {
	if !s.Exists {
		return nil
	}
	var out []jobState
	switch s.Status {
	case store.StatusPending:
		// A pending job always has attempts left (revoke and fail
		// dead-letter it otherwise), so a hidden acquire can lease it.
		if s.Attempts < s.Max {
			n := s
			n.Status, n.Attempts, n.Hidden, n.Owner = store.StatusLeased, s.Attempts+1, true, ""
			out = append(out, n)
		}
	case store.StatusLeased:
		// Revocation: lease expiry or worker death. Owner and token stay.
		n := s
		if s.Attempts >= s.Max {
			n.Status = store.StatusDead
		} else {
			n.Status = store.StatusPending
		}
		out = append(out, n)
		// Release of an undeliverable (hence hidden) lease undoes its attempt.
		if s.Hidden {
			r := s
			r.Status, r.Attempts = store.StatusPending, s.Attempts-1
			out = append(out, r)
		}
	}
	return out
}

func holdsCurrent(s jobState, worker string, token int64) bool {
	return !s.Hidden && s.Owner == worker && s.Last == token
}

// applyComplete returns the state after a successful completion, and
// whether one is possible from s.
func completeFresh(s jobState, in opInput) (jobState, bool) {
	if s.Exists && s.Status == store.StatusLeased && holdsCurrent(s, in.Worker, in.Token) {
		n := s
		n.Status, n.Reported = store.StatusSucceeded, in.Token
		return n, true
	}
	return s, false
}

func completeAlready(s jobState, in opInput) bool {
	return s.Exists && s.Status == store.StatusSucceeded && s.Reported == in.Token && holdsCurrent(s, in.Worker, in.Token)
}

func failFresh(s jobState, in opInput) (jobState, bool) {
	if !(s.Exists && s.Status == store.StatusLeased && holdsCurrent(s, in.Worker, in.Token)) {
		return s, false
	}
	n := s
	n.Reported = in.Token
	switch {
	case in.Retryable && s.Attempts < s.Max:
		n.Status = store.StatusPending
	case in.Retryable:
		n.Status = store.StatusDead
	default:
		n.Status = store.StatusFailed
	}
	return n, true
}

func failAlready(s jobState, in opInput) bool {
	return s.Exists && s.Status != store.StatusLeased && s.Reported == in.Token && holdsCurrent(s, in.Worker, in.Token)
}

// step applies one observed op to one state (after internal transitions).
func step(s jobState, in opInput, out opOutput, defaultMax int) []jobState {
	if out.Unknown {
		// The op may not have happened at all...
		res := []jobState{s}
		// ...or it applied, with any outcome its preconditions allow.
		switch in.Method {
		case history.Submit:
			if !s.Exists {
				res = append(res, created(in, defaultMax))
			}
		case history.Complete:
			if n, ok := completeFresh(s, in); ok {
				res = append(res, n)
			}
		case history.FailJob:
			if n, ok := failFresh(s, in); ok {
				res = append(res, n)
			}
		}
		return res
	}

	switch in.Method {
	case history.Submit:
		if out.Code != "" {
			return nil
		}
		if !s.Exists && !out.Dup {
			return []jobState{created(in, defaultMax)}
		}
		if s.Exists && out.Dup {
			return []jobState{s}
		}
		return nil

	case history.Acquire:
		if s.Exists && s.Status == store.StatusPending && out.Status == store.StatusLeased &&
			out.Token > s.Last && out.Attempts == s.Attempts+1 && out.Owner == in.Worker {
			n := s
			n.Status, n.Attempts, n.Last, n.Hidden, n.Owner = store.StatusLeased, s.Attempts+1, out.Token, false, out.Owner
			return []jobState{n}
		}
		return nil

	case history.Complete:
		switch out.Code {
		case "":
			if out.Already {
				if completeAlready(s, in) {
					return []jobState{s}
				}
				return nil
			}
			if n, ok := completeFresh(s, in); ok {
				return []jobState{n}
			}
			return nil
		case "Aborted":
			if _, ok := completeFresh(s, in); s.Exists && !ok && !completeAlready(s, in) {
				return []jobState{s}
			}
			return nil
		case "NotFound":
			if !s.Exists {
				return []jobState{s}
			}
			return nil
		}
		return nil

	case history.FailJob:
		switch out.Code {
		case "":
			var res []jobState
			if n, ok := failFresh(s, in); ok && n.Status == out.Status {
				res = append(res, n)
			}
			if failAlready(s, in) && s.Status == out.Status {
				res = append(res, s)
			}
			return res
		case "Aborted":
			if _, ok := failFresh(s, in); s.Exists && !ok && !failAlready(s, in) {
				return []jobState{s}
			}
			return nil
		case "NotFound":
			if !s.Exists {
				return []jobState{s}
			}
			return nil
		}
		return nil

	case history.Get:
		switch out.Code {
		case "":
			if !s.Exists || out.Status != s.Status || out.Attempts != s.Attempts {
				return nil
			}
			if s.Hidden {
				if out.Token <= s.Last {
					return nil
				}
				// The read reveals the hidden lease's token and owner.
				n := s
				n.Last, n.Hidden, n.Owner = out.Token, false, out.Owner
				return []jobState{n}
			}
			if out.Token != s.Last || (s.Last != 0 && out.Owner != s.Owner) {
				return nil
			}
			return []jobState{s}
		case "NotFound":
			if !s.Exists {
				return []jobState{s}
			}
			return nil
		}
		return nil
	}
	return nil
}

func created(in opInput, defaultMax int) jobState {
	max := in.Max
	if max <= 0 {
		max = defaultMax
	}
	return jobState{Exists: true, Status: store.StatusPending, Max: max}
}

// Model returns the porcupine model of a single job. Partitioning by job
// is done before checking, so every history passed to it is one job's.
func Model(defaultMax int) porcupine.Model {
	nm := porcupine.NondeterministicModel{
		Init: func() []interface{} { return []interface{}{jobState{}} },
		Step: func(state, input, output interface{}) []interface{} {
			in, out := input.(opInput), output.(opOutput)
			var res []interface{}
			for _, s := range internalClosure(state.(jobState)) {
				for _, n := range step(s, in, out, defaultMax) {
					res = append(res, n)
				}
			}
			return res
		},
		Equal: func(a, b interface{}) bool { return a.(jobState) == b.(jobState) },
		DescribeOperation: func(input, output interface{}) string {
			in, _ := input.(opInput)
			out, _ := output.(opOutput)
			return describeOp(in, out)
		},
		DescribeState: func(state interface{}) string {
			s, _ := state.(jobState)
			if !s.Exists {
				return "absent"
			}
			lease := fmt.Sprintf("tok=%d owner=%s", s.Last, s.Owner)
			if s.Hidden {
				lease = fmt.Sprintf("tok>%d (unobserved lease)", s.Last)
			}
			return fmt.Sprintf("%s att=%d/%d %s reported=%d", s.Status, s.Attempts, s.Max, lease, s.Reported)
		},
	}
	return nm.ToModel()
}

func describeOp(in opInput, out opOutput) string {
	var call string
	switch in.Method {
	case history.Submit:
		call = fmt.Sprintf("submit(max=%d)", in.Max)
	case history.Acquire:
		call = fmt.Sprintf("acquire(%s)", in.Worker)
	case history.Complete:
		call = fmt.Sprintf("complete(%s, tok=%d)", in.Worker, in.Token)
	case history.FailJob:
		call = fmt.Sprintf("fail(%s, tok=%d, retryable=%v)", in.Worker, in.Token, in.Retryable)
	case history.Get:
		call = "get()"
	default:
		call = in.Method
	}
	switch {
	case out.Unknown:
		return call + " -> ?"
	case out.Code != "":
		return call + " -> " + out.Code
	}
	switch in.Method {
	case history.Submit:
		return fmt.Sprintf("%s -> dup=%v", call, out.Dup)
	case history.Complete:
		return fmt.Sprintf("%s -> ok already=%v", call, out.Already)
	case history.FailJob:
		return fmt.Sprintf("%s -> %s", call, out.Status)
	}
	return fmt.Sprintf("%s -> %s tok=%d att=%d owner=%s", call, out.Status, out.Token, out.Attempts, out.Owner)
}
