package main

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"time"
)

// Fault kinds. A fault targets one instance of a role and is healed after
// its duration (except instantaneous ones like restarts).
const (
	KillWorker      = "kill-worker"      // docker kill, docker start after the duration
	PauseWorker     = "pause-worker"     // docker pause / unpause
	PartitionWorker = "partition-worker" // disconnect from clientnet
	KillCoord       = "kill-coordinator"
	PauseCoord      = "pause-coordinator"
	RestartCoord    = "restart-coordinator"   // docker restart: SIGTERM, then start
	IsolateCoord    = "isolate-coordinator"   // disconnect from clientnet: clients cannot reach it
	PartitionCoord  = "partition-coordinator" // disconnect from backnet: cut from its store / Raft peers
	Latency         = "latency"               // toxiproxy latency + jitter on worker->coordinator traffic
	// Raft only. The target is whichever node leads when the fault fires.
	KillLeader      = "kill-leader"
	PauseLeader     = "pause-leader"
	PartitionLeader = "partition-leader" // the old leader stays reachable by clients: a minority-side leader
	RestartCluster  = "restart-cluster"  // kill every coordinator, then start them all
)

var (
	baseKinds = []string{KillWorker, PauseWorker, PartitionWorker, KillCoord, PauseCoord, RestartCoord,
		IsolateCoord, PartitionCoord, Latency}
	raftKinds = []string{KillLeader, PauseLeader, PartitionLeader, RestartCluster}
)

// Fault is one scheduled fault.
type Fault struct {
	At       time.Duration `json:"at"`
	Duration time.Duration `json:"duration"`
	Kind     string        `json:"kind"`
	// Target is the role ("worker", "coordinator", "leader", "cluster",
	// "network") and Index picks an instance among that role's containers,
	// sorted by name.
	Target    string `json:"target"`
	Index     int    `json:"index"`
	LatencyMs int    `json:"latency_ms,omitempty"`
	JitterMs  int    `json:"jitter_ms,omitempty"`
}

func (f Fault) String() string {
	s := fmt.Sprintf("t+%-7s %-22s %s", f.At.Round(100*time.Millisecond), f.Kind, f.target())
	if f.Duration > 0 {
		s += fmt.Sprintf(" for %s", f.Duration.Round(100*time.Millisecond))
	}
	if f.Kind == Latency {
		s += fmt.Sprintf(" (%dms ± %dms)", f.LatencyMs, f.JitterMs)
	}
	return s
}

func (f Fault) target() string {
	switch f.Target {
	case "leader", "cluster", "network":
		return f.Target
	}
	return fmt.Sprintf("%s[%d]", f.Target, f.Index)
}

// Topology is what the schedule may target.
type Topology struct {
	Workers      int
	Coordinators int
	Raft         bool
}

// parseKinds expands --faults: "all", or a comma-separated list of kinds.
func parseKinds(spec string, topo Topology) ([]string, error) {
	avail := append([]string{}, baseKinds...)
	if topo.Raft {
		avail = append(avail, raftKinds...)
	}
	if spec == "" || spec == "all" {
		return avail, nil
	}
	var kinds []string
	for _, k := range strings.Split(spec, ",") {
		k = strings.TrimSpace(k)
		if !slices.Contains(avail, k) {
			return nil, fmt.Errorf("unknown or unavailable fault %q (available: %s)", k, strings.Join(avail, ", "))
		}
		kinds = append(kinds, k)
	}
	return kinds, nil
}

// Schedule derives the full fault schedule from the seed. It depends on
// nothing but its arguments, so a seed always reproduces its schedule.
//
// Faults start after a warm-up and stop before the run ends. Faults never
// overlap on the same target (a container cannot be paused and killed at
// once), and coordinator-wide faults (leader, cluster) do not overlap with
// each other or with per-coordinator faults.
func Schedule(seed uint64, runFor time.Duration, kinds []string, topo Topology) []Fault {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	between := func(lo, hi time.Duration) time.Duration {
		return lo + time.Duration(rng.Int64N(int64(hi-lo)+1))
	}
	const warmup = 5 * time.Second
	end := runFor - 5*time.Second
	busy := map[string]time.Duration{} // target -> busy until
	var out []Fault
	for at := warmup + between(0, 2*time.Second); at < end; at += between(1500*time.Millisecond, 6*time.Second) {
		kind := kinds[rng.IntN(len(kinds))]
		f := Fault{At: at, Kind: kind}
		switch kind {
		case KillWorker, PauseWorker, PartitionWorker:
			f.Target, f.Index, f.Duration = "worker", rng.IntN(topo.Workers), between(time.Second, 10*time.Second)
		case KillCoord, PauseCoord, IsolateCoord, PartitionCoord:
			f.Target, f.Index, f.Duration = "coordinator", rng.IntN(topo.Coordinators), between(time.Second, 10*time.Second)
		case RestartCoord:
			f.Target, f.Index = "coordinator", rng.IntN(topo.Coordinators)
		case Latency:
			f.Target, f.Duration = "network", between(2*time.Second, 15*time.Second)
			f.LatencyMs, f.JitterMs = 20+rng.IntN(380), rng.IntN(200)
		case KillLeader, PauseLeader, PartitionLeader:
			f.Target, f.Duration = "leader", between(2*time.Second, 12*time.Second)
		case RestartCluster:
			f.Target, f.Duration = "cluster", between(time.Second, 5*time.Second)
		}
		if at+f.Duration > end {
			f.Duration = end - at
		}
		if clashes(f, busy) {
			continue
		}
		for _, k := range claims(f, topo) {
			busy[k] = max(busy[k], at+f.Duration+time.Second)
		}
		out = append(out, f)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].At < out[j].At })
	return out
}

// claims lists the busy-slots a fault occupies.
func claims(f Fault, topo Topology) []string {
	switch f.Target {
	case "worker":
		return []string{fmt.Sprintf("worker[%d]", f.Index)}
	case "coordinator":
		return []string{fmt.Sprintf("coordinator[%d]", f.Index), "coordinators-any"}
	case "leader", "cluster":
		// The leader could be any coordinator.
		keys := []string{"coordinators-all", "coordinators-any"}
		for i := 0; i < topo.Coordinators; i++ {
			keys = append(keys, fmt.Sprintf("coordinator[%d]", i))
		}
		return keys
	case "network":
		return []string{"network"}
	}
	return nil
}

func clashes(f Fault, busy map[string]time.Duration) bool {
	check := []string{}
	switch f.Target {
	case "worker":
		check = append(check, fmt.Sprintf("worker[%d]", f.Index))
	case "coordinator":
		check = append(check, fmt.Sprintf("coordinator[%d]", f.Index), "coordinators-all")
	case "leader", "cluster":
		check = append(check, "coordinators-any", "coordinators-all")
	case "network":
		check = append(check, "network")
	}
	for _, k := range check {
		if busy[k] > f.At {
			return true
		}
	}
	return false
}
