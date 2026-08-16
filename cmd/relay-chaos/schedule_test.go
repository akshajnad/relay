package main

import (
	"reflect"
	"testing"
	"time"
)

func TestScheduleIsReproducible(t *testing.T) {
	topo := Topology{Workers: 5, Coordinators: 3, Raft: true}
	kinds, err := parseKinds("all", topo)
	if err != nil {
		t.Fatal(err)
	}
	a := Schedule(42, 5*time.Minute, kinds, topo)
	b := Schedule(42, 5*time.Minute, kinds, topo)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed produced different schedules")
	}
	if c := Schedule(43, 5*time.Minute, kinds, topo); reflect.DeepEqual(a, c) {
		t.Fatal("different seeds produced the same schedule")
	}
	if len(a) < 30 {
		t.Fatalf("only %d faults in 5 minutes", len(a))
	}
}

func TestScheduleRespectsBoundsAndExclusivity(t *testing.T) {
	topo := Topology{Workers: 3, Coordinators: 3, Raft: true}
	kinds, _ := parseKinds("all", topo)
	for seed := uint64(1); seed <= 50; seed++ {
		const runFor = 2 * time.Minute
		sched := Schedule(seed, runFor, kinds, topo)
		busy := map[string]time.Duration{}
		for _, f := range sched {
			if f.At < 5*time.Second || f.At+f.Duration > runFor-5*time.Second {
				t.Fatalf("seed %d: fault outside the active window: %v", seed, f)
			}
			for _, k := range claims(f, topo) {
				if until, ok := busy[k]; ok && until > f.At && k != "coordinators-any" {
					t.Fatalf("seed %d: %v overlaps an earlier fault on %s", seed, f, k)
				}
			}
			for _, k := range claims(f, topo) {
				busy[k] = max(busy[k], f.At+f.Duration)
			}
		}
	}
}

func TestParseKinds(t *testing.T) {
	redis := Topology{Workers: 3, Coordinators: 1}
	if _, err := parseKinds("kill-leader", redis); err == nil {
		t.Fatal("leader faults accepted without raft")
	}
	ks, err := parseKinds("pause-worker,latency", redis)
	if err != nil || len(ks) != 2 {
		t.Fatalf("parse = %v, %v", ks, err)
	}
}
