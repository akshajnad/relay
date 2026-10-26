//go:build integration

// Package chaos runs relay-chaos against real Docker clusters. Run with
// `make integration`.
package chaos

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/akshajnad/relay/internal/check"
)

var repoRoot = func() string {
	_, here, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(here), "..", "..")
}()

func run(t *testing.T, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	t.Logf("%s %v:\n%s", name, args, out)
	if err != nil {
		t.Fatalf("%s failed: %v", name, err)
	}
}

// readReport loads the report of the single run under out.
func readReport(t *testing.T, out string) check.Report {
	t.Helper()
	runs, _ := filepath.Glob(filepath.Join(out, "*", "report.json"))
	if len(runs) != 1 {
		t.Fatalf("expected one run report under %s, found %v", out, runs)
	}
	b, err := os.ReadFile(runs[0])
	if err != nil {
		t.Fatal(err)
	}
	var rep check.Report
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatal(err)
	}
	return rep
}

// TestCheckerCatchesDisabledFencing builds Relay with the relay_nofence tag,
// which accepts lease reports without checking the fencing token, and
// proves the chaos checker flags the resulting violations on both
// backends (the Lua scripts and the Raft FSM). Pauses and partitions make
// workers zombies; long sleep jobs at a light load make their late reports
// land while the job is leased to someone else.
func TestCheckerCatchesDisabledFencing(t *testing.T) {
	run(t, "docker", "build", "-q", "-f", "deploy/Dockerfile", "--build-arg", "GO_TAGS=relay_nofence", "-t", "relay:nofence", ".")
	for _, backend := range []string{"redis", "raft"} {
		t.Run(backend, func(t *testing.T) {
			out := t.TempDir()
			run(t, "go", "run", "./cmd/relay-chaos", "run",
				"--backend", backend, "--seed", "7", "--duration", "60s",
				"--faults", "pause-worker,partition-worker",
				"--image", "relay:nofence",
				"--rate", "3", "--clients", "3", "--sleep-min-ms", "3000", "--sleep-max-ms", "6000",
				"--quiesce-timeout", "90s",
				"--out", out,
				"--expect-fail")
			rep := readReport(t, out)
			fencing := rep.Result(check.Fencing)
			if fencing.OK() {
				t.Fatalf("fencing invariant not flagged: %+v", rep.Results)
			}
			if len(rep.NonLinearizable) == 0 {
				t.Fatal("no job was flagged as non-linearizable")
			}
			vis, _ := filepath.Glob(filepath.Join(out, "*", "linearizability-*.html"))
			if len(vis) == 0 {
				t.Fatal("no porcupine visualization was written")
			}
			t.Logf("flagged %d fencing violations and %d non-linearizable jobs", fencing.Violations, len(rep.NonLinearizable))
		})
	}
}

// TestChaosShortRun is a quick clean run of every fault kind on each
// backend: the real build must pass.
func TestChaosShortRun(t *testing.T) {
	for _, backend := range []string{"redis", "raft"} {
		t.Run(backend, func(t *testing.T) {
			out := t.TempDir()
			run(t, "go", "run", "./cmd/relay-chaos", "run",
				"--backend", backend, "--seed", "3", "--duration", "60s", "--out", out)
			if rep := readReport(t, out); !rep.OK() {
				t.Fatalf("clean build failed the checker: %+v", rep.Results)
			}
		})
	}
}
