// Command relay-chaos is a seeded, reproducible chaos harness for Relay.
//
//	relay-chaos run --seed N --duration 5m --faults all --backend raft
//
// run brings up the chaos compose cluster, starts the workload, injects
// faults on a schedule derived only from the seed (docker kill/pause,
// network partitions, Toxiproxy latency, coordinator restarts), heals
// everything, waits for the cluster to quiesce, and checks the recorded
// history with internal/check. It exits non-zero on any violation and
// saves the history plus a Porcupine visualization.
//
// load is the workload itself; run starts it inside the cluster's network
// so its history shares the workers' clock. check re-checks a saved run.
// summarize folds a sweep's run directories into one results file.
package main

import (
	"fmt"
	"os"
	"strings"
)

func main() {
	args := os.Args[1:]
	cmd := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	var err error
	switch cmd {
	case "run":
		err = runMain(args)
	case "load":
		err = loadMain(args)
	case "check":
		err = checkMain(args)
	case "summarize":
		err = summarizeMain(args)
	default:
		err = fmt.Errorf("unknown command %q (want run, load, check or summarize)", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "relay-chaos:", err)
		os.Exit(1)
	}
}
