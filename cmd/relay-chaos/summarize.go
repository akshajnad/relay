package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/akshajnad/relay/internal/buildrev"
	"github.com/akshajnad/relay/internal/check"
	"github.com/akshajnad/relay/internal/history"
)

// SweepSummary is the committed record of a chaos sweep
// (bench/results/chaos-sweep-<backend>.json).
type SweepSummary struct {
	Backend          string         `json:"backend"`
	DurationPerSeed  string         `json:"duration_per_seed"`
	Image            buildrev.Image `json:"image"`
	Passed           int            `json:"passed"`
	Seeds            []SeedSummary  `json:"seeds"`
	Totals           SeedSummary    `json:"totals"`
	FaultKinds       map[string]int `json:"fault_kinds"`
	SummarizedAt     time.Time      `json:"summarized_at"`
	RunDirs          []string       `json:"run_dirs"`
	ElectionsCounted string         `json:"elections_counted"`
}

type SeedSummary struct {
	Seed          uint64 `json:"seed,omitempty"`
	Pass          *bool  `json:"pass,omitempty"`
	Jobs          int    `json:"jobs"`
	Ops           int    `json:"ops"`
	Indeterminate int    `json:"indeterminate"`
	Faults        int    `json:"faults"`
	Elections     int    `json:"elections"`
	StaleRejected int    `json:"stale_rejected"`
	Redirects     int    `json:"redirects"`
	FencingChecks int    `json:"fencing_checks"`
	Violations    int    `json:"violations"`
}

func (s *SeedSummary) add(o SeedSummary) {
	s.Jobs += o.Jobs
	s.Ops += o.Ops
	s.Indeterminate += o.Indeterminate
	s.Faults += o.Faults
	s.Elections += o.Elections
	s.StaleRejected += o.StaleRejected
	s.Redirects += o.Redirects
	s.FencingChecks += o.FencingChecks
	s.Violations += o.Violations
}

// leaderLogLine is what a coordinator logs once per won election
// (including the first one after every restart).
var leaderLogLine = []byte(`"msg":"this node is now the leader"`)

// summarizeMain folds the artifact directories of a sweep's runs into one
// SweepSummary. Every number comes from the saved artifacts: report.json
// (checker counts), schedule.json (faults), cluster.log (elections) and the
// histories (fenced-out reports and NOT_LEADER redirects).
func summarizeMain(args []string) error {
	fs := flag.NewFlagSet("summarize", flag.ExitOnError)
	out := fs.String("out", "", "write the summary here (default stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	dirs := fs.Args()
	if len(dirs) == 0 {
		return errors.New("usage: relay-chaos summarize [--out file] <run dir>...")
	}
	sum := SweepSummary{FaultKinds: map[string]int{}, SummarizedAt: time.Now().UTC(),
		ElectionsCounted: "coordinator log lines \"this node is now the leader\", including the first election after each start"}
	for _, dir := range dirs {
		s, cfg, kinds, err := summarizeRun(dir)
		if err != nil {
			return fmt.Errorf("%s: %w", dir, err)
		}
		if sum.Backend == "" {
			sum.Backend, sum.DurationPerSeed, sum.Image = cfg.Backend, cfg.Duration.String(), cfg.ImageBuild
			if sum.Image.Ref == "" {
				sum.Image = buildrev.Image{Ref: cfg.Image, Revision: "unknown"}
			}
		} else if cfg.Backend != sum.Backend || cfg.Duration.String() != sum.DurationPerSeed || cfg.ImageBuild.ID != sum.Image.ID {
			return fmt.Errorf("%s: backend, duration or image differs from the other runs", dir)
		}
		if *s.Pass {
			sum.Passed++
		}
		sum.Seeds = append(sum.Seeds, s)
		sum.Totals.add(s)
		for k, n := range kinds {
			sum.FaultKinds[k] += n
		}
		sum.RunDirs = append(sum.RunDirs, filepath.Base(dir))
	}
	sort.Slice(sum.Seeds, func(i, j int) bool { return sum.Seeds[i].Seed < sum.Seeds[j].Seed })
	sort.Strings(sum.RunDirs)

	b, _ := json.MarshalIndent(sum, "", "  ")
	b = append(b, '\n')
	fmt.Fprintf(os.Stderr, "%s: %d/%d seeds pass, %d jobs, %d faults, %d violations\n",
		sum.Backend, sum.Passed, len(sum.Seeds), sum.Totals.Jobs, sum.Totals.Faults, sum.Totals.Violations)
	if *out == "" {
		_, err := os.Stdout.Write(b)
		return err
	}
	return os.WriteFile(*out, b, 0o644)
}

func summarizeRun(dir string) (SeedSummary, runConfig, map[string]int, error) {
	var s SeedSummary
	var cfg runConfig
	if err := readJSON(filepath.Join(dir, "config.json"), &cfg); err != nil {
		return s, cfg, nil, err
	}
	var rep check.Report
	if err := readJSON(filepath.Join(dir, "report.json"), &rep); err != nil {
		return s, cfg, nil, err
	}
	var sched []struct {
		Kind string `json:"kind"`
	}
	if err := readJSON(filepath.Join(dir, "schedule.json"), &sched); err != nil {
		return s, cfg, nil, err
	}
	pass := rep.OK()
	s = SeedSummary{Seed: cfg.Seed, Pass: &pass, Jobs: rep.Jobs, Ops: rep.Ops, Indeterminate: rep.Indeterminate,
		Faults: len(sched), FencingChecks: rep.Result(check.Fencing).Checked}
	for _, res := range rep.Results {
		s.Violations += res.Violations
	}
	kinds := map[string]int{}
	for _, f := range sched {
		kinds[f.Kind]++
	}

	if f, err := os.Open(filepath.Join(dir, "cluster.log")); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			if bytes.Contains(sc.Bytes(), leaderLogLine) {
				s.Elections++
			}
		}
		f.Close()
	}

	h, err := history.ReadDir(filepath.Join(dir, "history"))
	if err != nil {
		return s, cfg, nil, err
	}
	for _, op := range h.Ops {
		switch {
		case op.Code() == "Aborted":
			s.StaleRejected++
		case op.Code() == "Unavailable" && op.Done.Err == "not the leader":
			s.Redirects++
		}
	}
	return s, cfg, kinds, nil
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
