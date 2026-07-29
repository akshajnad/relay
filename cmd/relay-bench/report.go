package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/akshajnad/relay/internal/metrics"
)

type Report struct {
	Timestamp time.Time  `json:"timestamp"`
	Config    Config     `json:"config"`
	Host      HostInfo   `json:"host"`
	Docker    DockerInfo `json:"docker"`
	Runs      []Run      `json:"runs"`
	Target    Target     `json:"target"`
}

type Target struct {
	Description string          `json:"description"`
	P95Ms       float64         `json:"p95_ms"`
	Met         map[string]bool `json:"met"`
}

func (t *Target) evaluate(runs []Run) {
	t.Met = map[string]bool{}
	for _, r := range runs {
		t.Met[r.Mode] = r.SchedulingLatency.Count > 0 && r.SchedulingLatency.P95Ms < t.P95Ms
	}
}

type HostInfo struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	CPU       string `json:"cpu"`
	Cores     int    `json:"cores"`
	MemoryGB  string `json:"memory_gb"`
	GoVersion string `json:"go_version"`
}

type DockerInfo struct {
	ServerVersion   string  `json:"server_version"`
	OperatingSystem string  `json:"operating_system"`
	KernelVersion   string  `json:"kernel_version"`
	CPUs            int     `json:"cpus"`
	MemoryGB        float64 `json:"memory_gb"`
}

func hostInfo() HostInfo {
	h := HostInfo{OS: runtime.GOOS, Arch: runtime.GOARCH, Cores: runtime.NumCPU(), GoVersion: runtime.Version()}
	switch runtime.GOOS {
	case "darwin":
		h.CPU = cmdOut("sysctl", "-n", "machdep.cpu.brand_string")
		if b, err := strconv.ParseFloat(cmdOut("sysctl", "-n", "hw.memsize"), 64); err == nil {
			h.MemoryGB = fmt.Sprintf("%.0f", b/(1<<30))
		}
		if v := cmdOut("sw_vers", "-productVersion"); v != "" {
			h.OS = "macOS " + v
		}
	case "linux":
		h.CPU = procField("/proc/cpuinfo", "model name")
		if kb, err := strconv.ParseFloat(strings.Fields(procField("/proc/meminfo", "MemTotal") + " 0")[0], 64); err == nil {
			h.MemoryGB = fmt.Sprintf("%.0f", kb/(1<<20))
		}
	}
	return h
}

func dockerInfo() DockerInfo {
	var raw struct {
		ServerVersion   string
		OperatingSystem string
		KernelVersion   string
		NCPU            int
		MemTotal        float64
	}
	var d DockerInfo
	if err := json.Unmarshal([]byte(cmdOut("docker", "info", "--format", "{{json .}}")), &raw); err != nil {
		return d
	}
	return DockerInfo{
		ServerVersion:   raw.ServerVersion,
		OperatingSystem: raw.OperatingSystem,
		KernelVersion:   raw.KernelVersion,
		CPUs:            raw.NCPU,
		MemoryGB:        float64(int(raw.MemTotal/(1<<30)*10)) / 10,
	}
}

func cmdOut(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func procField(path, key string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), ":"); ok && strings.TrimSpace(k) == key {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func (r *Report) write(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, r.Timestamp.Format("20060102T150405Z")+".json")
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	return path, os.WriteFile(path, append(b, '\n'), 0o644)
}

func latencyRow(w *tabwriter.Writer, name string, s metrics.Summary) {
	fmt.Fprintf(w, "  %s\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%d\t\n", name, s.P50Ms, s.P95Ms, s.P99Ms, s.MaxMs, s.MeanMs, s.Count)
}

func printRun(r *Run) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(w, "  (ms)\tp50\tp95\tp99\tmax\tmean\tcount\t")
	latencyRow(w, "scheduling latency", r.SchedulingLatency)
	latencyRow(w, "queue wait", r.QueueWait)
	latencyRow(w, "acquire script RTT", r.AcquireScript)
	w.Flush()
	fmt.Printf("  throughput %.0f jobs/s (makespan %.2fs), %d leased, %d succeeded, %d requeued, %d stale\n",
		r.ThroughputPerSec, r.MakespanSeconds, r.Counters.Leased, r.Counters.Succeeded,
		r.Counters.Requeued, r.Counters.StaleRejections)
	if r.Profile != "" {
		fmt.Printf("  cpu profile: %s\n", r.Profile)
	}
}

func printSummary(r *Report) {
	c := r.Config
	fmt.Println("\n=== configuration ===")
	fmt.Printf("  host: %s, %s %s, %d cores, %s GB RAM, %s\n", r.Host.CPU, r.Host.OS, r.Host.Arch, r.Host.Cores, r.Host.MemoryGB, r.Host.GoVersion)
	fmt.Printf("  docker: %s on %s, %d CPUs, %.1f GB\n", r.Docker.ServerVersion, r.Docker.OperatingSystem, r.Docker.CPUs, r.Docker.MemoryGB)
	fmt.Printf("  cluster: %d worker containers x %d slots = %d slots; %d jobs of sleep %v; warmup %d\n",
		c.Workers, c.Concurrency, c.Workers*c.Concurrency, c.Jobs, c.Sleep, c.Warmup)
	fmt.Println("\n=== target: " + r.Target.Description + " ===")
	for _, run := range r.Runs {
		verdict := "MISSED"
		if r.Target.Met[run.Mode] {
			verdict = "MET"
		}
		fmt.Printf("  %-8s p95 %.2f ms  -> %s\n", run.Mode, run.SchedulingLatency.P95Ms, verdict)
	}
}
