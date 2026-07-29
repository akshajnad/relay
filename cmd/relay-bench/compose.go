package main

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// compose drives the benchmark's docker compose project.
type compose struct {
	cfg Config
	env []string
}

func newCompose(cfg Config) *compose {
	grpcPort := portOf(cfg.Coordinator)
	metricsPort := portOf(cfg.MetricsAddr)
	env := append(os.Environ(),
		"RELAY_GRPC_PUBLISH=127.0.0.1:"+grpcPort+":50051",
		"RELAY_METRICS_PUBLISH=127.0.0.1:"+metricsPort+":9090",
		// The benchmark talks to Redis only through the coordinator.
		"RELAY_REDIS_PUBLISH=127.0.0.1::6379",
		"RELAY_WORKER_CONCURRENCY="+strconv.Itoa(cfg.Concurrency),
		"RELAY_SLEEP_DURATION="+cfg.Sleep.String(),
	)
	return &compose{cfg: cfg, env: env}
}

func portOf(addr string) string {
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		return addr[i+1:]
	}
	return addr
}

func (c *compose) run(args ...string) error {
	full := append([]string{"compose", "-p", c.cfg.Project, "-f", c.cfg.ComposeFile}, args...)
	cmd := exec.Command("docker", full...)
	cmd.Env = c.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %s: %w\n%s", strings.Join(full, " "), err, out)
	}
	return nil
}

func (c *compose) up(workers int) error {
	_ = c.run("down", "-v", "--remove-orphans")
	args := []string{"up", "-d", "--wait", "--scale", fmt.Sprintf("worker=%d", workers)}
	if c.cfg.Build {
		args = append(args, "--build")
	} else {
		args = append(args, "--no-build")
	}
	return c.run(args...)
}

func (c *compose) scale(workers int) error {
	args := []string{"up", "-d", "--no-build", "--no-recreate", "--scale", fmt.Sprintf("worker=%d", workers)}
	if workers > 0 {
		args = append(args, "--wait")
	}
	return c.run(args...)
}

func (c *compose) down() error {
	return c.run("down", "-v", "--remove-orphans", "--timeout", "5")
}
