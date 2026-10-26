package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// cluster drives one compose project.
type cluster struct {
	project string
	file    string
	env     []string
	profile string
}

func (c *cluster) composeArgs(args ...string) []string {
	full := []string{"compose", "-p", c.project, "-f", c.file}
	if c.profile != "" {
		full = append(full, "--profile", c.profile)
	}
	return append(full, args...)
}

func (c *cluster) compose(args ...string) (string, error) {
	return c.run("docker", c.composeArgs(args...)...)
}

func (c *cluster) run(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Env = c.env
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	if err != nil {
		return out.String(), fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, out.String())
	}
	return out.String(), nil
}

func (c *cluster) docker(args ...string) error {
	_, err := c.run("docker", args...)
	return err
}

// containers returns the container ids of a service, sorted by container
// name so an index from the schedule maps to the same instance every run.
func (c *cluster) containers(service string) ([]string, error) {
	out, err := c.run("docker", "ps", "-a",
		"--filter", "label=com.docker.compose.project="+c.project,
		"--filter", "label=com.docker.compose.service="+service,
		"--format", "{{.Names}} {{.ID}}")
	if err != nil {
		return nil, err
	}
	type pair struct{ name, id string }
	var ps []pair
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Fields(line)
		if len(f) == 2 {
			ps = append(ps, pair{f[0], f[1]})
		}
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].name < ps[j].name })
	ids := make([]string, len(ps))
	for i, p := range ps {
		ids[i] = p.id
	}
	return ids, nil
}

// hostPort returns the 127.0.0.1 address a service's container port is
// published on.
func (c *cluster) hostPort(service string, port int) (string, error) {
	out, err := c.compose("port", service, fmt.Sprint(port))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

func (c *cluster) network(name string) string { return c.project + "_" + name }

// Fault primitives. Healing operations treat "already in that state" as
// success, so healing everything at the end is always safe.

func (c *cluster) kill(id string) error  { return c.docker("kill", id) }
func (c *cluster) start(id string) error { return c.docker("start", id) }
func (c *cluster) pause(id string) error { return c.docker("pause", id) }
func (c *cluster) restart(id string) error {
	return c.docker("restart", "--time", "5", id)
}

func (c *cluster) unpause(id string) error {
	err := c.docker("unpause", id)
	if err != nil && strings.Contains(err.Error(), "is not paused") {
		return nil
	}
	return err
}

func (c *cluster) disconnect(id, network string) error {
	return c.docker("network", "disconnect", "--force", c.network(network), id)
}

// state returns a container's state ("running", "paused", "exited"...).
func (c *cluster) state(id string) string {
	out, err := c.run("docker", "inspect", "-f", "{{.State.Status}}", id)
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(out)
}

// networks returns the compose networks (short names) a container is on.
func (c *cluster) networks(id string) map[string]bool {
	out, err := c.run("docker", "inspect", "-f", "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}", id)
	got := map[string]bool{}
	if err != nil {
		return got
	}
	for _, n := range strings.Fields(out) {
		got[strings.TrimPrefix(n, c.project+"_")] = true
	}
	return got
}

// toxiproxy is a minimal client for Toxiproxy's HTTP API.
type toxiproxy struct {
	base string
	http *http.Client
}

func newToxiproxy(addr string) *toxiproxy {
	return &toxiproxy{base: "http://" + addr, http: &http.Client{Timeout: 5 * time.Second}}
}

func (t *toxiproxy) do(method, path string, body any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, t.base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("toxiproxy %s %s: %s: %s", method, path, resp.Status, msg)
	}
	return nil
}

func (t *toxiproxy) proxies() ([]string, error) {
	resp, err := t.http.Get(t.base + "/proxies")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var m map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	var names []string
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	return names, nil
}

// addLatency adds latency with jitter in both directions of every proxy.
func (t *toxiproxy) addLatency(latencyMs, jitterMs int) error {
	names, err := t.proxies()
	if err != nil {
		return err
	}
	for _, p := range names {
		for _, stream := range []string{"upstream", "downstream"} {
			err := t.do("POST", "/proxies/"+p+"/toxics", map[string]any{
				"name": "latency_" + stream, "type": "latency", "stream": stream, "toxicity": 1.0,
				"attributes": map[string]int{"latency": latencyMs, "jitter": jitterMs},
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// clearLatency removes the latency toxics; missing toxics are fine.
func (t *toxiproxy) clearLatency() error {
	names, err := t.proxies()
	if err != nil {
		return err
	}
	for _, p := range names {
		for _, stream := range []string{"upstream", "downstream"} {
			if err := t.do("DELETE", "/proxies/"+p+"/toxics/latency_"+stream, nil); err != nil &&
				!strings.Contains(err.Error(), "404") {
				return err
			}
		}
	}
	return nil
}

func absPath(p string) string {
	if strings.HasPrefix(p, "/") {
		return p
	}
	wd, _ := os.Getwd()
	return wd + "/" + p
}
