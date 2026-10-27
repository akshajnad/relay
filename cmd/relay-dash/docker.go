package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// docker runs the docker CLI. Every argument is passed as its own argv
// entry; container ids reaching it are first matched against the project's
// own containers.
type docker struct {
	dir string // compose files
}

func (d *docker) path(file string) string { return filepath.Join(d.dir, file) }

func (d *docker) command(ctx context.Context, env []string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Env = append(os.Environ(), env...)
	return cmd
}

func (d *docker) output(ctx context.Context, args ...string) (string, error) {
	var out bytes.Buffer
	cmd := d.command(ctx, nil, args...)
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(out.String()))
	}
	return out.String(), nil
}

// stream runs a command, passing each line of its combined output to line.
func (d *docker) stream(ctx context.Context, env []string, line func(string), args ...string) error {
	cmd := d.command(ctx, env, args...)
	pr, pw := io.Pipe()
	cmd.Stdout, cmd.Stderr = pw, pw
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc := bufio.NewScanner(pr)
		for sc.Scan() {
			line(sc.Text())
		}
		_, _ = io.Copy(io.Discard, pr)
	}()
	err := cmd.Run()
	_ = pw.Close()
	wg.Wait()
	if err != nil {
		return fmt.Errorf("docker %s: %w", strings.Join(args, " "), err)
	}
	return nil
}

func (d *docker) available(ctx context.Context) error {
	_, err := d.output(ctx, "version", "--format", "{{.Server.Version}}")
	return err
}

// composeArgs builds `docker compose -p <project> -f <file> [-f observability] args...`.
func (d *docker) composeArgs(l layout, observability bool, args ...string) []string {
	full := []string{"compose", "-p", l.Project, "-f", d.path(l.File)}
	if observability {
		full = append(full, "-f", d.path(observabilityFile))
	}
	return append(full, args...)
}

// container is one container of the watched compose project.
type container struct {
	ID      string `json:"id"` // short id; a worker reports it as its hostname
	Name    string `json:"name"`
	Service string `json:"service"`
	State   string `json:"state"` // running, paused, exited, ...
	// Isolated is true when the container is on no network: the dashboard
	// disconnected it to simulate a partition.
	Isolated bool `json:"isolated"`
	Number   int  `json:"-"`
}

// containers lists a project's containers, ordered by service and replica.
func (d *docker) containers(ctx context.Context, project string) ([]container, error) {
	out, err := d.output(ctx, "ps", "-a",
		"--filter", "label=com.docker.compose.project="+project,
		"--format", `{{.ID}}	{{.Names}}	{{.State}}	{{.Label "com.docker.compose.service"}}	{{.Label "com.docker.compose.container-number"}}	{{.Networks}}`)
	if err != nil {
		return nil, err
	}
	var cs []container
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		f := strings.Split(line, "\t")
		if len(f) != 6 {
			continue
		}
		n, _ := strconv.Atoi(f[4])
		cs = append(cs, container{ID: f[0], Name: f[1], State: f[2], Service: f[3], Number: n,
			Isolated: strings.TrimSpace(f[5]) == "" && f[2] != "exited" && f[2] != "created"})
	}
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].Service != cs[j].Service {
			return cs[i].Service < cs[j].Service
		}
		return cs[i].Number < cs[j].Number
	})
	return cs, nil
}

// env returns a container's environment as a map.
func (d *docker) env(ctx context.Context, id string) (map[string]string, error) {
	out, err := d.output(ctx, "inspect", "-f", "{{json .Config.Env}}", id)
	if err != nil {
		return nil, err
	}
	var kvs []string
	if err := json.Unmarshal([]byte(out), &kvs); err != nil {
		return nil, err
	}
	m := map[string]string{}
	for _, kv := range kvs {
		if k, v, ok := strings.Cut(kv, "="); ok {
			m[k] = v
		}
	}
	return m, nil
}

func (d *docker) imageExists(ctx context.Context, image string) bool {
	_, err := d.output(ctx, "image", "inspect", "--format", "{{.Id}}", image)
	return err == nil
}

// Fault primitives. network is the compose project's default network, the
// only one the Relay compose files use.

func defaultNetwork(project string) string { return project + "_default" }

func (d *docker) fault(ctx context.Context, action string, c container, project string) error {
	var args []string
	switch action {
	case "kill":
		args = []string{"kill", c.ID}
	case "start":
		args = []string{"start", c.ID}
	case "restart":
		args = []string{"restart", "--time", "5", c.ID}
	case "pause":
		args = []string{"pause", c.ID}
	case "resume":
		args = []string{"unpause", c.ID}
	case "isolate":
		args = []string{"network", "disconnect", "--force", defaultNetwork(project), c.ID}
	case "reconnect":
		// A plain `network connect` would drop the service's DNS alias,
		// which is how every other container finds this one.
		args = []string{"network", "connect", "--alias", c.Service, defaultNetwork(project), c.ID}
	default:
		return fmt.Errorf("unknown fault %q", action)
	}
	_, err := d.output(ctx, args...)
	if err != nil && (action == "resume" && strings.Contains(err.Error(), "is not paused") ||
		action == "reconnect" && strings.Contains(err.Error(), "already exists")) {
		return nil // already healed
	}
	return err
}
