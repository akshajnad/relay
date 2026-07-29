// Command relayctl is the Relay CLI: submit jobs, inspect them, and list
// workers.
//
//	relayctl [--coordinator addr] submit --type sleep --payload '{"duration_ms":100}' [--idempotency-key k] [--max-attempts n] [--wait]
//	relayctl get [--json] <job-id>
//	relayctl workers
//	relayctl health
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/envflag"
	"github.com/akshajnad/relay/internal/rpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/encoding/protojson"
)

const usage = `usage: relayctl [--coordinator addr] <command> [flags]

commands:
  submit    submit a job
  get       show a job and its transition history
  workers   list registered workers
  health    exit 0 if the coordinator reports SERVING
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "relayctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("relayctl", flag.ExitOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage); fs.PrintDefaults() }
	addr := fs.String("coordinator", "localhost:50051", "coordinator gRPC address")
	timeout := fs.Duration("timeout", 10*time.Second, "per-request timeout")
	if err := envflag.Parse(fs, "RELAY", args); err != nil {
		return err
	}
	if fs.NArg() == 0 {
		fs.Usage()
		os.Exit(2)
	}
	conn, err := rpc.Dial(*addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	c := client{
		jobs:    relayv1.NewJobServiceClient(conn),
		workers: relayv1.NewWorkerServiceClient(conn),
		timeout: *timeout,
	}

	cmd, rest := fs.Arg(0), fs.Args()[1:]
	switch cmd {
	case "submit":
		return c.submit(rest)
	case "get":
		return c.get(rest)
	case "workers":
		return c.listWorkers()
	case "health":
		ctx, cancel := c.ctx()
		defer cancel()
		resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if resp.Status != healthpb.HealthCheckResponse_SERVING {
			return fmt.Errorf("coordinator is %v", resp.Status)
		}
		return nil
	}
	fs.Usage()
	return fmt.Errorf("unknown command %q", cmd)
}

type client struct {
	jobs    relayv1.JobServiceClient
	workers relayv1.WorkerServiceClient
	timeout time.Duration
}

func (c client) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), c.timeout)
}

func (c client) submit(args []string) error {
	fs := flag.NewFlagSet("submit", flag.ExitOnError)
	typ := fs.String("type", "noop", "job type (noop, sleep, fail-n-times)")
	payload := fs.String("payload", "", "job payload (JSON for the built-in handlers)")
	key := fs.String("idempotency-key", "", "deduplication key")
	maxAttempts := fs.Int("max-attempts", 0, "attempts before the job is dead-lettered (0 = server default)")
	wait := fs.Bool("wait", false, "wait for the job to reach a terminal state, then print it")
	fs.Parse(args)

	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.jobs.SubmitJob(ctx, &relayv1.SubmitJobRequest{
		Type: *typ, Payload: []byte(*payload), IdempotencyKey: *key, MaxAttempts: int32(*maxAttempts),
	})
	if err != nil {
		return err
	}
	if resp.Duplicate {
		fmt.Printf("%s (duplicate: idempotency key already used)\n", resp.JobId)
	} else {
		fmt.Println(resp.JobId)
	}
	if !*wait {
		return nil
	}
	for {
		ctx, cancel := c.ctx()
		got, err := c.jobs.GetJob(ctx, &relayv1.GetJobRequest{JobId: resp.JobId})
		cancel()
		if err != nil {
			return err
		}
		switch got.Job.Status {
		case relayv1.JobStatus_JOB_STATUS_SUCCEEDED, relayv1.JobStatus_JOB_STATUS_FAILED, relayv1.JobStatus_JOB_STATUS_DEAD:
			printJob(got)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c client) get(args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "print the raw response as JSON")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: relayctl get [--json] <job-id>")
	}
	ctx, cancel := c.ctx()
	defer cancel()
	got, err := c.jobs.GetJob(ctx, &relayv1.GetJobRequest{JobId: fs.Arg(0)})
	if err != nil {
		return err
	}
	if *asJSON {
		fmt.Println(protojson.MarshalOptions{Multiline: true}.Format(got))
		return nil
	}
	printJob(got)
	return nil
}

func printJob(got *relayv1.GetJobResponse) {
	j := got.Job
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	row := func(k string, v any) { fmt.Fprintf(w, "%s\t%v\n", k, v) }
	row("id", j.Id)
	row("type", j.Type)
	row("status", strings.TrimPrefix(j.Status.String(), "JOB_STATUS_"))
	row("attempts", fmt.Sprintf("%d / %d", j.Attempts, j.MaxAttempts))
	if j.IdempotencyKey != "" {
		row("idempotency_key", j.IdempotencyKey)
	}
	row("payload", string(j.Payload))
	row("lease_owner", j.LeaseOwner)
	row("lease_token", j.LeaseToken)
	row("created", ts(j.CreatedAtMs))
	row("finished", ts(j.FinishedAtMs))
	if j.LastError != "" {
		row("last_error", j.LastError)
	}
	if len(j.Result) > 0 {
		row("result", string(j.Result))
	}
	w.Flush()

	fmt.Println("\nhistory:")
	w = tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, e := range got.History {
		fmt.Fprintf(w, "  %s\t%s\t%s\ttoken=%d\t%s\n", ts(e.AtMs), e.Kind, e.WorkerId, e.LeaseToken, e.Detail)
	}
	w.Flush()
}

func (c client) listWorkers() error {
	ctx, cancel := c.ctx()
	defer cancel()
	resp, err := c.workers.ListWorkers(ctx, &relayv1.ListWorkersRequest{})
	if err != nil {
		return err
	}
	ws := resp.Workers
	sort.Slice(ws, func(i, k int) bool {
		if ws[i].State != ws[k].State {
			return ws[i].State < ws[k].State
		}
		return ws[i].Id < ws[k].Id
	})
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tSTATE\tHOSTNAME\tCONCURRENCY\tACTIVE\tLAST HEARTBEAT")
	for _, x := range ws {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\t%s\n", x.Id, x.State, x.Hostname, x.Concurrency, x.ActiveJobs, ts(x.LastHeartbeatMs))
	}
	return w.Flush()
}

func ts(ms int64) string {
	if ms == 0 {
		return "-"
	}
	return time.UnixMilli(ms).Format("15:04:05.000")
}
