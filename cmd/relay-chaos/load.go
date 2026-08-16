package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/check"
	"github.com/akshajnad/relay/internal/history"
	"github.com/akshajnad/relay/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// loadConfig is the workload's configuration. It runs inside the chaos
// cluster's Docker network so its history shares the workers' clock.
type loadConfig struct {
	Coordinators   string
	HistoryDir     string
	Out            string
	Seed           uint64
	Duration       time.Duration
	Clients        int
	Rate           float64
	DupFraction    float64
	SleepMinMs     int
	SleepMaxMs     int
	QuiesceTimeout time.Duration
	RPCTimeout     time.Duration
	// FreezeDir, if set, is where the load and the orchestrator hand off
	// before the final reads: the load creates "quiesced", the orchestrator
	// kills every worker and creates "frozen". After that no recorded
	// operation can follow the snapshot, so it really is final.
	FreezeDir string
}

func loadMain(args []string) error {
	var cfg loadConfig
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	fs.StringVar(&cfg.Coordinators, "coordinators", "coordinator:50051", "coordinator endpoints: host:port or id=host:port, comma-separated")
	fs.StringVar(&cfg.HistoryDir, "history-dir", "/history", "directory for the client history file")
	fs.StringVar(&cfg.Out, "out", "/out/final.json", "where to write the post-quiesce job states")
	fs.Uint64Var(&cfg.Seed, "seed", 1, "workload seed (job mix and duplicate keys)")
	fs.DurationVar(&cfg.Duration, "duration", time.Minute, "how long to submit")
	fs.IntVar(&cfg.Clients, "clients", 8, "concurrent submitters")
	fs.Float64Var(&cfg.Rate, "rate", 40, "total submits per second")
	fs.Float64Var(&cfg.DupFraction, "dup-fraction", 0.1, "fraction of submits that reuse an earlier idempotency key")
	fs.IntVar(&cfg.SleepMinMs, "sleep-min-ms", 20, "shortest sleep job")
	fs.IntVar(&cfg.SleepMaxMs, "sleep-max-ms", 1500, "longest sleep job")
	fs.DurationVar(&cfg.QuiesceTimeout, "quiesce-timeout", 3*time.Minute, "how long to wait for every job to finish after submitting stops")
	fs.DurationVar(&cfg.RPCTimeout, "rpc-timeout", 5*time.Second, "per-RPC timeout")
	fs.StringVar(&cfg.FreezeDir, "freeze-dir", "", "hand-off directory for freezing workers before the final reads")
	if err := fs.Parse(args); err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	rec, err := history.Open(cfg.HistoryDir, "client")
	if err != nil {
		return err
	}
	defer rec.Close()
	conn, closeConn, err := dialCoordinators(cfg.Coordinators, grpc.WithChainUnaryInterceptor(history.UnaryClientInterceptor(rec)))
	if err != nil {
		return err
	}
	defer closeConn()
	l := &load{cfg: cfg, jobs: relayv1.NewJobServiceClient(conn), log: log,
		rng: rand.New(rand.NewPCG(cfg.Seed, cfg.Seed+1)), byKey: map[string]submission{}}
	return l.run()
}

type submission struct {
	req *relayv1.SubmitJobRequest
}

type load struct {
	cfg  loadConfig
	jobs relayv1.JobServiceClient
	log  *slog.Logger

	mu    sync.Mutex
	rng   *rand.Rand
	seq   int
	keys  []string              // every key submitted so far
	byKey map[string]submission // key -> the request first sent with it
	acked []string              // distinct acknowledged job ids
	seen  map[string]bool

	submitted, acks, gaveUp atomic.Int64
}

// next picks the next request: usually a fresh job, sometimes an exact
// resubmission of an earlier key (which must map to the same job).
func (l *load) next() *relayv1.SubmitJobRequest {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.keys) > 0 && l.rng.Float64() < l.cfg.DupFraction {
		// Prefer recent keys, so duplicates often race the original.
		window := min(len(l.keys), 20)
		k := l.keys[len(l.keys)-1-l.rng.IntN(window)]
		return l.byKey[k].req
	}
	l.seq++
	key := fmt.Sprintf("s%d-k%d", l.cfg.Seed, l.seq)
	var req *relayv1.SubmitJobRequest
	if l.rng.Float64() < 0.6 {
		req = &relayv1.SubmitJobRequest{Type: "sleep",
			Payload:     []byte(fmt.Sprintf(`{"duration_ms":%d}`, l.cfg.SleepMinMs+l.rng.IntN(l.cfg.SleepMaxMs-l.cfg.SleepMinMs+1))),
			MaxAttempts: int32(2 + l.rng.IntN(4))}
	} else {
		req = &relayv1.SubmitJobRequest{Type: "fail-n-times",
			Payload:     []byte(fmt.Sprintf(`{"n":%d}`, l.rng.IntN(4))),
			MaxAttempts: int32(1 + l.rng.IntN(4))}
	}
	req.IdempotencyKey = key
	l.keys = append(l.keys, key)
	l.byKey[key] = submission{req: req}
	return req
}

func (l *load) run() error {
	start := time.Now()
	stopSubmitting := start.Add(l.cfg.Duration)
	// An unacknowledged submit keeps retrying a little past the end, then
	// gives up; it is not an acknowledged job, so the checker ignores it.
	giveUp := stopSubmitting.Add(30 * time.Second)
	l.seen = map[string]bool{}

	interval := time.Duration(float64(time.Second) * float64(l.cfg.Clients) / l.cfg.Rate)
	var wg sync.WaitGroup
	for i := 0; i < l.cfg.Clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Stagger clients across the interval.
			time.Sleep(interval * time.Duration(i) / time.Duration(l.cfg.Clients))
			for time.Now().Before(stopSubmitting) {
				t0 := time.Now()
				l.submitUntilAcked(l.next(), giveUp)
				if d := interval - time.Since(t0); d > 0 {
					time.Sleep(d)
				}
			}
		}(i)
	}
	readerDone := make(chan struct{})
	go func() { l.reader(stopSubmitting); close(readerDone) }()
	wg.Wait()
	<-readerDone
	l.log.Info("submitting done", "submits", l.submitted.Load(), "acked_jobs", len(l.ackedIDs()), "gave_up", l.gaveUp.Load())

	l.quiesce()
	if err := l.freeze(); err != nil {
		return err
	}
	return l.writeFinal()
}

func (l *load) submitUntilAcked(req *relayv1.SubmitJobRequest, giveUp time.Time) {
	backoff := 50 * time.Millisecond
	for {
		ctx, cancel := context.WithTimeout(context.Background(), l.cfg.RPCTimeout)
		resp, err := l.jobs.SubmitJob(ctx, req)
		cancel()
		l.submitted.Add(1)
		if err == nil {
			l.ack(resp.JobId)
			return
		}
		if status.Code(err) == codes.InvalidArgument || time.Now().After(giveUp) {
			l.gaveUp.Add(1)
			return
		}
		time.Sleep(backoff/2 + rand.N(backoff/2))
		backoff = min(2*backoff, 2*time.Second)
	}
}

func (l *load) ack(id string) {
	l.acks.Add(1)
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.seen[id] {
		l.seen[id] = true
		l.acked = append(l.acked, id)
	}
}

func (l *load) ackedIDs() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.acked...)
}

// reader issues fresh reads of random jobs during the run; they are part
// of the linearizability check.
func (l *load) reader(until time.Time) {
	for time.Now().Before(until) {
		time.Sleep(100 * time.Millisecond)
		ids := l.ackedIDs()
		if len(ids) == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), l.cfg.RPCTimeout)
		_, _ = l.jobs.GetJob(ctx, &relayv1.GetJobRequest{JobId: ids[rand.IntN(len(ids))]})
		cancel()
	}
}

// quiesce waits until every acknowledged job is terminal, or the timeout.
func (l *load) quiesce() {
	deadline := time.Now().Add(l.cfg.QuiesceTimeout)
	pending := l.ackedIDs()
	for len(pending) > 0 && time.Now().Before(deadline) {
		var still []string
		for _, id := range pending {
			ctx, cancel := context.WithTimeout(context.Background(), l.cfg.RPCTimeout)
			resp, err := l.jobs.GetJob(ctx, &relayv1.GetJobRequest{JobId: id})
			cancel()
			if err != nil || !store.Terminal(history.StatusName(resp.GetJob().GetStatus())) {
				still = append(still, id)
			}
		}
		pending = still
		if len(pending) > 0 {
			l.log.Info("quiescing", "non_terminal", len(pending), "left", time.Until(deadline).Round(time.Second))
			time.Sleep(time.Second)
		}
	}
}

// freeze waits for the orchestrator to stop every worker.
func (l *load) freeze() error {
	if l.cfg.FreezeDir == "" {
		return nil
	}
	if err := os.WriteFile(filepath.Join(l.cfg.FreezeDir, "quiesced"), nil, 0o644); err != nil {
		return err
	}
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(l.cfg.FreezeDir, "frozen")); err == nil {
			l.log.Info("workers frozen; reading final states")
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("timed out waiting for the workers to be frozen")
}

// writeFinal reads every acknowledged job once more, through the normal
// (recorded) client, and writes the snapshot the checker compares against.
func (l *load) writeFinal() error {
	final := check.Final{}
	for _, id := range l.ackedIDs() {
		var resp *relayv1.GetJobResponse
		var err error
		for attempt := 0; attempt < 20; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), l.cfg.RPCTimeout)
			resp, err = l.jobs.GetJob(ctx, &relayv1.GetJobRequest{JobId: id})
			cancel()
			if err == nil || status.Code(err) == codes.NotFound {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		if err != nil {
			final[id] = check.FinalJob{ReadErr: status.Code(err).String() + ": " + status.Convert(err).Message()}
			continue
		}
		final[id] = finalFromProto(resp)
	}
	b, err := json.MarshalIndent(final, "", " ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(l.cfg.Out), 0o755); err != nil {
		return err
	}
	l.log.Info("final states written", "jobs", len(final), "path", l.cfg.Out)
	return os.WriteFile(l.cfg.Out, b, 0o644)
}

func finalFromProto(resp *relayv1.GetJobResponse) check.FinalJob {
	j := resp.GetJob()
	f := check.FinalJob{Job: store.Job{
		ID: j.Id, Type: j.Type, Payload: j.Payload, IdempotencyKey: j.IdempotencyKey,
		Status: history.StatusName(j.Status), Attempts: int(j.Attempts), MaxAttempts: int(j.MaxAttempts),
		LeaseOwner: j.LeaseOwner, LeaseExpiresAt: j.LeaseExpiresAtMs, LeaseToken: j.LeaseToken,
		CreatedAt: j.CreatedAtMs, EnqueuedAt: j.EnqueuedAtMs, LeasedAt: j.LeasedAtMs,
		FinishedAt: j.FinishedAtMs, LastError: j.LastError, Result: j.Result,
	}}
	for _, e := range resp.GetHistory() {
		f.Events = append(f.Events, store.Event{AtMs: e.AtMs, Kind: e.Kind, Worker: e.WorkerId, Token: e.LeaseToken, Detail: e.Detail})
	}
	return f
}
