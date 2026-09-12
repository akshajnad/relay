package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akshajnad/relay/internal/history"
)

// driveConfig is the in-cluster load driver's configuration. It runs as a
// container next to the workers, so its submit timestamps and their
// handler timestamps come from the same clock.
type driveConfig struct {
	Client      string
	Target      string
	HistoryDir  string
	Mode        string
	Jobs        int
	Rate        float64
	Type        string
	SleepMs     int
	Submitters  int
	MaxAttempts int
	KeyPrefix   string
}

func driveMain(args []string) error {
	var cfg driveConfig
	fs := flag.NewFlagSet("drive", flag.ExitOnError)
	fs.StringVar(&cfg.Client, "client", "relay", "relay or asynq")
	fs.StringVar(&cfg.Target, "target", "", "relay: coordinator endpoints; asynq: Redis address")
	fs.StringVar(&cfg.HistoryDir, "history-dir", "/history", "where to record submit times")
	fs.StringVar(&cfg.Mode, "mode", "preload", "preload (as fast as possible) or steady (--rate)")
	fs.IntVar(&cfg.Jobs, "jobs", 1000, "jobs to submit")
	fs.Float64Var(&cfg.Rate, "rate", 500, "steady mode: submits per second")
	fs.StringVar(&cfg.Type, "type", "noop", "job type: noop or sleep")
	fs.IntVar(&cfg.SleepMs, "sleep-ms", 20, "sleep job duration")
	fs.IntVar(&cfg.Submitters, "submitters", 32, "concurrent submitters")
	fs.IntVar(&cfg.MaxAttempts, "max-attempts", 3, "attempts per job")
	fs.StringVar(&cfg.KeyPrefix, "key-prefix", "job", "job keys are <prefix>-<n>; phases use distinct prefixes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rec, err := history.Open(cfg.HistoryDir, "driver")
	if err != nil {
		return err
	}
	defer rec.Close()
	client, err := newJobClient(cfg.Client, cfg.Target, false)
	if err != nil {
		return err
	}
	defer client.Close()
	return drive(cfg, client, rec)
}

func payloadFor(typ string, sleepMs int) []byte {
	if typ == "sleep" {
		return []byte(fmt.Sprintf(`{"duration_ms":%d}`, sleepMs))
	}
	return nil
}

// drive submits cfg.Jobs jobs, recording each submit's invocation and
// acknowledgement. An unacknowledged submit is retried with the same key.
func drive(cfg driveConfig, client jobClient, rec *history.Recorder) error {
	payload := payloadFor(cfg.Type, cfg.SleepMs)
	var next atomic.Int64
	var failed atomic.Int64
	var interval time.Duration
	start := time.Now()
	if cfg.Mode == "steady" {
		interval = time.Duration(float64(time.Second) / cfg.Rate)
	}
	var wg sync.WaitGroup
	for s := 0; s < cfg.Submitters; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n := int(next.Add(1)) - 1
				if n >= cfg.Jobs {
					return
				}
				if interval > 0 {
					if d := time.Until(start.Add(time.Duration(n) * interval)); d > 0 {
						time.Sleep(d)
					}
				}
				key := fmt.Sprintf("%s-%d", cfg.KeyPrefix, n)
				if !submitOne(client, rec, key, cfg.Type, payload, cfg.MaxAttempts) {
					failed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("submitted %d jobs in %v (%.0f/s), %d gave up\n", cfg.Jobs, elapsed.Round(time.Millisecond),
		float64(cfg.Jobs)/elapsed.Seconds(), failed.Load())
	if failed.Load() > 0 {
		return fmt.Errorf("%d submits never succeeded", failed.Load())
	}
	return nil
}

func submitOne(client jobClient, rec *history.Recorder, key, typ string, payload []byte, maxAttempts int) bool {
	for attempt := 0; attempt < 50; attempt++ {
		op := rec.Begin(history.Record{Method: history.Submit, Key: key})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		id, err := client.Submit(ctx, key, typ, payload, maxAttempts)
		cancel()
		if err == nil {
			rec.End(op, history.OK, history.Record{JobID: id, Key: key})
			return true
		}
		rec.End(op, history.Info, history.Record{Err: err.Error(), Key: key})
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
