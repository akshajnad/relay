package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/google/uuid"
)

// LoadConfig is the load generator's settings. Mix weights are relative;
// each submit picks a job type in proportion to them.
type LoadConfig struct {
	Rate        float64 `json:"rate"` // submits per second
	MixNoop     float64 `json:"mix_noop"`
	MixSleep    float64 `json:"mix_sleep"`
	MixFail     float64 `json:"mix_fail"`
	SleepMinMs  int     `json:"sleep_min_ms"`
	SleepMaxMs  int     `json:"sleep_max_ms"`
	FailN       int     `json:"fail_n"`       // fail-n-times fails this many attempts
	MaxAttempts int     `json:"max_attempts"` // 0 = server default
	// DupPct is the percentage of submits that reuse a recent idempotency
	// key, to show deduplication.
	DupPct float64 `json:"dup_pct"`
}

func defaultLoadConfig() LoadConfig {
	return LoadConfig{Rate: 20, MixNoop: 0, MixSleep: 1, MixFail: 0, SleepMinMs: 200, SleepMaxMs: 1500, FailN: 1, MaxAttempts: 3}
}

func (c LoadConfig) Validate() error {
	switch {
	case c.Rate <= 0 || c.Rate > 5000:
		return errors.New("rate must be between 0 and 5000 jobs/s")
	case c.MixNoop < 0 || c.MixSleep < 0 || c.MixFail < 0 || c.MixNoop+c.MixSleep+c.MixFail == 0:
		return errors.New("the job mix needs at least one positive weight")
	case c.SleepMinMs < 0 || c.SleepMaxMs < c.SleepMinMs || c.SleepMaxMs > 10*60*1000:
		return errors.New("sleep range must satisfy 0 ≤ min ≤ max ≤ 10 minutes")
	case c.FailN < 0 || c.FailN > 100:
		return errors.New("fail-n-times n must be between 0 and 100")
	case c.MaxAttempts < 0 || c.MaxAttempts > 100:
		return errors.New("max attempts must be between 0 (server default) and 100")
	case c.DupPct < 0 || c.DupPct > 100:
		return errors.New("duplicate percentage must be between 0 and 100")
	}
	return nil
}

// next draws one submit from the configuration.
func (c LoadConfig) next(r *rand.Rand) *relayv1.SubmitJobRequest {
	req := &relayv1.SubmitJobRequest{MaxAttempts: int32(c.MaxAttempts)}
	x := r.Float64() * (c.MixNoop + c.MixSleep + c.MixFail)
	switch {
	case x < c.MixNoop:
		req.Type = "noop"
	case x < c.MixNoop+c.MixSleep:
		ms := c.SleepMinMs
		if c.SleepMaxMs > c.SleepMinMs {
			ms += r.IntN(c.SleepMaxMs - c.SleepMinMs + 1)
		}
		req.Type = "sleep"
		req.Payload, _ = json.Marshal(map[string]int{"duration_ms": ms})
	default:
		req.Type = "fail-n-times"
		req.Payload, _ = json.Marshal(map[string]int{"n": c.FailN})
	}
	return req
}

// loadGen submits jobs at a steady rate.
type loadGen struct {
	mu      sync.Mutex
	cfg     LoadConfig
	running bool
	cancel  context.CancelFunc
	started time.Time
	lastErr string

	submitted, duplicates, errs, dropped atomic.Int64
}

type loadView struct {
	Running    bool       `json:"running"`
	Config     LoadConfig `json:"config"`
	Submitted  int64      `json:"submitted"`
	Duplicates int64      `json:"duplicates"`
	Errors     int64      `json:"errors"`
	// Dropped counts submits skipped because the submitters fell behind
	// the requested rate.
	Dropped int64   `json:"dropped"`
	Seconds float64 `json:"seconds"`
	LastErr string  `json:"last_error,omitempty"`
}

func newLoadGen() *loadGen { return &loadGen{cfg: defaultLoadConfig()} }

func (l *loadGen) view() loadView {
	l.mu.Lock()
	defer l.mu.Unlock()
	v := loadView{Running: l.running, Config: l.cfg, Submitted: l.submitted.Load(), Duplicates: l.duplicates.Load(),
		Errors: l.errs.Load(), Dropped: l.dropped.Load(), LastErr: l.lastErr}
	if l.running {
		v.Seconds = time.Since(l.started).Seconds()
	}
	return v
}

// submitFunc submits one job on the dashboard's behalf.
type submitFunc func(ctx context.Context, req *relayv1.SubmitJobRequest, origin string, force bool) (*relayv1.SubmitJobResponse, error)

// start (re)starts the generator with cfg.
func (l *loadGen) start(parent context.Context, cfg LoadConfig, submit submitFunc) {
	l.stop()
	ctx, cancel := context.WithCancel(parent)
	l.mu.Lock()
	l.cfg, l.running, l.cancel, l.started, l.lastErr = cfg, true, cancel, time.Now(), ""
	l.mu.Unlock()
	for _, c := range []*atomic.Int64{&l.submitted, &l.duplicates, &l.errs, &l.dropped} {
		c.Store(0)
	}

	// A pacer releases submit slots at the configured rate; a pool sized
	// to the rate takes them. A slot no submitter is free to take is
	// dropped and counted, so a slow cluster shows up as a shortfall
	// rather than as a growing backlog inside the dashboard.
	slots := make(chan struct{}, 64)
	go func() {
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		last, owed := time.Now(), 0.0
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-tick.C:
				owed += now.Sub(last).Seconds() * cfg.Rate
				last = now
				for ; owed >= 1; owed-- {
					select {
					case slots <- struct{}{}:
					default:
						l.dropped.Add(1)
					}
				}
			}
		}
	}()
	submitters := min(128, max(4, int(cfg.Rate/20)))
	keys := &recentKeys{}
	for i := range submitters {
		r := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(i)))
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-slots:
				}
				req := cfg.next(r)
				if cfg.DupPct > 0 {
					if k, ok := keys.pick(r); ok && r.Float64()*100 < cfg.DupPct {
						req.IdempotencyKey = k
					} else {
						req.IdempotencyKey = "dash-" + uuid.NewString()
						keys.add(req.IdempotencyKey)
					}
				}
				sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				resp, err := submit(sctx, req, "load", false)
				cancel()
				switch {
				case err != nil && ctx.Err() == nil:
					l.errs.Add(1)
					l.mu.Lock()
					l.lastErr = err.Error()
					l.mu.Unlock()
				case err == nil && resp.GetDuplicate():
					l.duplicates.Add(1)
				case err == nil:
					l.submitted.Add(1)
				}
			}
		}()
	}
}

func (l *loadGen) stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.cancel != nil {
		l.cancel()
		l.cancel = nil
	}
	l.running = false
}

// recentKeys remembers the last few idempotency keys for duplicate submits.
type recentKeys struct {
	mu   sync.Mutex
	keys []string
	next int
}

func (k *recentKeys) add(key string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.keys) < 256 {
		k.keys = append(k.keys, key)
		return
	}
	k.keys[k.next] = key
	k.next = (k.next + 1) % len(k.keys)
}

func (k *recentKeys) pick(r *rand.Rand) (string, bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.keys) == 0 {
		return "", false
	}
	return k.keys[r.IntN(len(k.keys))], true
}

func (c LoadConfig) String() string {
	return fmt.Sprintf("%.0f jobs/s (noop %.0f : sleep %.0f : fail %.0f)", c.Rate, c.MixNoop, c.MixSleep, c.MixFail)
}
