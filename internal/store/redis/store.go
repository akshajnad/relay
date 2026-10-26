// Package redisstore holds all durable Relay state in Redis. Every state
// transition is a single Lua script (see scripts/), so correctness never
// depends on locks inside the coordinator process.
package redisstore

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/akshajnad/relay/internal/store"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Prefix is the namespace of every key Relay writes. It is mirrored in
// scripts/common.lua.
const Prefix = "relay:"

//go:embed scripts/*.lua
var scriptFS embed.FS

func loadScript(name string) *redis.Script {
	common, err := scriptFS.ReadFile("scripts/common.lua")
	if err != nil {
		panic(err)
	}
	body, err := scriptFS.ReadFile("scripts/" + name + ".lua")
	if err != nil {
		panic(err)
	}
	return redis.NewScript(string(common) + "\n" + string(body))
}

var (
	scriptSubmit        = loadScript("submit")
	scriptRegister      = loadScript("register")
	scriptAcquire       = loadScript("acquire")
	scriptHeartbeat     = loadScript("heartbeat")
	scriptComplete      = loadScript("complete")
	scriptFail          = loadScript("fail")
	scriptRelease       = loadScript("release")
	scriptReap          = loadScript("reap")
	scriptExpireWorkers = loadScript("expire_workers")

	allScripts = []*redis.Script{
		scriptSubmit, scriptRegister, scriptAcquire, scriptHeartbeat, scriptComplete,
		scriptFail, scriptRelease, scriptReap, scriptExpireWorkers,
	}
)

var _ store.Store = (*Store)(nil)

// Store is the Redis implementation of store.Store.
type Store struct {
	rdb *redis.Client
	cfg store.Config
	// jitter returns the backoff jitter fraction in [0,1). Lua's math.random
	// is deterministic inside Redis scripts, so randomness comes from Go.
	jitter func() float64
}

func New(rdb *redis.Client, cfg store.Config) *Store {
	return &Store{rdb: rdb, cfg: cfg, jitter: rand.Float64}
}

func (s *Store) Config() store.Config { return s.cfg }

// LoadScripts preloads every script so the hot path is a single EVALSHA.
// (Script.Run also falls back to EVAL on NOSCRIPT, e.g. after a Redis
// restart, so this is an optimization rather than a requirement.)
func (s *Store) LoadScripts(ctx context.Context) error {
	for _, sc := range allScripts {
		if err := sc.Load(ctx, s.rdb).Err(); err != nil {
			return fmt.Errorf("load script: %w", err)
		}
	}
	return nil
}

// Submit creates a job, or returns the existing job's id with
// duplicate=true if the idempotency key was seen before.
func (s *Store) Submit(ctx context.Context, p store.SubmitParams) (id string, duplicate bool, err error) {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = s.cfg.DefaultMaxAttempts
	}
	newID, err := uuid.NewV7()
	if err != nil {
		return "", false, err
	}
	res, err := scriptSubmit.Run(ctx, s.rdb, nil,
		newID.String(), p.Type, p.Payload, p.IdempotencyKey, p.MaxAttempts).Slice()
	if err != nil {
		return "", false, err
	}
	return res[0].(string), res[1].(int64) == 1, nil
}

// RegisterWorker adds a live worker and returns its id.
func (s *Store) RegisterWorker(ctx context.Context, hostname string, concurrency int) (string, error) {
	id := "w-" + hostname + "-" + uuid.NewString()[:8]
	if err := scriptRegister.Run(ctx, s.rdb, nil, id, hostname, concurrency).Err(); err != nil {
		return "", err
	}
	return id, nil
}

// Acquire leases the oldest eligible job to workerID.
func (s *Store) Acquire(ctx context.Context, workerID string) (store.Acquired, error) {
	res, err := scriptAcquire.Run(ctx, s.rdb, nil, workerID, s.cfg.LeaseDuration.Milliseconds()).Slice()
	if err != nil {
		return store.Acquired{}, err
	}
	switch res[0].(string) {
	case "NO_WORKER":
		return store.Acquired{}, store.ErrWorkerNotAlive
	case "EMPTY":
		next := res[2].(int64)
		in := time.Duration(-1)
		if next >= 0 {
			in = time.Duration(next) * time.Millisecond
		}
		return store.Acquired{StoreNowMs: res[1].(int64), NextEligibleIn: in}, nil
	}
	job, err := parseJob(res[3].([]interface{}))
	if err != nil {
		return store.Acquired{}, err
	}
	return store.Acquired{Job: job, StoreNowMs: res[1].(int64), EligibleAtMs: res[2].(int64), More: res[4].(int64) == 1}, nil
}

// Renew records liveness for workerID and renews the leases it holds.
// It returns the held leases that could not be renewed.
func (s *Store) Renew(ctx context.Context, workerID string, held []store.Lease) ([]store.Lease, error) {
	args := make([]interface{}, 0, 2+2*len(held))
	args = append(args, workerID, s.cfg.LeaseDuration.Milliseconds())
	for _, l := range held {
		args = append(args, l.JobID, l.Token)
	}
	res, err := scriptHeartbeat.Run(ctx, s.rdb, nil, args...).Slice()
	if err != nil {
		return nil, err
	}
	if res[0].(string) == "NO_WORKER" {
		return nil, store.ErrWorkerNotAlive
	}
	flat := res[1].([]interface{})
	lost := make([]store.Lease, 0, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		tok, _ := strconv.ParseInt(flat[i+1].(string), 10, 64)
		lost = append(lost, store.Lease{JobID: flat[i].(string), Token: tok})
	}
	return lost, nil
}

// Complete marks the job SUCCEEDED if workerID still holds lease token.
// already is true when this lease had completed the job before (a retried
// call); the stored result is not rewritten.
func (s *Store) Complete(ctx context.Context, jobID, workerID string, token int64, result []byte) (already bool, err error) {
	res, err := scriptComplete.Run(ctx, s.rdb, nil, jobID, workerID, token, result).Slice()
	if err != nil {
		return false, err
	}
	switch res[0].(string) {
	case "OK":
		return false, nil
	case "ALREADY":
		return true, nil
	case "NOT_FOUND":
		return false, store.ErrNotFound
	}
	return false, staleErr(jobID, res)
}

// Fail records a failed attempt if workerID still holds lease token.
func (s *Store) Fail(ctx context.Context, jobID, workerID string, token int64, errMsg string, retryable bool) (store.FailOutcome, error) {
	r := 0
	if retryable {
		r = 1
	}
	res, err := scriptFail.Run(ctx, s.rdb, nil, jobID, workerID, token, errMsg, r,
		s.cfg.BackoffBase.Milliseconds(), s.cfg.BackoffMax.Milliseconds(), s.jitter()).Slice()
	if err != nil {
		return store.FailOutcome{}, err
	}
	switch res[0].(string) {
	case "RETRY":
		return store.FailOutcome{Status: store.StatusPending, RetryIn: time.Duration(res[1].(int64)) * time.Millisecond}, nil
	case "FAILED":
		return store.FailOutcome{Status: store.StatusFailed}, nil
	case "DEAD":
		return store.FailOutcome{Status: store.StatusDead}, nil
	case "ALREADY":
		return store.FailOutcome{Status: res[1].(string), Already: true}, nil
	case "NOT_FOUND":
		return store.FailOutcome{}, store.ErrNotFound
	}
	return store.FailOutcome{}, staleErr(jobID, res)
}

// Release returns a just-acquired lease that could not be delivered to the
// worker. The attempt does not count.
func (s *Store) Release(ctx context.Context, jobID, workerID string, token int64) error {
	res, err := scriptRelease.Run(ctx, s.rdb, nil, jobID, workerID, token).Slice()
	if err != nil {
		return err
	}
	if res[0].(string) != "OK" {
		return &store.StaleLeaseError{JobID: jobID}
	}
	return nil
}

// ReapExpired requeues jobs whose leases expired. It loops in batches until
// no expired lease remains.
func (s *Store) ReapExpired(ctx context.Context) (store.RevokeResult, error) {
	var total store.RevokeResult
	for {
		res, err := scriptReap.Run(ctx, s.rdb, nil, s.cfg.ScanBatch).Slice()
		if err != nil {
			return total, err
		}
		total.Requeued += int(res[0].(int64))
		total.Dead += int(res[1].(int64))
		if int(res[2].(int64)) < s.cfg.ScanBatch {
			return total, nil
		}
	}
}

// ExpireWorkers declares dead every worker that has not heartbeated within
// deadAfter and revokes all of its leases. It returns the expired worker ids.
func (s *Store) ExpireWorkers(ctx context.Context, deadAfter time.Duration) (store.RevokeResult, []string, error) {
	var total store.RevokeResult
	var workers []string
	for {
		res, err := scriptExpireWorkers.Run(ctx, s.rdb, nil, deadAfter.Milliseconds(), s.cfg.ScanBatch).Slice()
		if err != nil {
			return total, workers, err
		}
		total.Requeued += int(res[0].(int64))
		total.Dead += int(res[1].(int64))
		ids := res[2].([]interface{})
		for _, id := range ids {
			workers = append(workers, id.(string))
		}
		if len(ids) < s.cfg.ScanBatch {
			return total, workers, nil
		}
	}
}

// Get returns a job and its transition history.
func (s *Store) Get(ctx context.Context, id string) (*store.Job, []store.Event, error) {
	pipe := s.rdb.Pipeline()
	h := pipe.HGetAll(ctx, Prefix+"job:"+id)
	ev := pipe.LRange(ctx, Prefix+"job:"+id+":events", 0, -1)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, nil, err
	}
	if len(h.Val()) == 0 {
		return nil, nil, store.ErrNotFound
	}
	job, err := jobFromMap(h.Val())
	if err != nil {
		return nil, nil, err
	}
	events := make([]store.Event, 0, len(ev.Val()))
	for _, raw := range ev.Val() {
		var e store.Event
		if err := json.Unmarshal([]byte(raw), &e); err != nil {
			return nil, nil, fmt.Errorf("decode event: %w", err)
		}
		events = append(events, e)
	}
	return job, events, nil
}

// ListWorkers returns every worker ever registered, live or dead.
func (s *Store) ListWorkers(ctx context.Context) ([]store.Worker, error) {
	ids, err := s.rdb.SMembers(ctx, Prefix+"workers:all").Result()
	if err != nil {
		return nil, err
	}
	pipe := s.rdb.Pipeline()
	hashes := make([]*redis.MapStringStringCmd, len(ids))
	active := make([]*redis.IntCmd, len(ids))
	for i, id := range ids {
		hashes[i] = pipe.HGetAll(ctx, Prefix+"worker:"+id)
		active[i] = pipe.SCard(ctx, Prefix+"worker:"+id+":jobs")
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	workers := make([]store.Worker, 0, len(ids))
	for i := range ids {
		m := hashes[i].Val()
		if len(m) == 0 {
			continue
		}
		workers = append(workers, store.Worker{
			ID:            m["id"],
			Hostname:      m["hostname"],
			Concurrency:   atoi(m["concurrency"]),
			State:         m["state"],
			RegisteredAt:  atoi64(m["registered_at"]),
			LastHeartbeat: atoi64(m["last_heartbeat"]),
			ActiveJobs:    int(active[i].Val()),
		})
	}
	return workers, nil
}

// QueueDepth counts pending (ready, including backoff) and leased jobs.
func (s *Store) QueueDepth(ctx context.Context) (store.Depth, error) {
	pipe := s.rdb.Pipeline()
	ready := pipe.ZCard(ctx, Prefix+"ready")
	leased := pipe.ZCard(ctx, Prefix+"leases")
	if _, err := pipe.Exec(ctx); err != nil {
		return store.Depth{}, err
	}
	return store.Depth{Pending: ready.Val(), Leased: leased.Val()}, nil
}

func staleErr(jobID string, res []interface{}) error {
	e := &store.StaleLeaseError{JobID: jobID}
	if len(res) >= 4 {
		e.Status, _ = res[1].(string)
		e.CurrentOwner, _ = res[2].(string)
		if tok, ok := res[3].(string); ok {
			e.CurrentToken = atoi64(tok)
		}
	}
	return e
}

func parseJob(flat []interface{}) (*store.Job, error) {
	m := make(map[string]string, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		m[flat[i].(string)] = flat[i+1].(string)
	}
	return jobFromMap(m)
}

func jobFromMap(m map[string]string) (*store.Job, error) {
	if m["id"] == "" {
		return nil, errors.New("malformed job hash")
	}
	j := &store.Job{
		ID:             m["id"],
		Type:           m["type"],
		Payload:        []byte(m["payload"]),
		IdempotencyKey: m["idempotency_key"],
		Status:         m["status"],
		Attempts:       atoi(m["attempts"]),
		MaxAttempts:    atoi(m["max_attempts"]),
		LeaseOwner:     m["lease_owner"],
		LeaseExpiresAt: atoi64(m["lease_expires_at"]),
		LeaseToken:     atoi64(m["lease_token"]),
		CreatedAt:      atoi64(m["created_at"]),
		EnqueuedAt:     atoi64(m["enqueued_at"]),
		LeasedAt:       atoi64(m["leased_at"]),
		FinishedAt:     atoi64(m["finished_at"]),
		LastError:      m["last_error"],
	}
	if r := m["result"]; r != "" {
		j.Result = []byte(r)
	}
	return j, nil
}

func atoi(s string) int { return int(atoi64(s)) }

func atoi64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
