//go:build integration

package integration

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	relayv1 "github.com/akshajnad/relay/gen/relay/v1"
	"github.com/akshajnad/relay/internal/metrics"
)

const (
	succeeded = relayv1.JobStatus_JOB_STATUS_SUCCEEDED
	leased    = relayv1.JobStatus_JOB_STATUS_LEASED
	dead      = relayv1.JobStatus_JOB_STATUS_DEAD
)

func sleepPayload(ms int) []byte { return []byte(fmt.Sprintf(`{"duration_ms":%d}`, ms)) }

func waitLeased(t *testing.T, c *Cluster, id string) *relayv1.Job {
	t.Helper()
	var j *relayv1.Job
	Eventually(t, 30*time.Second, "job "+id+" to be leased", func() bool {
		j = c.Get(id).Job
		return j.Status == leased
	})
	return j
}

func scrapeMetrics(t *testing.T, c *Cluster) metrics.Snapshot {
	t.Helper()
	resp, err := http.Get("http://" + c.MetricsAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s metrics.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

// docker kill a worker in the middle of a job: the failure detector
// revokes its lease, another worker runs the job, and the job reaches
// SUCCEEDED exactly once. Background jobs keep flowing throughout.
func TestWorkerKilledMidJob(t *testing.T) {
	c := NewCluster(t, ClusterOpts{Workers: 3})

	victim := c.Submit(&relayv1.SubmitJobRequest{Type: "sleep", Payload: sleepPayload(5000), MaxAttempts: 3}).JobId
	var background []string
	for i := 0; i < 30; i++ {
		background = append(background, c.Submit(&relayv1.SubmitJobRequest{Type: "sleep", Payload: sleepPayload(200), MaxAttempts: 3}).JobId)
	}

	j := waitLeased(t, c, victim)
	container := c.ContainerOf(j.LeaseOwner)
	t.Logf("killing worker %s (container %.12s) while it runs job %s", j.LeaseOwner, container, victim)
	docker(t, "kill", container)

	final := c.WaitTerminal(append([]string{victim}, background...), 60*time.Second)
	got := final[victim]
	if got.Job.Status != succeeded {
		t.Fatalf("victim job ended %v: %s", got.Job.Status, kinds(got.History))
	}
	if got.Job.LeaseOwner == j.LeaseOwner || got.Job.Attempts != 2 {
		t.Fatalf("victim job: owner %s attempts %d, want a new owner on attempt 2", got.Job.LeaseOwner, got.Job.Attempts)
	}
	if n := terminalEvents(got.History); n != 1 {
		t.Fatalf("victim job has %d terminal transitions: %s", n, kinds(got.History))
	}
	requeued := false
	for _, e := range got.History {
		if e.Kind == "REQUEUED" && e.WorkerId == j.LeaseOwner && strings.Contains(e.Detail, "declared dead") {
			requeued = true
		}
	}
	if !requeued {
		t.Fatalf("expected a REQUEUED event from the failure detector: %s", kinds(got.History))
	}
	for _, id := range background {
		if r := final[id]; r.Job.Status != succeeded || terminalEvents(r.History) != 1 {
			t.Fatalf("background job %s: %v, %s", id, r.Job.Status, kinds(r.History))
		}
	}
}

// docker pause a worker until it is declared dead and its job is completed
// elsewhere, then unpause it: the zombie's completion is rejected by
// fencing, it logs and drops the result, and the stored result is intact.
func TestPausedWorkerIsFencedOut(t *testing.T) {
	c := NewCluster(t, ClusterOpts{Workers: 1, Concurrency: 1})

	// With one worker, the job necessarily lands on it.
	id := c.Submit(&relayv1.SubmitJobRequest{Type: "sleep", Payload: sleepPayload(8000), MaxAttempts: 3}).JobId
	j := waitLeased(t, c, id)
	zombie, zombieContainer := j.LeaseOwner, c.ContainerOf(j.LeaseOwner)

	c.Scale(3)
	c.WaitAliveWorkers(3)
	t.Logf("pausing worker %s (container %.12s) mid-job", zombie, zombieContainer)
	docker(t, "pause", zombieContainer)
	unpaused := false
	defer func() {
		if !unpaused {
			docker(t, "unpause", zombieContainer)
		}
	}()

	var done *relayv1.GetJobResponse
	Eventually(t, 60*time.Second, "job to complete on another worker", func() bool {
		done = c.Get(id)
		return done.Job.Status == succeeded
	})
	if done.Job.LeaseOwner == zombie || done.Job.LeaseToken != j.LeaseToken+1 {
		t.Fatalf("job completed by %s with token %d; zombie %s held token %d", done.Job.LeaseOwner, done.Job.LeaseToken, zombie, j.LeaseToken)
	}

	docker(t, "unpause", zombieContainer)
	unpaused = true
	// The zombie's sleep timer has long expired, so it reports at once.
	Eventually(t, 30*time.Second, "zombie to log the stale-lease rejection", func() bool {
		for _, line := range strings.Split(docker(t, "logs", zombieContainer), "\n") {
			if strings.Contains(line, `"msg":"stale lease, dropping result"`) && strings.Contains(line, id) {
				t.Logf("zombie log: %s", line)
				return true
			}
		}
		return false
	})
	Eventually(t, 30*time.Second, "zombie to re-register", func() bool {
		return strings.Contains(docker(t, "logs", zombieContainer), "re-registered after being declared dead")
	})

	after := c.Get(id)
	if string(after.Job.Result) != string(done.Job.Result) || after.Job.FinishedAtMs != done.Job.FinishedAtMs ||
		after.Job.LeaseOwner != done.Job.LeaseOwner {
		t.Fatalf("zombie overwrote the job: before %v, after %v", done.Job, after.Job)
	}
	if n := terminalEvents(after.History); n != 1 {
		t.Fatalf("%d terminal transitions: %s", n, kinds(after.History))
	}
	if s := scrapeMetrics(t, c); s.Counters.StaleRejections < 1 {
		t.Fatalf("coordinator recorded no stale rejection: %+v", s.Counters)
	}
}

// Kill and restart the coordinator mid-run while submissions continue
// (retrying with the same idempotency keys): no job is lost, none is
// duplicated, and each reaches SUCCEEDED exactly once.
func TestCoordinatorRestart(t *testing.T) {
	c := NewCluster(t, ClusterOpts{Workers: 3})
	const total = 300
	keys := make([]string, total)
	for i := range keys {
		keys[i] = fmt.Sprintf("restart-%03d", i)
	}
	req := func(i int) *relayv1.SubmitJobRequest {
		return &relayv1.SubmitJobRequest{Type: "sleep", Payload: sleepPayload(200), IdempotencyKey: keys[i], MaxAttempts: 5}
	}

	var mu sync.Mutex
	ids := map[string]string{} // key -> job id
	record := func(key string, resp *relayv1.SubmitJobResponse) {
		if resp == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if prev, ok := ids[key]; ok && prev != resp.JobId {
			t.Errorf("key %s mapped to two jobs: %s and %s", key, prev, resp.JobId)
		}
		ids[key] = resp.JobId
	}
	for i := 0; i < total/2; i++ {
		record(keys[i], c.Submit(req(i)))
	}
	first := ids[keys[0]]
	Eventually(t, 30*time.Second, "work to be under way", func() bool { return c.Get(first).Job.Status == succeeded })

	t.Log("killing coordinator")
	c.mustCompose("kill", "coordinator")

	// Keep submitting through the outage: the rest of the keys, plus every
	// key again, as a client retrying after timeouts would.
	var sawOutage atomic.Bool
	submitted := make(chan struct{})
	go func() {
		defer close(submitted)
		order := rand.Perm(total)
		for _, i := range order {
			record(keys[i], c.SubmitRetrying(req(i), func(error) { sawOutage.Store(true) }))
		}
	}()
	Eventually(t, 30*time.Second, "a submission to fail during the outage", sawOutage.Load)
	t.Log("restarting coordinator")
	c.mustCompose("up", "-d", "--wait", "--no-build", "--no-recreate", "coordinator")
	<-submitted

	if len(ids) != total {
		t.Fatalf("%d keys recorded, want %d", len(ids), total)
	}
	all := make([]string, 0, total)
	distinct := map[string]bool{}
	for _, id := range ids {
		all = append(all, id)
		distinct[id] = true
	}
	if len(distinct) != total {
		t.Fatalf("%d distinct job ids for %d keys", len(distinct), total)
	}
	final := c.WaitTerminal(all, 120*time.Second)
	for id, r := range final {
		if r.Job.Status != succeeded || terminalEvents(r.History) != 1 {
			t.Fatalf("job %s: %v, %s", id, r.Job.Status, kinds(r.History))
		}
	}
	if n := c.CountJobs(); n != total {
		t.Fatalf("redis holds %d jobs, want %d", n, total)
	}
}

// 1,000 concurrent submissions where 10% reuse another submission's
// idempotency key: exactly 900 jobs exist and all of them succeed.
func TestIdempotentSubmissions(t *testing.T) {
	c := NewCluster(t, ClusterOpts{Workers: 3})
	const unique, dups = 900, 100
	keys := make([]string, 0, unique+dups)
	for i := 0; i < unique; i++ {
		keys = append(keys, fmt.Sprintf("idem-%04d", i))
	}
	for i := 0; i < dups; i++ {
		keys = append(keys, keys[rand.IntN(unique)])
	}
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	var mu sync.Mutex
	byKey := map[string]string{}
	created, duplicates := 0, 0
	var wg sync.WaitGroup
	sem := make(chan struct{}, 32)
	for _, k := range keys {
		wg.Add(1)
		sem <- struct{}{}
		go func(k string) {
			defer wg.Done()
			defer func() { <-sem }()
			resp := c.Submit(&relayv1.SubmitJobRequest{Type: "noop", IdempotencyKey: k})
			mu.Lock()
			defer mu.Unlock()
			if prev, ok := byKey[k]; ok && prev != resp.JobId {
				t.Errorf("key %s returned two job ids", k)
			}
			byKey[k] = resp.JobId
			if resp.Duplicate {
				duplicates++
			} else {
				created++
			}
		}(k)
	}
	wg.Wait()
	if created != unique || duplicates != dups {
		t.Fatalf("created %d, duplicates %d; want %d and %d", created, duplicates, unique, dups)
	}
	if n := c.CountJobs(); n != unique {
		t.Fatalf("redis holds %d jobs, want %d", n, unique)
	}
	ids := make([]string, 0, unique)
	for _, id := range byKey {
		ids = append(ids, id)
	}
	for id, r := range c.WaitTerminal(ids, 60*time.Second) {
		if r.Job.Status != succeeded || terminalEvents(r.History) != 1 {
			t.Fatalf("job %s: %v, %s", id, r.Job.Status, kinds(r.History))
		}
	}
}

// fail-n-times retries with exponential backoff and then succeeds, or ends
// in DEAD (and the dead-letter set) once attempts run out.
func TestFailNTimesRetriesThenSucceedsOrDies(t *testing.T) {
	const backoffBase = 300 * time.Millisecond
	c := NewCluster(t, ClusterOpts{Workers: 3, Env: map[string]string{"RELAY_BACKOFF_BASE": backoffBase.String()}})

	ok := c.Submit(&relayv1.SubmitJobRequest{Type: "fail-n-times", Payload: []byte(`{"n":2}`), MaxAttempts: 5}).JobId
	doomed := c.Submit(&relayv1.SubmitJobRequest{Type: "fail-n-times", Payload: []byte(`{"n":6}`), MaxAttempts: 5}).JobId
	final := c.WaitTerminal([]string{ok, doomed}, 60*time.Second)

	r := final[ok]
	if r.Job.Status != succeeded || r.Job.Attempts != 3 || countEvents(r.History, "RETRY") != 2 {
		t.Fatalf("n=2: %v after %d attempts: %s", r.Job.Status, r.Job.Attempts, kinds(r.History))
	}
	// Equal-jitter backoff guarantees at least half the nominal delay:
	// retry k waits >= base * 2^(k-1) / 2.
	retry := 0
	var failedAt int64
	for _, e := range r.History {
		switch e.Kind {
		case "RETRY":
			retry++
			failedAt = e.AtMs
		case "LEASED":
			if retry == 0 {
				continue
			}
			minGap := (backoffBase << (retry - 1)) / 2
			if gap := time.Duration(e.AtMs-failedAt) * time.Millisecond; gap < minGap {
				t.Fatalf("retry %d leased %v after failing, want >= %v", retry, gap, minGap)
			}
		}
	}

	r = final[doomed]
	if r.Job.Status != dead || r.Job.Attempts != 5 || r.Job.LastError != "planned failure 5 of 6" {
		t.Fatalf("n=6: %v", r.Job)
	}
	if err := c.Redis.ZScore(ctx(t), "relay:dead", doomed).Err(); err != nil {
		t.Fatalf("dead job missing from relay:dead: %v", err)
	}
}
