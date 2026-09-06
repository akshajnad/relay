package coordinator

import (
	"context"
	"time"
)

// RunLiveness runs the failure detector and the lease reaper until ctx is
// done. The detector is the fast path: a worker that misses DeadAfter worth
// of heartbeats is declared dead and every job it held is requeued at once.
// The reaper is the backstop for any lease that expires without its worker
// being declared dead, e.g. a lease whose AcquireJob response never reached
// the worker.
func (c *Coordinator) RunLiveness(ctx context.Context) {
	if c.cfg.StartupGrace > 0 {
		c.log.Info("liveness loops start after grace period", "grace", c.cfg.StartupGrace)
		select {
		case <-time.After(c.cfg.StartupGrace):
		case <-ctx.Done():
			return
		}
	}
	detect := time.NewTicker(c.cfg.HeartbeatInterval)
	defer detect.Stop()
	reap := time.NewTicker(c.cfg.ReapInterval)
	defer reap.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-detect.C:
			if c.serving() {
				c.detectDeadWorkers(ctx)
			}
		case <-reap.C:
			if c.serving() {
				c.reapExpiredLeases(ctx)
			}
		}
	}
}

func (c *Coordinator) detectDeadWorkers(ctx context.Context) {
	sctx, cancel := context.WithTimeout(ctx, c.cfg.StoreTimeout)
	defer cancel()
	r, workers, err := c.store.ExpireWorkers(sctx, c.cfg.DeadAfter)
	if err != nil {
		c.log.Error("failure detector", "err", err)
		return
	}
	if len(workers) == 0 {
		return
	}
	c.metrics.WorkersExpired.Add(int64(len(workers)))
	c.metrics.Requeued.Add(int64(r.Requeued))
	c.metrics.Dead.Add(int64(r.Dead))
	c.notifier.Signal(r.Requeued)
	c.log.Warn("workers declared dead; leases revoked",
		"workers", workers, "requeued", r.Requeued, "dead_lettered", r.Dead)
}

func (c *Coordinator) reapExpiredLeases(ctx context.Context) {
	sctx, cancel := context.WithTimeout(ctx, c.cfg.StoreTimeout)
	defer cancel()
	r, err := c.store.ReapExpired(sctx)
	if err != nil {
		c.log.Error("reaper", "err", err)
		return
	}
	if r.Requeued+r.Dead == 0 {
		return
	}
	c.metrics.Requeued.Add(int64(r.Requeued))
	c.metrics.Dead.Add(int64(r.Dead))
	c.metrics.LeaseExpirations.Add(int64(r.Requeued + r.Dead))
	c.notifier.Signal(r.Requeued)
	c.log.Warn("expired leases reaped", "requeued", r.Requeued, "dead_lettered", r.Dead)
}
