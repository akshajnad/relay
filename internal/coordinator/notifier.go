package coordinator

import (
	"container/list"
	"sync"
	"time"
)

// Notifier wakes long-polling AcquireJob calls when work may be available.
//
// Waiters queue in FIFO order and Signal(n) wakes at most n of them, so one
// submitted job wakes one worker instead of the whole pool. A caller must
// Subscribe *before* it checks Redis: a job submitted between the check and
// the wait then still finds the waiter in the queue, so no wake-up is lost.
//
// Signals are hints, not reservations. A woken waiter may find the queue
// empty (another worker took the job first) and simply waits again.
type Notifier struct {
	mu      sync.Mutex
	waiters list.List // of *Waiter
	timer   *time.Timer
	timerAt time.Time
	closed  bool
}

// Waiter is a single-use subscription. Exactly one of these happens to it:
// it receives from C, or the caller calls Cancel.
type Waiter struct {
	n        *Notifier
	ch       chan struct{}
	elem     *list.Element
	signaled bool
}

func NewNotifier() *Notifier { return &Notifier{} }

// Subscribe enqueues a new waiter. If the notifier is closed, the returned
// waiter is already signaled.
func (n *Notifier) Subscribe() *Waiter {
	w := &Waiter{n: n, ch: make(chan struct{}, 1)}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		w.signaled = true
		w.ch <- struct{}{}
		return w
	}
	w.elem = n.waiters.PushBack(w)
	return w
}

// C is ready once the waiter has been signaled.
func (w *Waiter) C() <-chan struct{} { return w.ch }

// Cancel withdraws a waiter that is no longer going to wait, either because
// it found a job on its own or because it gave up. If a signal had already
// been delivered to it, the signal is handed to the next waiter rather than
// being swallowed.
func (w *Waiter) Cancel() {
	n := w.n
	n.mu.Lock()
	if !w.signaled {
		n.waiters.Remove(w.elem)
		n.mu.Unlock()
		return
	}
	closed := n.closed
	n.mu.Unlock()
	if !closed {
		n.Signal(1)
	}
}

// Signal wakes up to k waiters, oldest first.
func (n *Notifier) Signal(k int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for ; k > 0; k-- {
		front := n.waiters.Front()
		if front == nil {
			return
		}
		w := n.waiters.Remove(front).(*Waiter)
		w.signaled = true
		w.ch <- struct{}{}
	}
}

// SignalAfter wakes one waiter after d, for work that becomes eligible in
// the future (retry backoff). Only the earliest pending deadline is kept;
// the waiter it wakes passes the signal on if more work is eligible.
func (n *Notifier) SignalAfter(d time.Duration) {
	at := time.Now().Add(d)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	if n.timer != nil && !n.timerAt.After(at) {
		return
	}
	if n.timer != nil {
		n.timer.Stop()
	}
	n.timerAt = at
	n.timer = time.AfterFunc(d, func() {
		n.mu.Lock()
		if n.timerAt.Equal(at) {
			n.timer = nil
		}
		n.mu.Unlock()
		n.Signal(1)
	})
}

// Waiting reports the number of queued waiters.
func (n *Notifier) Waiting() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.waiters.Len()
}

// Close wakes every waiter and makes future subscriptions return at once.
func (n *Notifier) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.closed = true
	if n.timer != nil {
		n.timer.Stop()
	}
	for e := n.waiters.Front(); e != nil; e = n.waiters.Front() {
		w := n.waiters.Remove(e).(*Waiter)
		w.signaled = true
		w.ch <- struct{}{}
	}
}

func (n *Notifier) Closed() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.closed
}
