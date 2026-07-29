package coordinator

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func signaled(w *Waiter) bool {
	select {
	case <-w.C():
		return true
	default:
		return false
	}
}

func TestNotifierSignalsFIFO(t *testing.T) {
	n := NewNotifier()
	a, b, c := n.Subscribe(), n.Subscribe(), n.Subscribe()
	n.Signal(2)
	if !signaled(a) || !signaled(b) || signaled(c) {
		t.Fatal("Signal(2) should wake exactly the two oldest waiters")
	}
	if n.Waiting() != 1 {
		t.Fatalf("waiting = %d, want 1", n.Waiting())
	}
	n.Signal(5)
	if !signaled(c) || n.Waiting() != 0 {
		t.Fatal("remaining waiter not woken")
	}
	n.Signal(1) // no waiters: a no-op, not an error
}

func TestNotifierCancelUnsignaled(t *testing.T) {
	n := NewNotifier()
	a, b := n.Subscribe(), n.Subscribe()
	a.Cancel()
	n.Signal(1)
	if !signaled(b) {
		t.Fatal("signal went to a cancelled waiter")
	}
}

// A waiter that was signaled but then found a job on its own must pass the
// signal on, or a second job would sit unnoticed until a poll timeout.
func TestNotifierCancelForwardsSignal(t *testing.T) {
	n := NewNotifier()
	a, b := n.Subscribe(), n.Subscribe()
	n.Signal(1)
	a.Cancel()
	if !signaled(b) {
		t.Fatal("cancelled signaled waiter did not forward its signal")
	}
}

func TestNotifierSignalAfterKeepsEarliest(t *testing.T) {
	n := NewNotifier()
	w := n.Subscribe()
	n.SignalAfter(time.Hour)
	n.SignalAfter(20 * time.Millisecond)
	n.SignalAfter(time.Hour) // later deadline must not displace the earlier one
	select {
	case <-w.C():
	case <-time.After(2 * time.Second):
		t.Fatal("SignalAfter never fired")
	}
}

func TestNotifierClose(t *testing.T) {
	n := NewNotifier()
	a := n.Subscribe()
	n.Close()
	if !signaled(a) {
		t.Fatal("Close did not wake waiter")
	}
	if !signaled(n.Subscribe()) {
		t.Fatal("Subscribe after Close should return a signaled waiter")
	}
	n.Subscribe().Cancel() // must not panic or re-signal
}

// Hammer the notifier from many goroutines: every signal is either consumed
// by exactly one waiter or forwarded, and nothing deadlocks. Run with -race.
func TestNotifierConcurrent(t *testing.T) {
	n := NewNotifier()
	var woken atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				w := n.Subscribe()
				select {
				case <-w.C():
					woken.Add(1)
				case <-time.After(time.Millisecond):
					w.Cancel()
				case <-stop:
					w.Cancel()
					return
				}
			}
		}()
	}
	for i := 0; i < 2000; i++ {
		n.Signal(1)
	}
	deadline := time.Now().Add(5 * time.Second)
	for woken.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
	if woken.Load() == 0 {
		t.Fatal("no waiter was ever woken")
	}
}
