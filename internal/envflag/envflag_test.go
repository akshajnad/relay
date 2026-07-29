package envflag

import (
	"flag"
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	t.Setenv("RELAY_REDIS_ADDR", "redis:6379")
	t.Setenv("RELAY_CONCURRENCY", "8")
	t.Setenv("RELAY_POLL_TIMEOUT", "2s")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	addr := fs.String("redis-addr", "localhost:6379", "")
	conc := fs.Int("concurrency", 1, "")
	poll := fs.Duration("poll-timeout", 5*time.Second, "")
	if err := Parse(fs, "RELAY", []string{"--concurrency", "3"}); err != nil {
		t.Fatal(err)
	}
	if *addr != "redis:6379" {
		t.Errorf("redis-addr = %q, want env value", *addr)
	}
	if *conc != 3 {
		t.Errorf("concurrency = %d, want flag value 3 to beat env", *conc)
	}
	if *poll != 2*time.Second {
		t.Errorf("poll-timeout = %v, want 2s", *poll)
	}
}

func TestParseInvalidEnv(t *testing.T) {
	t.Setenv("RELAY_CONCURRENCY", "lots")
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Int("concurrency", 1, "")
	if err := Parse(fs, "RELAY", nil); err == nil {
		t.Fatal("expected error for invalid env value")
	}
}
