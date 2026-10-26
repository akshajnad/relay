// Package redistest provides a real Redis 7 for tests: a testcontainers
// container shared by every test in a package, or an existing server named
// by RELAY_TEST_REDIS_ADDR. The container starts on first use, so tests that
// never touch Redis run without Docker.
package redistest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

const Image = "redis:7.4-alpine"

var (
	once      sync.Once
	addr      string
	startErr  error
	container testcontainers.Container
)

// Main runs the package's tests and terminates the container afterwards.
// Call it from TestMain.
func Main(m *testing.M) {
	code := m.Run()
	if container != nil {
		_ = container.Terminate(context.Background())
	}
	os.Exit(code)
}

func start() {
	if a := os.Getenv("RELAY_TEST_REDIS_ADDR"); a != "" {
		addr = a
		return
	}
	ctx := context.Background()
	c, err := tcredis.Run(ctx, Image)
	if err != nil {
		startErr = fmt.Errorf("start %s (is Docker running?): %w", Image, err)
		return
	}
	container = c
	uri, err := c.ConnectionString(ctx)
	if err != nil {
		startErr = err
		return
	}
	opt, err := redis.ParseURL(uri)
	if err != nil {
		startErr = err
		return
	}
	addr = opt.Addr
}

// Addr returns the address of the shared Redis, starting it if needed.
func Addr(t testing.TB) string {
	t.Helper()
	once.Do(start)
	if startErr != nil {
		t.Fatalf("redistest: %v", startErr)
	}
	return addr
}

// Client returns a client on a freshly flushed database. Tests that use it
// must not run in parallel with each other.
func Client(t testing.TB) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: Addr(t), PoolSize: 64})
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	t.Cleanup(func() { rdb.Close() })
	return rdb
}
