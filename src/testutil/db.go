// Package testutil is shared test scaffolding, not itself a test file, so
// it can be imported from _test.go files across every package in this
// module. Tests need a real Postgres with the lab schema already applied
// (`make up`) -- the mechanisms under test (atomic claims, transactional
// commits, unique constraints) are exactly the things a mock would paper
// over.
package testutil

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultTestDSN = "postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"

// OpenPool connects to the test database (TEST_DATABASE_URL, or the
// docker-compose default) and registers cleanup. Tests skip rather than
// fail when the database is unreachable, so `go test ./...` stays usable
// without `make up` -- it just reports what it couldn't run.
func OpenPool(t testing.TB) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultTestDSN
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("test db unavailable (%v) -- run `make up` first", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("test db unavailable (%v) -- run `make up` first", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// Reset truncates every lab table so each test starts from empty, exactly
// what the /_reset endpoint does for manual experiments.
func Reset(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		TRUNCATE queue_messages, inbox_processed, orders, notifications_sent,
		         outbox_events, audit_log RESTART IDENTITY`)
	if err != nil {
		t.Fatalf("reset test db: %v", err)
	}
}
