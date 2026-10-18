package outbox_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/heartlezz7/idempotent/src/consumer"
	"github.com/heartlezz7/idempotent/src/outbox"
	"github.com/heartlezz7/idempotent/src/queue"
	"github.com/heartlezz7/idempotent/src/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

// applyOrder pushes an order through the safe consumer so it lands one
// pending outbox_events row, the same way the real pipeline produces one.
func applyOrder(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orderID string) {
	t.Helper()
	payload, err := json.Marshal(consumer.OrderPayload{OrderID: orderID, Status: "paid", Version: 1})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, _, err := queue.Publish(ctx, pool, "orders", nil, payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
	msg, err := queue.Claim(ctx, pool, "orders", "worker", 30)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := consumer.ProcessOrderSafe(ctx, pool, msg, consumer.ChaosNone); err != nil {
		t.Fatalf("process order: %v", err)
	}
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return n
}

func TestRunOnce_PublishesPendingOutboxRowsAsQueueMessages(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	applyOrder(t, ctx, pool, "order-relay-1")
	if got := countRows(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE NOT published`); got != 1 {
		t.Fatalf("expected 1 pending outbox row before relaying, got %d", got)
	}

	n, err := outbox.RunOnce(ctx, pool, 10)
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 event published, got %d", n)
	}

	if got := countRows(t, ctx, pool, `SELECT count(*) FROM outbox_events WHERE published`); got != 1 {
		t.Fatalf("expected the outbox row to be marked published, got %d rows", got)
	}
	if got := countRows(t, ctx, pool, `SELECT count(*) FROM queue_messages WHERE topic = 'notifications'`); got != 1 {
		t.Fatalf("expected exactly one notifications message on the queue, got %d", got)
	}
}

func TestRunOnce_UsesADeterministicDedupeKeyDerivedFromTheEventID(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	applyOrder(t, ctx, pool, "order-relay-2")
	var eventID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM outbox_events LIMIT 1`).Scan(&eventID); err != nil {
		t.Fatalf("select outbox event id: %v", err)
	}

	if _, err := outbox.RunOnce(ctx, pool, 10); err != nil {
		t.Fatalf("run once: %v", err)
	}

	var dedupeKey string
	err := pool.QueryRow(ctx, `SELECT dedupe_key FROM queue_messages WHERE topic = 'notifications' LIMIT 1`).Scan(&dedupeKey)
	if err != nil {
		t.Fatalf("select dedupe_key: %v", err)
	}
	want := "outbox:" + strconv.FormatInt(eventID, 10)
	if dedupeKey != want {
		t.Fatalf("expected dedupe_key %q derived from the event id, got %q", want, dedupeKey)
	}
}

func TestRunOnce_RepublishingAfterAMissedPublishedFlagDoesNotDuplicate(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	applyOrder(t, ctx, pool, "order-relay-3")
	if _, err := outbox.RunOnce(ctx, pool, 10); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if got := countRows(t, ctx, pool, `SELECT count(*) FROM queue_messages WHERE topic = 'notifications'`); got != 1 {
		t.Fatalf("expected 1 message after the first relay run, got %d", got)
	}

	// Simulate a relay crash between publishing and marking the row
	// published: flip it back to unpublished, then run the relay again.
	if _, err := pool.Exec(ctx, `UPDATE outbox_events SET published = false`); err != nil {
		t.Fatalf("simulate crash (reset published flag): %v", err)
	}
	n, err := outbox.RunOnce(ctx, pool, 10)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected the second run to report 1 (re-)published event, got %d", n)
	}

	// The deterministic dedupe_key means Publish deduped the retry instead
	// of creating a second queue message.
	if got := countRows(t, ctx, pool, `SELECT count(*) FROM queue_messages WHERE topic = 'notifications'`); got != 1 {
		t.Fatalf("expected still exactly 1 notifications message after the simulated crash+retry, got %d", got)
	}
}

func TestRunOnce_NothingPendingIsANoOp(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	n, err := outbox.RunOnce(ctx, pool, 10)
	if err != nil {
		t.Fatalf("run once on an empty outbox: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 published, got %d", n)
	}
}
