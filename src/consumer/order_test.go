package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/heartlezz7/idempotent/src/consumer"
	"github.com/heartlezz7/idempotent/src/model"
	"github.com/heartlezz7/idempotent/src/queue"
	"github.com/heartlezz7/idempotent/src/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

func publishOrder(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orderID, status string, version int64) int64 {
	t.Helper()
	payload, err := json.Marshal(consumer.OrderPayload{OrderID: orderID, Status: status, Version: version})
	if err != nil {
		t.Fatalf("marshal order payload: %v", err)
	}
	id, _, err := queue.Publish(ctx, pool, "orders", nil, payload)
	if err != nil {
		t.Fatalf("publish order: %v", err)
	}
	return id
}

func claim(t *testing.T, ctx context.Context, pool *pgxpool.Pool, topic string) *model.Message {
	t.Helper()
	msg, err := queue.Claim(ctx, pool, topic, "test-worker", 30)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if msg == nil {
		t.Fatalf("expected a claimable message on topic %q, got none", topic)
	}
	return msg
}

func auditCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, consumerName, op string) int64 {
	t.Helper()
	var n int64
	err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_log WHERE consumer = $1 AND op = $2`, consumerName, op).Scan(&n)
	if err != nil {
		t.Fatalf("count audit_log: %v", err)
	}
	return n
}

func TestProcessOrderNaive_RedeliveryDuplicatesTheEffect(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id := publishOrder(t, ctx, pool, "order-naive-1", "paid", 1)
	for i := 0; i < 3; i++ {
		msg := claim(t, ctx, pool, "orders")
		if _, err := consumer.ProcessOrderNaive(ctx, pool, msg); err != nil {
			t.Fatalf("process (delivery %d): %v", i, err)
		}
		if err := queue.ForceRedeliver(ctx, pool, id); err != nil {
			t.Fatalf("force redeliver: %v", err)
		}
	}

	if got := auditCount(t, ctx, pool, "naive", "order.apply"); got != 3 {
		t.Fatalf("naive consumer: expected 3 audit rows for 3 deliveries, got %d", got)
	}
}

func TestProcessOrderSafe_MonotonicGuardAbsorbsRedelivery(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id := publishOrder(t, ctx, pool, "order-safe-1", "paid", 1)
	for i := 0; i < 3; i++ {
		msg := claim(t, ctx, pool, "orders")
		if _, err := consumer.ProcessOrderSafe(ctx, pool, msg, consumer.ChaosNone); err != nil {
			t.Fatalf("process (delivery %d): %v", i, err)
		}
		if err := queue.ForceRedeliver(ctx, pool, id); err != nil {
			t.Fatalf("force redeliver: %v", err)
		}
	}

	if got := auditCount(t, ctx, pool, "safe", "order.apply"); got != 1 {
		t.Fatalf("safe consumer: expected exactly 1 audit row despite 3 deliveries, got %d", got)
	}
}

func TestProcessOrderSafe_StaleAndOutOfOrderVersionsAreNoOps(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	publishOrder(t, ctx, pool, "order-safe-2", "shipped", 5)
	msg := claim(t, ctx, pool, "orders")
	outcome, err := consumer.ProcessOrderSafe(ctx, pool, msg, consumer.ChaosNone)
	if err != nil {
		t.Fatalf("process v5: %v", err)
	}
	if !outcome.Applied {
		t.Fatalf("the first delivery for an order must apply, got %+v", outcome)
	}

	publishOrder(t, ctx, pool, "order-safe-2", "paid", 2) // stale, arrives late
	msg = claim(t, ctx, pool, "orders")
	outcome, err = consumer.ProcessOrderSafe(ctx, pool, msg, consumer.ChaosNone)
	if err != nil {
		t.Fatalf("process stale v2: %v", err)
	}
	if outcome.Applied {
		t.Fatalf("a version older than what's stored must be rejected as a no-op, got %+v", outcome)
	}
	if !outcome.Acked {
		t.Fatal("a correctly-detected stale version is not a failure and must still be acked")
	}

	if got := auditCount(t, ctx, pool, "safe", "order.apply"); got != 1 {
		t.Fatalf("expected exactly 1 audit row (only v5 applied), got %d", got)
	}
}

func TestProcessOrderSafe_AppliedWriteQueuesOutboxEvent(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	publishOrder(t, ctx, pool, "order-safe-3", "paid", 1)
	msg := claim(t, ctx, pool, "orders")
	if _, err := consumer.ProcessOrderSafe(ctx, pool, msg, consumer.ChaosNone); err != nil {
		t.Fatalf("process: %v", err)
	}

	var pending int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE NOT published`).Scan(&pending); err != nil {
		t.Fatalf("count outbox_events: %v", err)
	}
	if pending != 1 {
		t.Fatalf("expected exactly one pending outbox event, got %d", pending)
	}
}

func TestProcessOrderSafe_ChaosCrashBeforeCommitRollsBackCleanly(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id := publishOrder(t, ctx, pool, "order-crash-1", "paid", 1)
	msg := claim(t, ctx, pool, "orders")
	outcome, err := consumer.ProcessOrderSafe(ctx, pool, msg, consumer.ChaosCrashBeforeCommit)
	if !errors.Is(err, consumer.ErrChaosCrashBeforeCommit) {
		t.Fatalf("expected ErrChaosCrashBeforeCommit, got %v", err)
	}
	if outcome.Applied || outcome.Acked {
		t.Fatalf("a rolled-back attempt must report Applied=false, Acked=false, got %+v", outcome)
	}

	var orders int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&orders); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	if orders != 0 {
		t.Fatalf("rollback must leave no order row behind, found %d", orders)
	}
	var outboxRows int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&outboxRows); err != nil {
		t.Fatalf("count outbox_events: %v", err)
	}
	if outboxRows != 0 {
		t.Fatalf("rollback must undo the outbox write too, found %d rows", outboxRows)
	}

	// The message is still leased; force the lease to expire and retry.
	if err := queue.ForceExpireLease(ctx, pool, id); err != nil {
		t.Fatalf("force expire lease: %v", err)
	}
	retryMsg := claim(t, ctx, pool, "orders")
	if retryMsg.ID != id {
		t.Fatalf("expected to reclaim the same message %d, got %d", id, retryMsg.ID)
	}
	outcome, err = consumer.ProcessOrderSafe(ctx, pool, retryMsg, consumer.ChaosNone)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !outcome.Applied {
		t.Fatalf("retry after the rollback must apply cleanly, got %+v", outcome)
	}
	if got := auditCount(t, ctx, pool, "safe", "order.apply"); got != 1 {
		t.Fatalf("expected exactly 1 audit row after crash+retry, got %d", got)
	}
}

func TestProcessOrderSafe_ChaosCrashAfterCommitRedeliveryIsSafeNoOp(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id := publishOrder(t, ctx, pool, "order-crash-2", "paid", 1)
	msg := claim(t, ctx, pool, "orders")
	outcome, err := consumer.ProcessOrderSafe(ctx, pool, msg, consumer.ChaosCrashAfterCommitNoAck)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if !outcome.Applied {
		t.Fatal("the effect must be durable even though ack was skipped")
	}
	if outcome.Acked {
		t.Fatal("ChaosCrashAfterCommitNoAck must not ack")
	}

	if err := queue.ForceRedeliver(ctx, pool, id); err != nil {
		t.Fatalf("force redeliver: %v", err)
	}
	redeliveredMsg := claim(t, ctx, pool, "orders")
	outcome, err = consumer.ProcessOrderSafe(ctx, pool, redeliveredMsg, consumer.ChaosNone)
	if err != nil {
		t.Fatalf("process redelivery: %v", err)
	}
	if outcome.Applied {
		t.Fatal("redelivery of an already-committed change must be rejected by the version guard")
	}

	if got := auditCount(t, ctx, pool, "safe", "order.apply"); got != 1 {
		t.Fatalf("expected exactly 1 audit row despite the redelivery, got %d", got)
	}
}
