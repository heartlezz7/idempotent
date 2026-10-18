package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/heartlezz7/idempotent/src/consumer"
	"github.com/heartlezz7/idempotent/src/queue"
	"github.com/heartlezz7/idempotent/src/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

func publishNotification(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orderID, body string) int64 {
	t.Helper()
	payload, err := json.Marshal(consumer.NotificationPayload{OrderID: orderID, Body: body})
	if err != nil {
		t.Fatalf("marshal notification payload: %v", err)
	}
	id, _, err := queue.Publish(ctx, pool, "notifications", nil, payload)
	if err != nil {
		t.Fatalf("publish notification: %v", err)
	}
	return id
}

func sentCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM notifications_sent`).Scan(&n); err != nil {
		t.Fatalf("count notifications_sent: %v", err)
	}
	return n
}

func TestProcessNotificationNaive_RedeliveryDuplicatesTheSend(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id := publishNotification(t, ctx, pool, "order-n1", "hello")
	for i := 0; i < 2; i++ {
		msg := claim(t, ctx, pool, "notifications")
		if _, err := consumer.ProcessNotificationNaive(ctx, pool, msg); err != nil {
			t.Fatalf("process (delivery %d): %v", i, err)
		}
		if err := queue.ForceRedeliver(ctx, pool, id); err != nil {
			t.Fatalf("force redeliver: %v", err)
		}
	}

	if got := sentCount(t, ctx, pool); got != 2 {
		t.Fatalf("naive notification consumer: expected 2 sends for 2 deliveries, got %d", got)
	}
}

func TestProcessNotificationSafe_InboxAbsorbsRedelivery(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id := publishNotification(t, ctx, pool, "order-n2", "hello")
	for i := 0; i < 3; i++ {
		msg := claim(t, ctx, pool, "notifications")
		outcome, err := consumer.ProcessNotificationSafe(ctx, pool, msg, consumer.ChaosNone)
		if err != nil {
			t.Fatalf("process (delivery %d): %v", i, err)
		}
		if i == 0 && !outcome.Applied {
			t.Fatalf("the first delivery must send, got %+v", outcome)
		}
		if i > 0 && outcome.Applied {
			t.Fatalf("redeliveries must be absorbed by the inbox, got %+v", outcome)
		}
		if err := queue.ForceRedeliver(ctx, pool, id); err != nil {
			t.Fatalf("force redeliver: %v", err)
		}
	}

	if got := sentCount(t, ctx, pool); got != 1 {
		t.Fatalf("safe notification consumer: expected exactly 1 send despite 3 deliveries, got %d", got)
	}

	var inboxRows int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM inbox_processed WHERE consumer = $1`, consumer.InboxConsumerName).
		Scan(&inboxRows); err != nil {
		t.Fatalf("count inbox_processed: %v", err)
	}
	if inboxRows != 1 {
		t.Fatalf("expected exactly one inbox row for the one message id, got %d", inboxRows)
	}
}

func TestProcessNotificationSafe_ChaosCrashBeforeCommitRollsBackTheInboxClaimToo(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id := publishNotification(t, ctx, pool, "order-n3", "hello")
	msg := claim(t, ctx, pool, "notifications")
	_, err := consumer.ProcessNotificationSafe(ctx, pool, msg, consumer.ChaosCrashBeforeCommit)
	if !errors.Is(err, consumer.ErrChaosCrashBeforeCommit) {
		t.Fatalf("expected ErrChaosCrashBeforeCommit, got %v", err)
	}
	if got := sentCount(t, ctx, pool); got != 0 {
		t.Fatalf("rollback must leave no send behind, found %d", got)
	}

	if err := queue.ForceExpireLease(ctx, pool, id); err != nil {
		t.Fatalf("force expire lease: %v", err)
	}
	retryMsg := claim(t, ctx, pool, "notifications")
	outcome, err := consumer.ProcessNotificationSafe(ctx, pool, retryMsg, consumer.ChaosNone)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if !outcome.Applied {
		t.Fatalf("retry after rollback must send exactly once, got %+v", outcome)
	}
	if got := sentCount(t, ctx, pool); got != 1 {
		t.Fatalf("expected exactly 1 send after crash+retry, got %d", got)
	}
}

func TestProcessNotificationSafe_ChaosCrashAfterCommitRedeliveryIsSafeNoOp(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id := publishNotification(t, ctx, pool, "order-n4", "hello")
	msg := claim(t, ctx, pool, "notifications")
	outcome, err := consumer.ProcessNotificationSafe(ctx, pool, msg, consumer.ChaosCrashAfterCommitNoAck)
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if !outcome.Applied || outcome.Acked {
		t.Fatalf("expected Applied=true, Acked=false, got %+v", outcome)
	}

	if err := queue.ForceRedeliver(ctx, pool, id); err != nil {
		t.Fatalf("force redeliver: %v", err)
	}
	redeliveredMsg := claim(t, ctx, pool, "notifications")
	outcome, err = consumer.ProcessNotificationSafe(ctx, pool, redeliveredMsg, consumer.ChaosNone)
	if err != nil {
		t.Fatalf("process redelivery: %v", err)
	}
	if outcome.Applied {
		t.Fatal("the inbox must catch the redelivery of an already-sent notification")
	}
	if got := sentCount(t, ctx, pool); got != 1 {
		t.Fatalf("expected exactly 1 send despite the redelivery, got %d", got)
	}
}
