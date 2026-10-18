package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/heartlezz7/idempotent/src/queue"
	"github.com/heartlezz7/idempotent/src/testutil"
)

func mustPayload(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return b
}

func TestPublish_FirstCallCreatesMessage(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id, deduped, err := queue.Publish(ctx, pool, "orders", nil, mustPayload(t, map[string]any{"order_id": "o-1"}))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if deduped {
		t.Fatal("a first-time publish must not be reported as deduped")
	}
	if id == 0 {
		t.Fatal("expected a non-zero message id")
	}
}

func TestPublish_SameDedupeKeySamePayloadReplaysSameMessage(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()
	key := "checkout-42"
	payload := mustPayload(t, map[string]any{"order_id": "o-2", "amount": 5000})

	id1, deduped1, err := queue.Publish(ctx, pool, "orders", &key, payload)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if deduped1 {
		t.Fatal("first publish under a fresh dedupe_key must not be deduped")
	}

	id2, deduped2, err := queue.Publish(ctx, pool, "orders", &key, payload)
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if !deduped2 {
		t.Fatal("retrying with the same dedupe_key and payload must report deduped=true")
	}
	if id1 != id2 {
		t.Fatalf("expected the same message id back, got %d then %d", id1, id2)
	}
}

func TestPublish_SameDedupeKeyDifferentPayloadConflicts(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()
	key := "checkout-43"

	if _, _, err := queue.Publish(ctx, pool, "orders", &key, mustPayload(t, map[string]any{"amount": 5000})); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	_, _, err := queue.Publish(ctx, pool, "orders", &key, mustPayload(t, map[string]any{"amount": 9999}))
	if !errors.Is(err, queue.ErrDedupeConflict) {
		t.Fatalf("expected ErrDedupeConflict for a reused key with a different payload, got %v", err)
	}
}

func TestPublish_KeyReuseAcrossTopicsDoesNotCollide(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()
	key := "shared-key"

	id1, _, err := queue.Publish(ctx, pool, "orders", &key, mustPayload(t, map[string]any{"x": 1}))
	if err != nil {
		t.Fatalf("publish to orders: %v", err)
	}
	id2, deduped, err := queue.Publish(ctx, pool, "notifications", &key, mustPayload(t, map[string]any{"x": 2}))
	if err != nil {
		t.Fatalf("publish to notifications: %v", err)
	}
	if deduped {
		t.Fatal("the same key on a different topic must not be treated as a dedupe replay")
	}
	if id1 == id2 {
		t.Fatal("expected two distinct messages, scoped by topic")
	}
}

func TestPublish_NoDedupeKeyNeverDeduplicates(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()
	payload := mustPayload(t, map[string]any{"order_id": "o-4"})

	id1, _, err := queue.Publish(ctx, pool, "orders", nil, payload)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	id2, deduped, err := queue.Publish(ctx, pool, "orders", nil, payload)
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if deduped {
		t.Fatal("fire-and-forget publishes (no dedupe_key) must never report deduped=true")
	}
	if id1 == id2 {
		t.Fatal("expected two distinct messages when no dedupe_key was supplied")
	}
}

func TestClaim_ExactlyOneWinnerUnderConcurrency(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	if _, _, err := queue.Publish(ctx, pool, "race", nil, mustPayload(t, map[string]any{"x": 1})); err != nil {
		t.Fatalf("publish: %v", err)
	}

	const workers = 12
	var wg sync.WaitGroup
	won := make([]bool, workers)
	for i := range won {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			msg, err := queue.Claim(ctx, pool, "race", fmt.Sprintf("worker-%d", i), 30)
			if err != nil {
				t.Errorf("claim from worker %d: %v", i, err)
				return
			}
			won[i] = msg != nil
		}(i)
	}
	wg.Wait()

	winners := 0
	for _, w := range won {
		if w {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("expected exactly one of %d concurrent claimants to win the single message, got %d", workers, winners)
	}
}

func TestClaim_NothingAvailableReturnsNilNotError(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	msg, err := queue.Claim(ctx, pool, "empty-topic", "worker", 30)
	if err != nil {
		t.Fatalf("claim on an empty topic should not error: %v", err)
	}
	if msg != nil {
		t.Fatalf("expected no message, got %+v", msg)
	}
}

func TestClaim_ExpiredLeaseCanBeStolen(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id, _, err := queue.Publish(ctx, pool, "orders", nil, mustPayload(t, map[string]any{"x": 1}))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := queue.Claim(ctx, pool, "orders", "owner-a", 30); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	// Immediately after a claim, the lease is live -- nobody else can take it.
	if msg, err := queue.Claim(ctx, pool, "orders", "owner-b", 30); err != nil {
		t.Fatalf("second claim: %v", err)
	} else if msg != nil {
		t.Fatal("a live lease must not be stealable")
	}

	if err := queue.ForceExpireLease(ctx, pool, id); err != nil {
		t.Fatalf("force expire lease: %v", err)
	}

	msg, err := queue.Claim(ctx, pool, "orders", "owner-c", 30)
	if err != nil {
		t.Fatalf("claim after expiry: %v", err)
	}
	if msg == nil || msg.ID != id {
		t.Fatalf("expected to steal message %d after its lease expired, got %+v", id, msg)
	}
}

func TestAck_MarksMessageDone(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id, _, err := queue.Publish(ctx, pool, "orders", nil, mustPayload(t, map[string]any{"x": 1}))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := queue.Claim(ctx, pool, "orders", "owner", 30); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := queue.Ack(ctx, pool, id); err != nil {
		t.Fatalf("ack: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, `SELECT status FROM queue_messages WHERE id = $1`, id).Scan(&status); err != nil {
		t.Fatalf("query status: %v", err)
	}
	if status != "done" {
		t.Fatalf("expected status 'done' after ack, got %q", status)
	}
}

func TestRelease_ReturnsMessageToPendingImmediately(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id, _, err := queue.Publish(ctx, pool, "orders", nil, mustPayload(t, map[string]any{"x": 1}))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := queue.Claim(ctx, pool, "orders", "owner", 30); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := queue.Release(ctx, pool, id); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Released without waiting out any lease: immediately claimable again.
	msg, err := queue.Claim(ctx, pool, "orders", "owner-2", 30)
	if err != nil {
		t.Fatalf("claim after release: %v", err)
	}
	if msg == nil || msg.ID != id {
		t.Fatalf("expected to reclaim released message %d, got %+v", id, msg)
	}
}

func TestForceRedeliver_MakesADoneMessageClaimableAgain(t *testing.T) {
	pool := testutil.OpenPool(t)
	testutil.Reset(t, pool)
	ctx := context.Background()

	id, _, err := queue.Publish(ctx, pool, "orders", nil, mustPayload(t, map[string]any{"x": 1}))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := queue.Claim(ctx, pool, "orders", "owner", 30); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := queue.Ack(ctx, pool, id); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// A done message is not normally claimable.
	if msg, err := queue.Claim(ctx, pool, "orders", "owner-2", 30); err != nil {
		t.Fatalf("claim after ack: %v", err)
	} else if msg != nil {
		t.Fatal("an acked message must not be claimable without a forced redelivery")
	}

	if err := queue.ForceRedeliver(ctx, pool, id); err != nil {
		t.Fatalf("force redeliver: %v", err)
	}

	msg, err := queue.Claim(ctx, pool, "orders", "owner-3", 30)
	if err != nil {
		t.Fatalf("claim after forced redelivery: %v", err)
	}
	if msg == nil || msg.ID != id {
		t.Fatalf("expected forced redelivery to make message %d claimable again, got %+v", id, msg)
	}
}
