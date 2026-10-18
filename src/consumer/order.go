package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/heartlezz7/idempotent/src/model"
	"github.com/heartlezz7/idempotent/src/queue"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OrderPayload is the body of a message on the "orders" topic.
type OrderPayload struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
	Version int64  `json:"version"`
}

// NotificationPayload is the body of a message on the "notifications"
// topic, always produced by the transactional outbox below.
type NotificationPayload struct {
	OrderID string `json:"order_id"`
	Body    string `json:"body"`
}

// ProcessOrderNaive applies every delivery unconditionally: no monotonic
// guard, no dedup check. A redelivered or out-of-order message still
// writes an audit_log row, so replaying the same message N times produces
// N rows instead of one -- the observable duplicate this lab exists to
// catch.
func ProcessOrderNaive(ctx context.Context, pool *pgxpool.Pool, msg *model.Message) (Outcome, error) {
	var p OrderPayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return Outcome{}, err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return Outcome{}, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO orders (id, status, version, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (id) DO UPDATE
		   SET status = EXCLUDED.status, version = EXCLUDED.version, updated_at = now()`,
		p.OrderID, p.Status, p.Version); err != nil {
		return Outcome{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_log (consumer, op, message_id, resource_id)
		VALUES ('naive', 'order.apply', $1, $2)`,
		msg.ID, p.OrderID); err != nil {
		return Outcome{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Outcome{}, err
	}
	if err := queue.Ack(ctx, pool, msg.ID); err != nil {
		return Outcome{}, err
	}
	return Outcome{Consumer: "naive", Resource: "order", MessageID: msg.ID, Applied: true, Acked: true,
		Detail: "applied unconditionally, no guard"}, nil
}

// ProcessOrderSafe applies the effect through a single monotonic-guarded
// upsert -- "WHERE orders.version < EXCLUDED.version". That one clause is
// what the doc calls natural idempotency: no dedup table is needed
// because a duplicate or out-of-order delivery simply fails the guard
// and is skipped, correctly, with no error.
//
// When the guard passes, it writes an outbox_events row in the SAME
// transaction as the order update (the transactional outbox), so the
// eventual notification is exactly as durable as the state change that
// triggered it -- never sent for a change that got rolled back, never
// silently dropped for one that committed.
func ProcessOrderSafe(ctx context.Context, pool *pgxpool.Pool, msg *model.Message, chaos Chaos) (Outcome, error) {
	var p OrderPayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return Outcome{}, err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return Outcome{}, err
	}
	defer tx.Rollback(ctx)

	var appliedID string
	err = tx.QueryRow(ctx, `
		INSERT INTO orders (id, status, version, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (id) DO UPDATE
		   SET status = EXCLUDED.status, version = EXCLUDED.version, updated_at = now()
		 WHERE orders.version < EXCLUDED.version
		RETURNING id`,
		p.OrderID, p.Status, p.Version).Scan(&appliedID)

	if errors.Is(err, pgx.ErrNoRows) {
		// The guard rejected it: this version was already applied, or it
		// arrived out of order behind a newer one. Correctly a no-op --
		// commit nothing, but the message WAS handled, so ack it.
		if err := tx.Commit(ctx); err != nil {
			return Outcome{}, err
		}
		if err := queue.Ack(ctx, pool, msg.ID); err != nil {
			return Outcome{}, err
		}
		return Outcome{Consumer: "safe", Resource: "order", MessageID: msg.ID, Applied: false, Acked: true,
			Detail: fmt.Sprintf("version guard rejected version=%d (stale or duplicate delivery)", p.Version)}, nil
	}
	if err != nil {
		return Outcome{}, err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_log (consumer, op, message_id, resource_id)
		VALUES ('safe', 'order.apply', $1, $2)`,
		msg.ID, p.OrderID); err != nil {
		return Outcome{}, err
	}

	np := NotificationPayload{OrderID: p.OrderID, Body: fmt.Sprintf("order %s is now %s", p.OrderID, p.Status)}
	npBytes, err := json.Marshal(np)
	if err != nil {
		return Outcome{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (topic, payload) VALUES ('notifications', $1::jsonb)`,
		string(npBytes)); err != nil {
		return Outcome{}, err
	}

	if chaos == ChaosCrashBeforeCommit {
		// Simulate the process dying mid-transaction. The deferred
		// Rollback undoes the order update AND the outbox row together:
		// there is no half-done state. The message stays leased; once its
		// lease expires (or the lab forces that), a retry re-executes
		// cleanly, because nothing committed the first time.
		return Outcome{Consumer: "safe", Resource: "order", MessageID: msg.ID, Applied: false, Acked: false,
				Detail: "crashed before commit: rolled back, message still leased for redelivery"},
			ErrChaosCrashBeforeCommit
	}

	if err := tx.Commit(ctx); err != nil {
		return Outcome{}, err
	}

	if chaos == ChaosCrashAfterCommitNoAck {
		// The order update and the outbox row are durable, but we
		// deliberately skip Ack -- simulating the process dying after
		// COMMIT but before telling the broker. The broker will
		// eventually redeliver this message; the version guard above
		// makes that redelivery a safe no-op, not a double effect.
		return Outcome{Consumer: "safe", Resource: "order", MessageID: msg.ID, Applied: true, Acked: false,
			Detail: "committed but not acked: redelivery will be rejected by the version guard"}, nil
	}

	if err := queue.Ack(ctx, pool, msg.ID); err != nil {
		return Outcome{}, err
	}
	return Outcome{Consumer: "safe", Resource: "order", MessageID: msg.ID, Applied: true, Acked: true,
		Detail: "applied, outbox event queued, acked"}, nil
}
