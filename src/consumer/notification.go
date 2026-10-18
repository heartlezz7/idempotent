package consumer

import (
	"context"
	"encoding/json"

	"github.com/heartlezz7/idempotent/src/model"
	"github.com/heartlezz7/idempotent/src/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

// InboxConsumerName scopes inbox_processed rows to this consumer group,
// the same way the HTTP branch scopes an idempotency key by
// (tenant_id, endpoint) rather than a bare key.
const InboxConsumerName = "notification-consumer"

// ProcessNotificationNaive sends unconditionally: no inbox check. Unlike
// an order status update, "send a notification" has no assignment form
// that makes doing it twice equal to doing it once -- every redelivery is
// a real duplicate send, which is exactly why this resource is the one
// that needs the inbox pattern.
func ProcessNotificationNaive(ctx context.Context, pool *pgxpool.Pool, msg *model.Message) (Outcome, error) {
	var p NotificationPayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return Outcome{}, err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return Outcome{}, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO notifications_sent (order_id, message_id, body) VALUES ($1, $2, $3)`,
		p.OrderID, msg.ID, p.Body); err != nil {
		return Outcome{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_log (consumer, op, message_id, resource_id)
		VALUES ('naive', 'notification.send', $1, $2)`,
		msg.ID, p.OrderID); err != nil {
		return Outcome{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Outcome{}, err
	}
	if err := queue.Ack(ctx, pool, msg.ID); err != nil {
		return Outcome{}, err
	}
	return Outcome{Consumer: "naive", Resource: "notification", MessageID: msg.ID, Applied: true, Acked: true,
		Detail: "sent unconditionally, no dedup"}, nil
}

// ProcessNotificationSafe is the inbox pattern from the doc's "Beyond
// HTTP" section, verbatim: claim the message identity in the same
// transaction as the effect via
// "INSERT INTO inbox_processed ... ON CONFLICT DO NOTHING". Zero rows
// inserted means this message was already handled -- skip, and ack,
// because a correctly-detected duplicate is not a failure.
func ProcessNotificationSafe(ctx context.Context, pool *pgxpool.Pool, msg *model.Message, chaos Chaos) (Outcome, error) {
	var p NotificationPayload
	if err := json.Unmarshal(msg.Payload, &p); err != nil {
		return Outcome{}, err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return Outcome{}, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
		INSERT INTO inbox_processed (consumer, message_id) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`,
		InboxConsumerName, msg.ID)
	if err != nil {
		return Outcome{}, err
	}
	if tag.RowsAffected() == 0 {
		// Already processed under this identity. No effect, no new send --
		// this is the inbox pattern doing its job.
		if err := tx.Commit(ctx); err != nil {
			return Outcome{}, err
		}
		if err := queue.Ack(ctx, pool, msg.ID); err != nil {
			return Outcome{}, err
		}
		return Outcome{Consumer: "safe", Resource: "notification", MessageID: msg.ID, Applied: false, Acked: true,
			Detail: "inbox already has this message_id: skipped, not re-sent"}, nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO notifications_sent (order_id, message_id, body) VALUES ($1, $2, $3)`,
		p.OrderID, msg.ID, p.Body); err != nil {
		return Outcome{}, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_log (consumer, op, message_id, resource_id)
		VALUES ('safe', 'notification.send', $1, $2)`,
		msg.ID, p.OrderID); err != nil {
		return Outcome{}, err
	}

	if chaos == ChaosCrashBeforeCommit {
		// Rollback undoes the inbox claim AND the send together. The
		// message stays leased for redelivery, and because the inbox
		// claim never committed, the retry is free to actually send.
		return Outcome{Consumer: "safe", Resource: "notification", MessageID: msg.ID, Applied: false, Acked: false,
				Detail: "crashed before commit: rolled back, message still leased for redelivery"},
			ErrChaosCrashBeforeCommit
	}

	if err := tx.Commit(ctx); err != nil {
		return Outcome{}, err
	}

	if chaos == ChaosCrashAfterCommitNoAck {
		// The send and the inbox row are both durable, but Ack is
		// deliberately skipped. On redelivery the inbox check above finds
		// its row already present and skips -- one send, not two.
		return Outcome{Consumer: "safe", Resource: "notification", MessageID: msg.ID, Applied: true, Acked: false,
			Detail: "committed but not acked: redelivery will be caught by the inbox"}, nil
	}

	if err := queue.Ack(ctx, pool, msg.ID); err != nil {
		return Outcome{}, err
	}
	return Outcome{Consumer: "safe", Resource: "notification", MessageID: msg.ID, Applied: true, Acked: true,
		Detail: "sent, inbox recorded, acked"}, nil
}
