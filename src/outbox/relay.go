// Package outbox implements the relay half of the transactional outbox
// pattern: a consumer writes its state change and an outbox_events row in
// one transaction (see consumer.ProcessOrderSafe); this relay is the
// separate process that later turns unpublished rows into real queue
// messages.
package outbox

import (
	"context"
	"fmt"

	"github.com/heartlezz7/idempotent/src/queue"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RunOnce publishes up to `limit` unpublished outbox rows and returns how
// many were newly published. Each row is claimed with FOR UPDATE SKIP
// LOCKED and published inside its own transaction, so a relay crash
// between publishing and marking a row published is safe: the downstream
// dedupe_key is derived deterministically from the outbox row's own id
// (never a fresh random token -- the doc's warning about
// "a fresh uuid.New() here would double-charge on retry"), so re-running
// RunOnce after a crash re-publishes to the SAME queue message instead of
// creating a second one.
func RunOnce(ctx context.Context, pool *pgxpool.Pool, limit int) (published int, err error) {
	for i := 0; i < limit; i++ {
		ok, err := relayOne(ctx, pool)
		if err != nil {
			return published, err
		}
		if !ok {
			break
		}
		published++
	}
	return published, nil
}

func relayOne(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var id int64
	var payload []byte
	var topic string
	err = tx.QueryRow(ctx, `
		SELECT id, topic, payload FROM outbox_events
		WHERE NOT published
		ORDER BY id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`).Scan(&id, &topic, &payload)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	dedupeKey := fmt.Sprintf("outbox:%d", id)
	if _, _, err := queue.Publish(ctx, tx, topic, &dedupeKey, payload); err != nil {
		return false, err
	}

	if _, err := tx.Exec(ctx, `UPDATE outbox_events SET published = true WHERE id = $1`, id); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
