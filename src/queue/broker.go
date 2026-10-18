// Package queue is a single-node, Postgres-backed stand-in for a message
// broker. It exists to give the consumer packages something to be
// idempotent against: at-least-once delivery, exactly like a Kafka
// consumer surviving a rebalance or an SQS message outliving its
// visibility timeout.
package queue

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"

	"github.com/heartlezz7/idempotent/src/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is satisfied by both *pgxpool.Pool and pgx.Tx, so Publish can be
// called either standalone (an external producer) or from inside a
// consumer's transaction (the transactional outbox relay).
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

var ErrDedupeConflict = errors.New("dedupe_key reused with a different payload")

// canonicalize re-marshals JSON through a generic interface{}, which
// sorts object keys at every level (encoding/json always emits map keys
// sorted). Cheap stand-in for the doc's "sort keys, normalise numbers"
// canonicalization step -- enough to make key ordering irrelevant to the
// hash without pulling in a JSON-canonicalization library.
func canonicalize(payload []byte) ([]byte, error) {
	var v any
	if err := json.Unmarshal(payload, &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func hashPayload(canonical []byte) []byte {
	sum := sha256.Sum256(canonical)
	return sum[:]
}

// Publish writes a message for a topic. When dedupeKey is non-nil it is
// the producer-side idempotency knob -- the analogue of Kafka's idempotent
// producer sequence numbers or SQS FIFO's MessageDeduplicationId.
// Uniqueness is scoped to (topic, dedupe_key): a retrying producer that
// reuses the same key gets the SAME message back (deduped=true) instead of
// a second row. Reusing the key for genuinely different content returns
// ErrDedupeConflict -- the queue-side analogue of a 422 on a mismatched
// HTTP fingerprint.
func Publish(ctx context.Context, db Querier, topic string, dedupeKey *string, payload []byte) (id int64, deduped bool, err error) {
	canon, err := canonicalize(payload)
	if err != nil {
		return 0, false, err
	}
	hash := hashPayload(canon)

	err = db.QueryRow(ctx, `
		INSERT INTO queue_messages (topic, dedupe_key, payload, payload_hash)
		VALUES ($1, $2, $3::jsonb, $4)
		ON CONFLICT (topic, dedupe_key) WHERE dedupe_key IS NOT NULL DO NOTHING
		RETURNING id`,
		topic, dedupeKey, string(canon), hash).Scan(&id)

	if err == nil {
		return id, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, err
	}
	if dedupeKey == nil {
		return 0, false, err
	}

	// Conflict: someone already published under this (topic, dedupe_key).
	var existingID int64
	var existingHash []byte
	err = db.QueryRow(ctx, `
		SELECT id, payload_hash FROM queue_messages
		WHERE topic = $1 AND dedupe_key = $2`,
		topic, *dedupeKey).Scan(&existingID, &existingHash)
	if err != nil {
		return 0, false, err
	}
	if string(existingHash) != string(hash) {
		return 0, false, ErrDedupeConflict
	}
	return existingID, true, nil
}

// Claim atomically leases one pending-or-lease-expired message for a
// topic. The FOR UPDATE SKIP LOCKED subquery plus the outer UPDATE form a
// SINGLE statement -- this is the queue world's version of the doc's rule
// "claim in one atomic statement, never SELECT then INSERT". Two workers
// racing here can never claim the same row: one wins, the other's
// subquery skips the locked row and finds nothing (or the next message).
func Claim(ctx context.Context, db Querier, topic, owner string, leaseSeconds int) (*model.Message, error) {
	row := db.QueryRow(ctx, `
		UPDATE queue_messages
		SET status = 'leased',
		    lease_owner = $1,
		    lease_expires_at = now() + ($2 * interval '1 second'),
		    attempts = attempts + 1
		WHERE id = (
			SELECT id FROM queue_messages
			WHERE topic = $3
			  AND available_at <= now()
			  AND (status = 'pending' OR (status = 'leased' AND lease_expires_at < now()))
			ORDER BY id
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, topic, dedupe_key, payload, payload_hash, status, attempts,
		          lease_owner, lease_expires_at, available_at, created_at`,
		owner, leaseSeconds, topic)

	var m model.Message
	err := row.Scan(&m.ID, &m.Topic, &m.DedupeKey, &m.Payload, &m.PayloadHash, &m.Status,
		&m.Attempts, &m.LeaseOwner, &m.LeaseExpiresAt, &m.AvailableAt, &m.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil // nothing to claim right now
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// Ack marks a claimed message done. Call it only after the consumer's
// effect has committed -- acking first and committing after would
// reintroduce the exact ambiguity idempotent processing exists to close.
func Ack(ctx context.Context, db Querier, id int64) error {
	_, err := db.Exec(ctx, `UPDATE queue_messages SET status = 'done' WHERE id = $1`, id)
	return err
}

// Release returns a message to pending immediately, for a transient
// failure where the consumer wants a fast retry rather than waiting out
// the lease. Mirrors the HTTP middleware's rule: release on 5xx, never
// save a failure as if it were a completed claim.
func Release(ctx context.Context, db Querier, id int64) error {
	_, err := db.Exec(ctx, `
		UPDATE queue_messages
		SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL
		WHERE id = $1`, id)
	return err
}

// ForceRedeliver is lab-only: it makes a message available again
// regardless of its current status, including 'done'. Simulates a broker
// redelivering a message the consumer already fully processed -- an acked
// offset that never reached the broker, or a rebalance replay. This is
// the direct test of consumer-side idempotency: the message layer offers
// no protection here, the CONSUMER must not double-apply the effect.
func ForceRedeliver(ctx context.Context, db Querier, id int64) error {
	_, err := db.Exec(ctx, `
		UPDATE queue_messages
		SET status = 'pending', lease_owner = NULL, lease_expires_at = NULL
		WHERE id = $1`, id)
	return err
}

// ForceExpireLease is lab-only: it backdates a message's lease so the
// next Claim can steal it, without waiting out the real lease duration.
// Used to exercise the "stuck in-progress" recovery path on demand.
func ForceExpireLease(ctx context.Context, db Querier, id int64) error {
	_, err := db.Exec(ctx, `
		UPDATE queue_messages
		SET lease_expires_at = now() - interval '1 second'
		WHERE id = $1 AND status = 'leased'`, id)
	return err
}
