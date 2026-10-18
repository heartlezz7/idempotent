-- =====================================================================
--  Idempotency lab — beyond HTTP: queues, consumers, outbox
--  Postgres 14+ (uses SKIP LOCKED, gen_random_uuid via pgcrypto not
--  required — ids are bigserial / client-supplied text)
--
--  HTTP idempotency (Idempotency-Key over REST) lives on another branch.
--  This schema models the doc's "Beyond HTTP" section: at-least-once
--  message delivery plus the patterns that make a consumer safe under
--  it -- the inbox table, natural idempotency via monotonic guards, and
--  the transactional outbox.
-- =====================================================================

-- ---------------------------------------------------------------------
-- queue_messages
--   A single-node, Postgres-backed stand-in for a broker (think: one
--   partition of a Kafka topic, or an SQS queue). At-least-once by
--   construction: a lease can expire and be reclaimed, and the lab
--   control plane can force a redelivery of an already-acked message
--   to simulate a broker replay after a rebalance.
--
--   dedupe_key is the producer-side idempotency knob -- the analogue of
--   Kafka's idempotent producer (PID + sequence) or SQS FIFO's
--   MessageDeduplicationId. Uniqueness is scoped by (topic, dedupe_key),
--   never global, same reasoning as (tenant, endpoint, key) on the HTTP
--   side: the scope is a security/correctness boundary, not just a
--   dedup trick.
-- ---------------------------------------------------------------------
CREATE TABLE queue_messages (
    id                bigserial   PRIMARY KEY,
    topic             text        NOT NULL,
    dedupe_key        text,                        -- NULL = no producer-side dedup requested
    payload           jsonb       NOT NULL,
    payload_hash      bytea       NOT NULL,         -- sha256 of canonical payload; catches a reused
                                                     -- dedupe_key attached to different content
    status            text        NOT NULL DEFAULT 'pending', -- 'pending' | 'leased' | 'done'
    attempts          int         NOT NULL DEFAULT 0,
    lease_owner       text,
    lease_expires_at  timestamptz,
    available_at      timestamptz NOT NULL DEFAULT now(),
    created_at        timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX queue_messages_dedupe
    ON queue_messages (topic, dedupe_key) WHERE dedupe_key IS NOT NULL;

CREATE INDEX queue_messages_poll ON queue_messages (topic, status, available_at);

-- ---------------------------------------------------------------------
-- inbox_processed
--   The consumer-side dedup table for effects that are NOT naturally
--   idempotent -- appending a notification, incrementing a counter,
--   calling a third party. "INSERT ... ON CONFLICT DO NOTHING in the
--   same transaction as the effect" -- zero rows inserted means already
--   handled, skip. This is the direct analogue of the HTTP
--   idempotency_keys table, keyed by message identity instead of a
--   client-supplied header.
-- ---------------------------------------------------------------------
CREATE TABLE inbox_processed (
    consumer     text        NOT NULL,   -- consumer group, e.g. 'notification-consumer'
    message_id   bigint      NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, message_id)
);

-- ---------------------------------------------------------------------
-- orders
--   The resource that needs NO dedup table at all. A status update is
--   naturally idempotent once it is expressed as an assignment guarded
--   by a monotonic version: "UPDATE ... WHERE version < incoming" makes
--   replays, redeliveries and out-of-order delivery all harmless in one
--   WHERE clause. This is the queue-side analogue of `information`'s
--   UNIQUE index on the HTTP branch: reach for the cheap structural
--   option before building key infrastructure.
-- ---------------------------------------------------------------------
CREATE TABLE orders (
    id         text        PRIMARY KEY,   -- business id, e.g. 'order-123'
    status     text        NOT NULL,
    version    bigint      NOT NULL DEFAULT 0,   -- the incoming message's version must exceed this
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- notifications_sent
--   The append-only, NOT-naturally-idempotent side effect. Standing in
--   for "send an email/SMS" -- there is no assignment that makes
--   sending twice equal to sending once, so this is the resource that
--   genuinely needs inbox_processed in front of it.
-- ---------------------------------------------------------------------
CREATE TABLE notifications_sent (
    id          bigserial   PRIMARY KEY,
    order_id    text        NOT NULL,
    message_id  bigint      NOT NULL,   -- which queue message caused this send
    body        text        NOT NULL,
    sent_at     timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- outbox_events
--   The transactional outbox: a consumer that needs to trigger a
--   downstream effect writes the state change and this row in the SAME
--   transaction, instead of calling the queue directly (the dual-write
--   problem). A relay polls unpublished rows and publishes them,
--   deriving the downstream dedupe_key deterministically from the
--   event id so a relay retry can never double-publish.
-- ---------------------------------------------------------------------
CREATE TABLE outbox_events (
    id          bigserial   PRIMARY KEY,
    topic       text        NOT NULL,
    payload     jsonb       NOT NULL,
    published   boolean     NOT NULL DEFAULT false,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX outbox_unpublished ON outbox_events (id) WHERE NOT published;

-- ---------------------------------------------------------------------
-- audit_log
--   The observable side effect, same idea as the HTTP lab: run a
--   message through three times and read GET /_stats. A correct
--   consumer applies the effect once; a naive one applies it every
--   delivery.
-- ---------------------------------------------------------------------
CREATE TABLE audit_log (
    id          bigserial   PRIMARY KEY,
    consumer    text        NOT NULL,   -- 'naive' | 'safe'
    op          text        NOT NULL,   -- 'order.apply' | 'notification.send'
    message_id  bigint,
    resource_id text,
    at          timestamptz NOT NULL DEFAULT now()
);
