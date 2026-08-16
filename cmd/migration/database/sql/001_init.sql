-- =====================================================================
--  Idempotency lab schema
--  Postgres 11+ (uses the built-in sha256() function)
-- =====================================================================

-- ---------------------------------------------------------------------
-- users
--   No naturally unique field. Two people can legitimately be called
--   "Somchai", so a UNIQUE constraint cannot dedupe creates here.
--   This is the resource that NEEDS Idempotency-Key.
--
--   content_hash + version let PUT/PATCH tell "you changed something"
--   apart from "you sent the same thing again", so an identical rewrite
--   does not bump the version and invalidate everyone else's ETag.
-- ---------------------------------------------------------------------
CREATE TABLE users (
    id           uuid        PRIMARY KEY,
    name         text        NOT NULL,
    content_hash bytea       NOT NULL,
    version      int         NOT NULL DEFAULT 1,
    deleted_at   timestamptz,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- information
--   phone IS naturally unique. That single index gives POST idempotency
--   for free -- no key table, no TTL, no lease. The only design question
--   left is what you RETURN on the duplicate.
--
--   Partial index so a soft-deleted row does not permanently reserve
--   a phone number.
-- ---------------------------------------------------------------------
CREATE TABLE information (
    id         uuid        PRIMARY KEY,
    phone      text        NOT NULL,
    age        int         NOT NULL,
    version    int         NOT NULL DEFAULT 1,
    deleted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX information_phone_live
    ON information (phone) WHERE deleted_at IS NULL;

-- ---------------------------------------------------------------------
-- audit_log
--   Append-only. This is the observable side effect of the lab: run the
--   same request three times and count the rows. A correct idempotent
--   endpoint appends once. A broken one appends three times.
-- ---------------------------------------------------------------------
CREATE TABLE audit_log (
    id          bigserial   PRIMARY KEY,
    op          text        NOT NULL,   -- 'create' | 'replace' | 'patch' | 'delete'
    resource    text        NOT NULL,   -- 'users' | 'information'
    resource_id text        NOT NULL,
    route       text        NOT NULL,   -- '/naive/...' or '/safe/...'
    at          timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- idempotency_keys
--   One row per logical operation. Lives in the SAME database as the
--   business tables so the receipt commits in the same transaction as
--   the effect. That single fact is what makes the scheme sound.
-- ---------------------------------------------------------------------
CREATE TABLE idempotency_keys (
    tenant_id        text        NOT NULL,   -- from X-Api-Key; the security scope
    endpoint         text        NOT NULL,   -- 'POST /safe/users'
    key              text        NOT NULL,   -- client-supplied Idempotency-Key
    request_hash     bytea       NOT NULL,   -- sha256 of method+path+body
    state            text        NOT NULL,   -- 'in_progress' | 'completed'
    response_code    int,
    response_body    text,
    response_headers text,                   -- JSON map of replayed headers
    lease_expires_at timestamptz NOT NULL,   -- must exceed your hard request timeout
    expires_at       timestamptz NOT NULL,   -- retention window
    created_at       timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, endpoint, key),
    CHECK (state IN ('in_progress', 'completed'))
);

CREATE INDEX idempotency_keys_sweep ON idempotency_keys (expires_at);
