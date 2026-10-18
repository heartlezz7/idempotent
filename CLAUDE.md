# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Go + Gin + Postgres lab that demonstrates HTTP idempotency mechanisms, not a
production service. Two parallel route trees (`/naive/...` and `/safe/...`)
expose the same two resources (`users`, `information`). Every mutation
appends to `audit_log`; `GET /_stats` is the scoreboard for whether a given
endpoint actually behaved idempotently under retries.

## Commands

```bash
make up      # docker compose up -d (postgres) + wait for healthy; applies migrations via docker-entrypoint-initdb.d
make run     # go mod tidy, then `go run ./cmd/server` on :8080
make test    # ./scripts/experiments.sh -- full experiment suite (needs curl + jq + running server)
make reset   # curl -sX POST localhost:8080/_reset
make down    # docker compose down -v (wipes the db volume)
make psql    # docker compose exec db psql -U postgres -d postgres
```

The entry point is `cmd/server/main.go`; `cmd/migration/` only holds the
schema SQL. `make swagger` regenerates `docs/swagger/` from the handler
annotations (its general API info lives on `main()` in `cmd/server/main.go`).

`POST /_reset` truncates all tables without restarting the stack; prefer it
over `make down && make up` between manual experiments.

To run a single experiment instead of the full suite, copy the relevant
block out of `scripts/experiments.sh` (it's a plain bash script, sourced
top-to-bottom; each block calls `reset` first so blocks are independent).

There is no Go test suite (`go test`) — correctness is verified behaviorally
via `scripts/experiments.sh` against `GET /_stats`, plus `go vet ./...` /
`go build ./...` for static checks.

## Architecture

**Two route trees, one handler set.** `cmd/server/main.go` mounts `/naive/*` and
`/safe/*` groups pointing at the *same* handler functions where the
resource allows it (e.g. `POST /safe/users` and `POST /naive/users` both
call `h.CreateUser` — only the middleware in front differs). Where the
mechanism differs by resource, naive and safe use separate handler
functions (`PutUserNaive` vs `PutUserSafe`, etc.). The design invariant:
business logic in `src/handler/` must stay unaware of idempotency: the
protection is entirely middleware/schema-level, never `if`-branches in the
handler.

**Why two resources.** `users` has no naturally unique field, so `POST`
needs the `Idempotency-Key` middleware + a receipt table. `information` has
a unique `phone`, so a `UNIQUE` index (catch Postgres error `23505`) gives
`POST` idempotency for free — no key table needed. This split is the point
of the lab: build key infrastructure only when nothing cheaper fits.

**The idempotency middleware** (`src/route/middleware/idempotent.go`) is
the core piece to understand before touching anything idempotency-related:

1. Requires `Idempotency-Key`; scopes by `X-Api-Key` (tenant) + method+path
   (endpoint) — the scope is a security boundary, not just dedup.
2. Fingerprints the request as `sha256(method ‖ path ‖ body)` (byte-exact —
   no JSON canonicalization, so reordered keys break replay detection; this
   is a documented simplification, not a bug to silently "fix").
3. Claims ownership with a single `INSERT ... ON CONFLICT` (`claimSQL`) that
   can also steal an expired lease. There is deliberately no
   `SELECT`-then-`INSERT` anywhere — that gap is where duplicates are born.
   Any change to this logic must preserve the single-statement claim.
4. On lost claim: replay the stored response (`completed`), reject with
   `422` (fingerprint mismatch — different body, same key), or `409` +
   `Retry-After` (still in flight).
5. On won claim: opens one Postgres transaction, stashes it in gin context
   via `ctxTxKey`, and runs the handler with its `ResponseWriter` swapped
   for a `buffered` writer (`WriteHeaderNow` suppressed) so nothing reaches
   the socket before commit.
6. `5xx` (or `c.Errors` set) → rollback + delete the claim (`releaseSQL`).
   Persisting a 5xx as `completed` would poison the key forever.
7. Otherwise the receipt (`completeSQL`) is written in the *same*
   transaction as the handler's effect, then committed, then the buffered
   response is flushed. This atomicity (effect + receipt, one commit) is
   the entire soundness argument for the scheme.

**Handler cooperation contract.** Handlers must read/write through
`middleware.DBFrom(c, pool)` rather than the pool directly. `DBFrom` returns
the middleware's open transaction when present (under `/safe/users` POST)
or the plain pool otherwise (`/naive`, or any endpoint not wrapped by the
middleware). This is the one thing a handler must do correctly for the
middleware's transactional guarantee to hold — bypassing it (e.g. calling
`h.pool.Exec` directly inside a handler mounted under the middleware) would
silently break atomicity.

**Shared handler helpers** live in `src/handler/helpers.go`, unexported, in
package `handler`: `fail` (500 with `err.Error()`), `notFound` (404
`not_found`), `invalidBody` (400 `invalid_body`), `audit` (append to
`audit_log`), `etag`/`parseETag` (`"v<version>"` ↔ int), `chaos` (the
`X-Chaos: fail-after-write` injection point) and `isUniqueViolation`
(Postgres `23505`). Use them instead of writing those responses inline, and
add any new helper used by more than one handler file there rather than in
whichever resource file needs it first. Pass `audit` the `h.db(c)` handle,
not the pool, so the audit row commits in the same transaction as the
effect.

**Per-resource mechanisms** (see `README.md`'s table for the full naive-vs-safe
matrix): `PUT`/`PATCH` safety comes from assignment semantics (not
accumulation) plus `content_hash`/`version` columns backing `ETag`/`If-Match`
— idempotency and optimistic concurrency are separate concerns here:
idempotency handles retries from the *same* client, `version`/`ETag` handles
concurrent writes from *different* clients. `DELETE` safety comes from a
guarded state transition (`204` → `410` → `410`) plus a tombstone
(`deleted_at`), never a hard delete on `/safe`.

**Schema** (`cmd/migration/database/sql/001_init.sql`, applied automatically
by Postgres's `docker-entrypoint-initdb.d` on first `make up`): `users`,
`information`, `audit_log` (append-only observable side effect), and
`idempotency_keys` (composite PK `(tenant_id, endpoint, key)`, `state` in
`in_progress`/`completed`, `lease_expires_at` for lease-stealing, `expires_at`
for TTL sweep via `idempotency_keys_sweep` index). The idempotency table
lives in the same database as the business tables specifically so the
receipt can commit in the same transaction as the effect — moving it to a
separate store would break the atomicity guarantee.

**Lab control endpoints** (`src/handler/lab.go`, mounted at root, no
`/naive` or `/safe` prefix): `GET /_stats` (scoreboard), `POST /_reset`
(truncate everything), `POST /_sweep` (delete expired keys — production
would run this as a cron/goroutine), `POST /_expire_keys` (force every key
to look expired, for testing TTL behavior without waiting 24h — lab-only).

## Known simplifications (documented in README, not accidental)

- Fingerprinting is byte-exact JSON, not canonicalized — a reordered map on
  a legitimate retry will 422.
- Only `Content-Type`, `Location`, `ETag` are replayed on cache-hit (see
  `replayableHeaders` in the middleware).
- `4xx` responses are cached as `completed` (deterministic validation
  errors are safe to replay); only `5xx` releases the claim.
- Lease is 60s, TTL is 24h — the lease must exceed the hard request
  timeout; the TTL must outlast the slowest client's retry window.
- No lease-steal fencing token — an expired lease means "probably dead,"
  backstopped by whatever uniqueness constraint exists on the resource
  itself (which is exactly why `information.phone` has one and `users`
  deliberately does not).

When modifying this repo, preserve the naive/safe symmetry (a fix to one
side should usually have a corresponding, deliberately-broken counterpart
on the other) and keep these simplifications documented inline rather than
"fixing" them without being asked — they're the pedagogical point.
