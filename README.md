# Idempotency lab — Go + Gin + Postgres

Two parallel route trees over the same two resources. Fire the same request at both and diff the outcome.

- **`/naive/…`** — no protection. The control group.
- **`/safe/…`** — each method protected by the mechanism that actually suits it.

Every mutation appends to `audit_log`. That table is the observable side effect: run a request three times, then read `GET /_stats`. A correct idempotent endpoint appends once.

## Run it

```bash
docker compose up -d --build   # postgres on :5433 (schema applied on first boot) + the app on :8080
make test                      # the full experiment suite (needs curl + jq)
make reset                     # POST /_reset: truncate every table without restarting
docker compose down -v         # stop everything and wipe the db volume
```

The `app` service reaches Postgres at `db:5432` on the compose network. See
`docker-compose.yml` for the env and `Dockerfile` for the image build.

To run the app on the host instead, start only the database and point
`DATABASE_URL` at the published port:

```bash
docker compose up -d db
DATABASE_URL='postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable' go run .
```

`make psql` opens a shell on the database.

## API spec (Swagger)

With the app running, open <http://localhost:8080/swagger/index.html>. The
raw spec is committed at `docs/swagger/swagger.yaml` (and `.json`).

The spec is generated from `swag` annotations on the handlers in
`src/handler/`. After changing a route, a request/response type or an
annotation, regenerate it:

```bash
make swagger   # swag fmt + swag init -> docs/swagger/
```

The target runs a pinned `swag` (`SWAG_VERSION`, default `v1.16.6`) via
`go run`, so nothing needs installing. Keep it in step with the
`github.com/swaggo/swag` version in `go.mod`, or the generated `docs.go`
may not compile.

## Docs

Open these in a browser; they are self-contained HTML.

- [`docs/HTTP-idempotency-sequence-flow.html`](docs/HTTP-idempotency-sequence-flow.html) — the contract hop by hop: which layer declares, honours, supplies and enforces idempotency, stepped through per method.
- [`docs/safe-post-users.html`](docs/safe-post-users.html) — sequence diagram of `POST /safe/users`: the atomic claim, effect + receipt in one commit, a lost response, and the replayed retry.
- [`docs/idempotency-key.html`](docs/idempotency-key.html) — lifecycle of an `idempotency_keys` row: `in_progress` → `completed` → swept, and the 5xx release path.

The two diagrams are generated with [Archify](https://github.com/tt-a1i/archify) from the `.json` files next to them. Edit the JSON and re-run `archify deliver` rather than editing the HTML.

## Why two resources

They demonstrate different mechanisms, and picking the right one is most of the job.

| | `users` | `information` |
|---|---|---|
| Unique field | none — two people can share a name | `phone` |
| Mechanism for POST | `Idempotency-Key` + receipt table | `UNIQUE` index; catch `23505` |
| Cost | table, TTL sweep, lease, state machine | one index |

The point of the split: **build key infrastructure only when nothing cheaper fits.** `information` gets idempotent creates for free.

## Routes

| Method | Naive | Safe | Mechanism on `/safe` |
|---|---|---|---|
| `GET /users` | list | list | pure read |
| `GET /users/:id` | read | read + `ETag`/`304` | conditional GET |
| `POST /users` | duplicates every retry | one row | `Idempotency-Key` middleware |
| `PUT /users/:id` | upsert that still bumps `version` and audits | `201`/`200`, version frozen on identical writes | assignment + content hash |
| `PATCH /users/:id` | `name = name \|\| x` | merge patch + `If-Match` | assignment + optimistic lock |
| `DELETE /users/:id` | hard delete, audits every call | `204` → `410` → `410` | guarded transition + tombstone |
| `POST /information` | `500` with a leaked constraint name | `201` then `200` with the same id | unique index |
| `PUT /information/:id` | — | `201`/`200` | assignment |
| `PATCH /information/:id` | `age = age + n` | `{"age": 31}` + `If-Match` | assignment |
| `DELETE /information/:id` | unguarded | `204` → `410` | guarded transition |

Lab controls: `GET /_stats`, `POST /_reset`, `POST /_sweep`, `POST /_expire_keys`.

## The middleware

`src/route/middleware/idempotent.go`. `POST /safe/users` and `POST /naive/users` run the **same handler function** — the only difference is the middleware in front. That is the design goal: business logic stays unaware of idempotency.

The handler cooperates on exactly one thing: it writes through `idem.DBFrom(c, pool)`, which returns the middleware's transaction when there is one. That is what puts the effect and the receipt in the same commit.

Sequence:

1. Require `Idempotency-Key`; scope by `X-Api-Key` + endpoint.
2. Fingerprint `sha256(method ‖ path ‖ body)`.
3. Claim in **one** `INSERT … ON CONFLICT`, which also steals an expired lease. Never `SELECT` then `INSERT`.
4. Lost the claim → `422` (fingerprint mismatch) / replay / `409` + `Retry-After`.
5. Won → `BEGIN`, run the handler with its output **buffered**.
6. `5xx` → rollback and **delete** the claim. Saving a 5xx would poison the key forever.
7. Otherwise write the receipt in the same transaction.
8. `COMMIT`, then flush the buffered bytes.

Step 5's buffering matters: streaming straight to the socket would send `201 Created` before the commit, promising something that might not land.

## Things to try

```bash
KEY=$(uuidgen)

# same key three times -> one user, two replays
for i in 1 2 3; do
  curl -si -X POST localhost:8080/safe/users \
    -H "Idempotency-Key: $KEY" -H 'Content-Type: application/json' \
    -d '{"name":"Somchai"}' | head -1
done
curl -s localhost:8080/_stats | jq

# same key, different body -> 422
curl -s -X POST localhost:8080/safe/users \
  -H "Idempotency-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"name":"Malee"}'

# crash after the write; compare the trees
curl -s -X POST localhost:8080/naive/users -H 'X-Chaos: fail-after-write' \
  -H 'Content-Type: application/json' -d '{"name":"Crash"}'
curl -s -X POST localhost:8080/naive/users \
  -H 'Content-Type: application/json' -d '{"name":"Crash"}'
curl -s localhost:8080/_stats | jq .users_live   # 2 — the 500 still committed
```

`X-Chaos: fail-after-write` returns `500` after the insert. On `/naive` the row is already committed, so the retry duplicates it. On `/safe` the transaction rolls back, the key is released, and the retry produces exactly one row.

## Notes and simplifications

- **Fingerprinting is byte-exact.** Production canonicalises JSON first (sort keys, normalise numbers) and strips volatile fields like trace IDs, otherwise a reordered map makes a legitimate retry fail with `422`.
- **Only `Content-Type`, `Location` and `ETag` are replayed.** Extend `replayableHeaders` if your clients depend on others.
- **`4xx` responses are stored as completed.** Deterministic validation errors are safe to replay; if you'd rather let clients correct and retry under the same key, release them alongside the `5xx` branch.
- **The lease is 60s** and the TTL 24h. The lease must exceed your hard request timeout; the TTL must outlast your slowest client's retry window, including a mobile app that retries tomorrow.
- **No lease-steal fencing.** An expired lease means "probably dead", not "definitely dead". The real backstop is a uniqueness constraint on the resource itself — `information.phone` has one, `users` deliberately does not, which is exactly why it needs keys.
- **`ExpireKeys` is lab-only.** It forces every key to look expired so you can watch the TTL boundary without waiting a day.
- Naive `information` has no `PUT` — the safe one is the only interesting version, since a two-field replacement is idempotent almost by accident.

## Your schema

Both fields in the `user` sample were tagged `db:"id"`, so a struct-scanning library would read the id column twice and never populate `Name`. Fixed in `src/model/user.go`, where `Name` is tagged `db:"name"`. Types were also omitted — `ID` is `string` (uuid) and `Age` is `int`.

`version` was added to both structs. It backs `ETag`/`If-Match`, which is how you get concurrency safety — idempotency alone does nothing against two *different* clients writing at once.
