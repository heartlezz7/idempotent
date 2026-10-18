# Idempotency lab — beyond HTTP: queues and consumers

The HTTP `Idempotency-Key` lab lives on another branch. This one is the doc's
["Beyond HTTP"](docs/api-idempotency.html) section made runnable: a
single-node, Postgres-backed stand-in for a message broker, and two ways to
write a consumer against it.

At-least-once delivery is simulated on purpose — a lease can expire and be
reclaimed, and the lab can force a redelivery of a message a consumer
already fully handled (an ack that never reached the broker, a rebalance
replay). **Naive** consumers apply every delivery unconditionally. **Safe**
consumers apply the mechanism that fits the resource, so a redelivered or
out-of-order message is a correct no-op instead of a duplicate effect.

Every applied effect appends to `audit_log`. Run a message through 3
deliveries, read `GET /_stats`: a correct consumer appends once.

## Run it

Two ways: everything in Docker, or Postgres in Docker and the API on the host.

### Option A: all in Docker

```bash
docker compose up -d --build   # postgres + the app on :8080, schema applied on first boot
docker compose logs -f app     # follow the app's logs
```

The `app` service gets its `DATABASE_URL` from `docker-compose.yml` and reaches
Postgres at `db:5432` on the compose network; it does not read `.env`.

### Option B: API on the host

```bash
cp .env.example .env           # once; fill in <username> and <password>
make up                        # postgres on localhost:5433, schema applied on first boot
make run                       # go mod tidy, then go run ./cmd/server on :8080
```

Use the `POSTGRES_USER` / `POSTGRES_PASSWORD` from `docker-compose.yml`
for `<username>` / `<password>`, percent-encoding any special characters.
Don't run the compose `app` service and `make run` at the same time; both
listen on `:8080`.

### Then

```bash
make test                      # the full experiment suite (needs curl + jq)
make unit-test                 # go test ./... against postgres (see Tests)
make reset                     # POST /_reset: truncate every table without restarting
make psql                      # psql shell on the database
make down                      # stop everything and wipe the db volume
```

Once it's up:

| | URL |
|---|---|
| API | <http://localhost:8080> |
| Swagger UI | <http://localhost:8080/swagger/index.html> |
| Raw spec (JSON) | <http://localhost:8080/swagger/doc.json> |
| Scoreboard | <http://localhost:8080/_stats> |

### Configuration

`cmd/server` loads `.env` from the working directory with
[`joho/godotenv`](https://github.com/joho/godotenv). Variables already set in
the environment take precedence, and a missing `.env` is fine.

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | `postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable` | Postgres connection string |
| `ADDR` | `:8080` | Listen address |

Special characters in the password must be percent-encoded in the URL — `@`
becomes `%40`. `POSTGRES_PASSWORD` only applies when the volume is first
created; after changing it in `docker-compose.yml`, run `make down` so the
database is re-initialised with the new password.

## API spec (Swagger)

With the app running, open the Swagger UI at
<http://localhost:8080/swagger/index.html> (raw spec:
<http://localhost:8080/swagger/doc.json>). The same spec is committed at
`docs/swagger/swagger.yaml` (and `.json`).

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

## Postman

Import [`postman/idempotency_lab.postman_collection.json`](postman/idempotency_lab.postman_collection.json)
(Postman → Import). `{{baseUrl}}` defaults to `http://localhost:8080`.

- **1. Endpoints** — every route, one request each: lab controls, the
  producer, naive and safe consumers (including both `?chaos=` points), the
  outbox relay, and the chaos endpoints. `POST /publish` saves the
  `message_id` into `{{messageId}}`, which `/_duplicate` and
  `/_expire_lease` use.
- **2. Scenarios** — the same ten experiments as `scripts/experiments.sh`:
  naive vs. guarded order consumers, out-of-order delivery, naive vs. inbox
  notification consumers, producer `dedupe_key` replay and `422`, both
  crash windows, and the transactional outbox end to end. Each one starts
  with `POST /_reset` and asserts the outcome with tests, so run a scenario
  (or the whole folder) in the Collection Runner and read the results.

To run the scenarios from the terminal:

```bash
npx newman run postman/idempotency_lab.postman_collection.json --folder "2. Scenarios"
```

## Docs

Open these in a browser; they are self-contained HTML.

- [`docs/api-idempotency.html`](docs/api-idempotency.html) — the conceptual deep dive; its "Beyond HTTP" tab is what this branch implements.
- [`docs/async-flow.html`](docs/async-flow.html) — architecture of the queue flow: producer dedupe, safe consumers, outbox relay, inbox.
- [`docs/queue-message.html`](docs/queue-message.html) — lifecycle of a `queue_messages` row, including the crash windows and the `/_duplicate` replay.

The two diagrams are generated with [Archify](https://github.com/tt-a1i/archify) from the `.json` files next to them. Edit the JSON and re-run `archify deliver` rather than editing the HTML.

## Why two resources

Same split as the HTTP branch, translated to messages: build the dedup
table only when the effect isn't naturally idempotent.

| | `orders` | `notifications` |
|---|---|---|
| Effect | overwrite a status | append-only send |
| Naturally idempotent? | yes — once expressed as an assignment | no |
| Mechanism | monotonic guard: `WHERE version < incoming` | inbox table: `INSERT ... ON CONFLICT DO NOTHING` |
| Cost | one `WHERE` clause, no extra table | a table, scoped by consumer group |

`orders` gets idempotent processing for free. `notifications` is the
resource that genuinely needs the inbox pattern — there's no assignment
that makes sending an email twice equal to sending it once.

## The pipeline

```
POST /publish  --topic=orders-->  queue_messages
                                        |
                              /safe/consume?topic=orders
                                        |
                    BEGIN: upsert orders WHERE version < incoming
                           + INSERT outbox_events   (same transaction)
                                  COMMIT, ack
                                        |
                              POST /relay/run
                                        |
                    outbox_events (unpublished) --publish--> queue_messages (topic=notifications)
                                        |
                          /safe/consume?topic=notifications
                                        |
                    BEGIN: INSERT inbox_processed ON CONFLICT DO NOTHING
                           + INSERT notifications_sent   (if new)
                                  COMMIT, ack
```

The transactional outbox (`src/outbox/relay.go`) is what removes the
dual-write problem: the order update and the intent to notify commit
together, in one transaction, in `src/consumer/order.go`. If the relay
itself crashes between publishing and marking a row published, re-running
it is safe — the downstream `dedupe_key` is derived deterministically from
the outbox row's own id (`outbox:<id>`), never a fresh random token, so a
retried publish replays the same queue message instead of creating a
second one.

## Routes

| Method | Route | Purpose |
|---|---|---|
| `POST` | `/publish` | Manual producer. `dedupe_key` is optional — the idempotent-producer analogue of Kafka's PID+sequence or SQS FIFO's `MessageDeduplicationId`. Reusing a key with a different payload returns `422`. |
| `POST` | `/naive/consume?topic=` | Claim + apply unconditionally. No dedup. |
| `POST` | `/safe/consume?topic=&chaos=` | Claim + apply through the mechanism that fits the resource. `chaos` injects a fault (below). |
| `POST` | `/relay/run?limit=` | Publish up to `limit` pending outbox rows. |
| `GET` | `/_stats` | Scoreboard: audit counts by consumer/op, queue message counts by topic/status, table sizes. |
| `POST` | `/_reset` | Truncate every lab table. |
| `POST` | `/_duplicate` | Lab-only: force a message back to `pending` regardless of status, simulating a broker redelivery. `{"message_id": n}` |
| `POST` | `/_expire_lease` | Lab-only: backdate a message's lease so the next claim can steal it immediately. `{"message_id": n}` |

Topics: `orders`, `notifications`.

## Chaos: crash windows, on demand

`?chaos=` on `/safe/consume` mirrors the doc's races-and-recovery crash
table:

| `chaos` value | Simulates | What the safe consumer does |
|---|---|---|
| *(unset)* | Clean run | Applies, commits, acks. |
| `crash_before_commit` | Process dies mid-transaction | Effect **and** outbox row roll back together — no half-done state. Message stays leased. A retry after the lease expires (or `/_expire_lease`) re-executes cleanly. |
| `crash_after_commit_before_ack` | Process dies after `COMMIT`, before telling the broker | Effect is durable, but the message is never acked. A forced redelivery (`/_duplicate`) finds the guard (or inbox) already satisfied — one effect, not two. |

## The claim (never check-then-act)

`src/queue/broker.go`'s `Claim` is a single statement — `FOR UPDATE SKIP
LOCKED` inside the `WHERE` of an `UPDATE` — never a `SELECT` then a
separate claim. Two consumers racing for the same pending message: one
wins the row, the other's subquery skips the locked row and finds nothing
(or the next message). This is the queue-world version of the HTTP
branch's `INSERT ... ON CONFLICT`.

## Things to try

```bash
# publish once, "deliver" it three times, watch the naive consumer triple-count
curl -s -X POST localhost:8080/_reset >/dev/null
ID=$(curl -s -X POST localhost:8080/publish -H 'Content-Type: application/json' \
  -d '{"topic":"orders","payload":{"order_id":"o-1","status":"paid","version":1}}' | jq -r .message_id)
for i in 1 2 3; do
  curl -s -X POST localhost:8080/naive/consume?topic=orders >/dev/null
  curl -s -X POST localhost:8080/_duplicate -H 'Content-Type: application/json' -d "{\"message_id\":$ID}" >/dev/null
done
curl -s localhost:8080/_stats | jq '.audit_log'   # naive: count 3

# same experiment against /safe -- one audit row no matter how many redeliveries
curl -s -X POST localhost:8080/_reset >/dev/null
ID=$(curl -s -X POST localhost:8080/publish -H 'Content-Type: application/json' \
  -d '{"topic":"orders","payload":{"order_id":"o-2","status":"paid","version":1}}' | jq -r .message_id)
for i in 1 2 3; do
  curl -s -X POST localhost:8080/safe/consume?topic=orders >/dev/null
  curl -s -X POST localhost:8080/_duplicate -H 'Content-Type: application/json' -d "{\"message_id\":$ID}" >/dev/null
done
curl -s localhost:8080/_stats | jq '.audit_log'   # safe: count 1

# crash mid-transaction, then let the retry re-execute
curl -s -X POST localhost:8080/_reset >/dev/null
ID=$(curl -s -X POST localhost:8080/publish -H 'Content-Type: application/json' \
  -d '{"topic":"orders","payload":{"order_id":"o-3","status":"paid","version":1}}' | jq -r .message_id)
curl -s -X POST "localhost:8080/safe/consume?topic=orders&chaos=crash_before_commit" | jq
curl -s -X POST localhost:8080/_expire_lease -H 'Content-Type: application/json' -d "{\"message_id\":$ID}" >/dev/null
curl -s -X POST localhost:8080/safe/consume?topic=orders | jq   # applies exactly once
```

`./scripts/experiments.sh` runs 10 such scenarios end to end (naive vs
safe duplication, out-of-order delivery, producer-side dedupe conflict,
both crash windows, and the full outbox → notification pipeline) and
asserts on `/_stats`.

## Tests

Two layers, same schema, no mocks — the mechanisms under test (atomic
claims, transactional commits, unique constraints) are exactly what a mock
would paper over.

```bash
make up          # postgres + schema
make unit-test   # go test -p 1 ./... -- package-level, talks straight to postgres
make run         # in another shell
make test        # scripts/experiments.sh -- end to end, over HTTP
```

`go test ./...` connects to `TEST_DATABASE_URL` (default: the `make up`
database on `:5433`) and truncates the lab tables before each test, same
as `/_reset`. Without a reachable database every test reports `SKIP` with
a `run make up first` message rather than failing — `go test ./...` stays
safe to run from a clean checkout.

All packages share that one database and truncate the same tables, so
run them serially: `make unit-test` passes `-p 1`. A plain parallel
`go test ./...` lets one package's truncate or inserts land mid-test in
another and fails intermittently.

Coverage: `src/queue` (producer dedupe + conflict, the atomic claim under
concurrent goroutines, lease expiry/stealing, ack/release, forced
redelivery), `src/consumer` (naive duplication vs. safe absorption for
both the monotonic-guard and inbox-table resources, out-of-order delivery,
and both chaos crash windows), `src/outbox` (relay publish, the
deterministic dedupe key, and a simulated relay crash that proves the
retry doesn't double-publish), and `src/handler` (the HTTP status/JSON
translation layer, via `httptest` against the same database).

## Notes and simplifications

- **Payload canonicalization is `json.Unmarshal` + `json.Marshal`**, which
  sorts object keys but doesn't normalize number formatting. Same
  simplification the doc calls out for HTTP fingerprinting, applied to the
  producer-side `dedupe_key` fingerprint here.
- **Lease ownership isn't fenced.** An expired lease means "probably dead,"
  not "definitely dead" — same caveat as the HTTP branch's key leases. The
  real backstop is that the *effect* itself is safe to reapply (the version
  guard, the inbox row), never the lease alone.
- **One broker, one node.** `queue_messages` is a single Postgres table,
  not a partitioned, replicated log. It's enough to demonstrate the
  consumer-side patterns; it is not a Kafka client.
- **The relay is polled, not scheduled.** `POST /relay/run` runs one pass
  on demand so experiments stay deterministic. A real deployment would run
  it on a timer or trigger it from `LISTEN/NOTIFY`.
- **`/_duplicate` and `/_expire_lease` are lab-only.** They force
  conditions a real broker only produces nondeterministically (a lost ack,
  a stale lease), so the consumer-side behavior can be exercised on demand
  instead of waited for.
