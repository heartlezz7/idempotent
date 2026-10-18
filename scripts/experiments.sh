#!/usr/bin/env bash
# Idempotency lab -- experiment suite.
#
# Requires: curl, jq, and a running server (make up && make run).
# Each experiment resets the database first, so you can also run them
# one at a time by copying the block you care about.

set -uo pipefail
BASE="${BASE:-http://localhost:8080}"

bold()  { printf '\n\033[1m%s\033[0m\n' "$*"; }
dim()   { printf '\033[2m%s\033[0m\n' "$*"; }
good()  { printf '  \033[32m%s\033[0m\n' "$*"; }
bad()   { printf '  \033[31m%s\033[0m\n' "$*"; }

reset() { curl -sX POST "$BASE/_reset" >/dev/null; }
stats() { curl -s "$BASE/_stats"; }

# call METHOD PATH [BODY] [EXTRA_CURL_ARGS...]
# prints "<status> <body>"
call() {
  local method="$1" path="$2" body="${3:-}"; shift 3 || shift 2
  if [[ -n "$body" ]]; then
    curl -s -o /tmp/idem.out -w '%{http_code}' -X "$method" "$BASE$path" \
      -H 'Content-Type: application/json' -d "$body" "$@"
  else
    curl -s -o /tmp/idem.out -w '%{http_code}' -X "$method" "$BASE$path" "$@"
  fi
  printf ' '
  cat /tmp/idem.out
  echo
}

KEY="$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)"

# =====================================================================
bold "1. POST /naive/users x3  -- the same body, three times"
reset
for i in 1 2 3; do
  dim "  attempt $i: $(call POST /naive/users '{"name":"Somchai"}')"
done
n=$(stats | jq .users_live)
[[ "$n" == "3" ]] && bad "users_live = $n   <- three people now exist" \
                  || good "users_live = $n"

# =====================================================================
bold "2. POST /safe/users x3  -- same Idempotency-Key"
reset
for i in 1 2 3; do
  dim "  attempt $i: $(call POST /safe/users '{"name":"Somchai"}' -H "Idempotency-Key: $KEY")"
done
n=$(stats | jq .users_live)
a=$(stats | jq .audit_entries)
[[ "$n" == "1" ]] && good "users_live = $n, audit_entries = $a  <- replayed twice" \
                  || bad  "users_live = $n"
dim "  replay header check:"
curl -sD- -o /dev/null -X POST "$BASE/safe/users" \
  -H "Idempotency-Key: $KEY" -H 'Content-Type: application/json' \
  -d '{"name":"Somchai"}' | grep -i 'idempotent-replayed' || true

# =====================================================================
bold "3. Same key, different payload  -- client bug, not a retry"
dim "  $(call POST /safe/users '{"name":"Malee"}' -H "Idempotency-Key: $KEY")"
good "expect 422 key_reused_with_different_payload"

# =====================================================================
bold "4. Key scoping  -- another tenant must not see that response"
dim "  $(call POST /safe/users '{"name":"Somchai"}' -H "Idempotency-Key: $KEY" -H 'X-Api-Key: other-tenant')"
good "expect 201 with a NEW id: the key namespace is per-tenant"

# =====================================================================
bold "5. Five concurrent retries, one key"
reset
CKEY="$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)"
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "  parallel $i -> %{http_code}\n" -X POST "$BASE/safe/users" \
    -H "Idempotency-Key: $CKEY" -H 'Content-Type: application/json' \
    -d '{"name":"Racer"}' &
done
wait
n=$(stats | jq .users_live)
[[ "$n" == "1" ]] && good "users_live = $n  <- one winner; losers got 409 or a replay" \
                  || bad  "users_live = $n"

# =====================================================================
bold "6. Crash after the write  (X-Chaos: fail-after-write)"
reset
CHKEY="$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)"
dim "  naive, attempt 1: $(call POST /naive/users '{"name":"Crash"}' -H 'X-Chaos: fail-after-write')"
dim "  naive, retry:     $(call POST /naive/users '{"name":"Crash"}')"
bad "naive users_live = $(stats | jq .users_live)  <- the 500 still committed a row"

reset
dim "  safe, attempt 1:  $(call POST /safe/users '{"name":"Crash"}' -H "Idempotency-Key: $CHKEY" -H 'X-Chaos: fail-after-write')"
dim "  safe, retry:      $(call POST /safe/users '{"name":"Crash"}' -H "Idempotency-Key: $CHKEY")"
good "safe users_live = $(stats | jq .users_live)  <- rolled back, key released, retry re-ran"

# =====================================================================
bold "7. PUT x3  -- assignment should be a no-op after the first"
reset
UID1="$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)"
for i in 1 2 3; do
  dim "  naive attempt $i: $(call PUT "/naive/users/$UID1" '{"name":"Nok"}')"
done
bad "naive: version climbed, audit_entries = $(stats | jq .audit_entries)"

reset
UID2="$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)"
for i in 1 2 3; do
  dim "  safe attempt $i:  $(call PUT "/safe/users/$UID2" '{"name":"Nok"}')"
done
good "safe: 201 then 200 200, version stayed 1, audit_entries = $(stats | jq .audit_entries)"

# =====================================================================
bold "8. PATCH  -- accumulation vs assignment"
reset
call POST /naive/information '{"phone":"0812345678","age":30}' >/dev/null
IID=$(curl -s "$BASE/naive/information" | jq -r '.[0].id')
for i in 1 2 3; do
  dim "  naive increment $i: $(call PATCH "/naive/information/$IID" '{"age_increment":1}')"
done
bad "naive age = $(curl -s "$BASE/naive/information/$IID" | jq .age)  <- depends on retry count"

ETAG=$(curl -sD- -o /dev/null "$BASE/safe/information/$IID" | grep -i '^etag:' | tr -d '\r' | awk '{print $2}')
for i in 1 2 3; do
  dim "  safe merge $i:     $(call PATCH "/safe/information/$IID" '{"age":31}' -H "If-Match: $ETAG")"
done
good "safe age = $(curl -s "$BASE/safe/information/$IID" | jq .age)  <- identical patch never advances the version, so the stale If-Match keeps working"

# =====================================================================
bold "9. POST /information  -- the unique constraint does the deduping"
reset
dim "  naive 1st: $(call POST /naive/information '{"phone":"0899999999","age":25}')"
dim "  naive 2nd: $(call POST /naive/information '{"phone":"0899999999","age":25}')"
bad "naive: 500 with a leaked constraint name -- data safe, API unusable"

reset
dim "  safe 1st:  $(call POST /safe/information '{"phone":"0899999999","age":25}')"
dim "  safe 2nd:  $(call POST /safe/information '{"phone":"0899999999","age":25}')"
good "safe: 201 then 200 with the same id. No key table involved."

# =====================================================================
bold "10. DELETE x3"
reset
call POST /safe/users '{"name":"Gone"}' -H "Idempotency-Key: $(uuidgen)" >/dev/null
DID=$(curl -s "$BASE/safe/users" | jq -r '.[0].id')
for i in 1 2 3; do
  dim "  naive attempt $i: $(call DELETE "/naive/users/$DID")"
done
bad "naive audit_entries = $(stats | jq .audit_entries)  <- side effect fired 3x for 1 deletion"

reset
call POST /safe/users '{"name":"Gone"}' -H "Idempotency-Key: $(uuidgen)" >/dev/null
DID=$(curl -s "$BASE/safe/users" | jq -r '.[0].id')
for i in 1 2 3; do
  dim "  safe attempt $i:  $(call DELETE "/safe/users/$DID")"
done
good "safe: 204 then 410 410. audit_entries = $(stats | jq .audit_entries)"

# =====================================================================
bold "11. TTL expiry  -- a retry after the window is a NEW operation"
reset
TKEY="$(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)"
dim "  before: $(call POST /safe/users '{"name":"Late"}' -H "Idempotency-Key: $TKEY")"
curl -sX POST "$BASE/_expire_keys" >/dev/null
curl -sX POST "$BASE/_sweep" >/dev/null
dim "  after:  $(call POST /safe/users '{"name":"Late"}' -H "Idempotency-Key: $TKEY")"
bad "users_live = $(stats | jq .users_live)  <- this is why the TTL must be documented"

bold "final stats"
stats | jq .
