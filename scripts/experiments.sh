#!/usr/bin/env bash
# Full experiment suite for the async/queue idempotency lab.
# Needs: curl, jq, a running server (make run), and a fresh db (make reset).
set -euo pipefail

HOST="${HOST:-http://localhost:8080}"
pass=0
fail=0

reset() { curl -sX POST "$HOST/_reset" >/dev/null; }

stat_count() {
  # stat_count <consumer> <op>
  curl -s "$HOST/_stats" | jq -r --arg c "$1" --arg o "$2" \
    '(.audit_log // []) | map(select(.consumer==$c and .op==$o)) | (.[0].count // 0)'
}

expect_eq() {
  local desc="$1" got="$2" want="$3"
  if [[ "$got" == "$want" ]]; then
    echo "  ok   $desc (got $got)"
    pass=$((pass+1))
  else
    echo "  FAIL $desc (got $got, want $want)"
    fail=$((fail+1))
  fi
}

publish() {
  # publish <topic> <dedupe_key|null> <payload-json>
  local topic="$1" key="$2" payload="$3"
  if [[ "$key" == "null" ]]; then
    curl -s -X POST "$HOST/publish" -H 'Content-Type: application/json' \
      -d "{\"topic\":\"$topic\",\"payload\":$payload}"
  else
    curl -s -X POST "$HOST/publish" -H 'Content-Type: application/json' \
      -d "{\"topic\":\"$topic\",\"dedupe_key\":\"$key\",\"payload\":$payload}"
  fi
}

echo "=== 1. Naive order consumer duplicates every redelivery ==="
reset
resp=$(curl -s -X POST "$HOST/publish" -H 'Content-Type: application/json' \
  -d '{"topic":"orders","payload":{"order_id":"order-1","status":"paid","version":1}}')
mid=$(jq -r .message_id <<<"$resp")
# process it 3 times by forcing redelivery between each consume
for i in 1 2 3; do
  curl -s -X POST "$HOST/naive/consume?topic=orders" >/dev/null
  curl -s -X POST "$HOST/_duplicate" -H 'Content-Type: application/json' -d "{\"message_id\":$mid}" >/dev/null
done
expect_eq "naive order consumer: 3 deliveries -> 3 audit rows" "$(stat_count naive order.apply)" "3"

echo "=== 2. Safe order consumer: monotonic guard absorbs redelivery ==="
reset
resp=$(curl -s -X POST "$HOST/publish" -H 'Content-Type: application/json' \
  -d '{"topic":"orders","payload":{"order_id":"order-2","status":"paid","version":1}}')
mid=$(jq -r .message_id <<<"$resp")
for i in 1 2 3; do
  curl -s -X POST "$HOST/safe/consume?topic=orders" >/dev/null
  curl -s -X POST "$HOST/_duplicate" -H 'Content-Type: application/json' -d "{\"message_id\":$mid}" >/dev/null
done
expect_eq "safe order consumer: 3 deliveries, same version -> 1 audit row" "$(stat_count safe order.apply)" "1"

echo "=== 3. Out-of-order delivery is harmless under the version guard ==="
reset
curl -s -X POST "$HOST/publish" -H 'Content-Type: application/json' \
  -d '{"topic":"orders","payload":{"order_id":"order-3","status":"shipped","version":5}}' >/dev/null
curl -s -X POST "$HOST/safe/consume?topic=orders" >/dev/null   # applies v5
curl -s -X POST "$HOST/publish" -H 'Content-Type: application/json' \
  -d '{"topic":"orders","payload":{"order_id":"order-3","status":"paid","version":2}}' >/dev/null
resp=$(curl -s -X POST "$HOST/safe/consume?topic=orders")      # stale v2 arrives late
expect_eq "stale version rejected, not applied" "$(jq -r .applied <<<"$resp")" "false"

echo "=== 4. Naive notification consumer sends twice on redelivery ==="
reset
resp=$(curl -s -X POST "$HOST/publish" -H 'Content-Type: application/json' \
  -d '{"topic":"notifications","payload":{"order_id":"order-4","body":"hi"}}')
mid=$(jq -r .message_id <<<"$resp")
curl -s -X POST "$HOST/naive/consume?topic=notifications" >/dev/null
curl -s -X POST "$HOST/_duplicate" -H 'Content-Type: application/json' -d "{\"message_id\":$mid}" >/dev/null
curl -s -X POST "$HOST/naive/consume?topic=notifications" >/dev/null
expect_eq "naive notification consumer: 2 deliveries -> 2 sends" "$(stat_count naive notification.send)" "2"

echo "=== 5. Safe notification consumer: inbox absorbs redelivery ==="
reset
resp=$(curl -s -X POST "$HOST/publish" -H 'Content-Type: application/json' \
  -d '{"topic":"notifications","payload":{"order_id":"order-5","body":"hi"}}')
mid=$(jq -r .message_id <<<"$resp")
curl -s -X POST "$HOST/safe/consume?topic=notifications" >/dev/null
curl -s -X POST "$HOST/_duplicate" -H 'Content-Type: application/json' -d "{\"message_id\":$mid}" >/dev/null
curl -s -X POST "$HOST/safe/consume?topic=notifications" >/dev/null
expect_eq "safe notification consumer: 2 deliveries -> 1 send" "$(stat_count safe notification.send)" "1"

echo "=== 6. Producer-side dedupe: same dedupe_key + same payload -> replay, not a new message ==="
reset
p='{"order_id":"order-6","status":"paid","version":1}'
r1=$(publish orders "order-6-key" "$p")
r2=$(publish orders "order-6-key" "$p")
id1=$(jq -r .message_id <<<"$r1"); id2=$(jq -r .message_id <<<"$r2")
expect_eq "same dedupe_key replays the same message id" "$id2" "$id1"
expect_eq "second publish reports deduped=true" "$(jq -r .deduped <<<"$r2")" "true"

echo "=== 7. Producer-side dedupe: same key, different payload -> 422 ==="
reset
publish orders "order-7-key" '{"order_id":"order-7","status":"paid","version":1}' >/dev/null
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$HOST/publish" -H 'Content-Type: application/json' \
  -d '{"topic":"orders","dedupe_key":"order-7-key","payload":{"order_id":"order-7","status":"CANCELLED","version":1}}')
expect_eq "mismatched payload under reused dedupe_key -> 422" "$code" "422"

echo "=== 8. Crash before commit: effect rolled back, redelivery re-executes cleanly ==="
reset
resp=$(curl -s -X POST "$HOST/publish" -H 'Content-Type: application/json' \
  -d '{"topic":"orders","payload":{"order_id":"order-8","status":"paid","version":1}}')
mid=$(jq -r .message_id <<<"$resp")
crash=$(curl -s -X POST "$HOST/safe/consume?topic=orders&chaos=crash_before_commit")
expect_eq "crash_before_commit reports applied=false" "$(jq -r .applied <<<"$crash")" "false"
expect_eq "order not created after rollback" "$(curl -s "$HOST/_stats" | jq -r .orders)" "0"
curl -s -X POST "$HOST/_expire_lease" -H 'Content-Type: application/json' -d "{\"message_id\":$mid}" >/dev/null
retry=$(curl -s -X POST "$HOST/safe/consume?topic=orders")
expect_eq "retry after lease expiry applies cleanly" "$(jq -r .applied <<<"$retry")" "true"
expect_eq "exactly one audit row after crash + retry" "$(stat_count safe order.apply)" "1"

echo "=== 9. Crash after commit, before ack: redelivery is a safe no-op ==="
reset
resp=$(curl -s -X POST "$HOST/publish" -H 'Content-Type: application/json' \
  -d '{"topic":"orders","payload":{"order_id":"order-9","status":"paid","version":1}}')
mid=$(jq -r .message_id <<<"$resp")
crash=$(curl -s -X POST "$HOST/safe/consume?topic=orders&chaos=crash_after_commit_before_ack")
expect_eq "crash_after_commit_before_ack still applies the effect" "$(jq -r .applied <<<"$crash")" "true"
expect_eq "but does not ack" "$(jq -r .acked <<<"$crash")" "false"
curl -s -X POST "$HOST/_duplicate" -H 'Content-Type: application/json' -d "{\"message_id\":$mid}" >/dev/null
redelivery=$(curl -s -X POST "$HOST/safe/consume?topic=orders")
expect_eq "redelivery rejected by the version guard, not double-applied" "$(jq -r .applied <<<"$redelivery")" "false"
expect_eq "exactly one audit row" "$(stat_count safe order.apply)" "1"

echo "=== 10. Transactional outbox: order update -> relay -> notification, end to end ==="
reset
curl -s -X POST "$HOST/publish" -H 'Content-Type: application/json' \
  -d '{"topic":"orders","payload":{"order_id":"order-10","status":"paid","version":1}}' >/dev/null
curl -s -X POST "$HOST/safe/consume?topic=orders" >/dev/null
pending=$(curl -s "$HOST/_stats" | jq -r .outbox_pending)
expect_eq "outbox has one pending event after the order applies" "$pending" "1"
curl -s -X POST "$HOST/relay/run" >/dev/null
curl -s -X POST "$HOST/safe/consume?topic=notifications" >/dev/null
sent=$(curl -s "$HOST/_stats" | jq -r .notifications_sent)
expect_eq "relay + safe notification consumer delivers exactly one send" "$sent" "1"

echo
echo "=== summary: $pass passed, $fail failed ==="
[[ "$fail" -eq 0 ]]
