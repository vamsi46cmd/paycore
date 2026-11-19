#!/usr/bin/env bash
# Chaos / recovery scenarios against the running docker compose stack.
#   tests/chaos/chaos.sh all          run every scenario
#   tests/chaos/chaos.sh kafka        run one (worker | kafka | postgres | redis | timeout | api-kill | concurrent-captures | concurrent-refunds)
# Each scenario injects a real failure, then prints EXPECTED vs OBSERVED counts and re-checks the financial invariants.
# Exit status is non-zero if any check fails. Requires: docker compose stack up, curl, python3.
set -uo pipefail
cd "$(dirname "$0")/../.."
API=${API:-http://localhost:8080}
ADMIN=${ADMIN_KEY:-dev-only-admin-key-change-me}
FAILED=0
CHECKS=0

sql() { docker compose exec -T postgres psql -U paycore -d paycore -Atc "$1" 2>/dev/null | tr -d '\r'; }
jget() { python3 -c "import sys,json; d=json.load(sys.stdin); print($1)" 2>/dev/null; }
check() { # check <description> <expected> <observed>
  CHECKS=$((CHECKS+1))
  if [ "$2" = "$3" ]; then printf '  PASS  %-72s expected=%s observed=%s\n' "$1" "$2" "$3"
  else printf '  FAIL  %-72s expected=%s observed=%s\n' "$1" "$2" "$3"; FAILED=$((FAILED+1)); fi
}
check_ge() { CHECKS=$((CHECKS+1)); if [ "$3" -ge "$2" ] 2>/dev/null; then printf '  PASS  %-72s expected>=%s observed=%s\n' "$1" "$2" "$3"; else printf '  FAIL  %-72s expected>=%s observed=%s\n' "$1" "$2" "$3"; FAILED=$((FAILED+1)); fi; }

new_merchant() {
  local r; r=$(curl -s -X POST "$API/api/v1/merchants" -H "X-Admin-Key: $ADMIN" -H 'Content-Type: application/json' -d "{\"name\":\"chaos-$RANDOM\"}")
  MID=$(echo "$r" | jget "d['merchantId']"); MKEY=$(echo "$r" | jget "d['apiKey']")
}
api() { local m=$1 p=$2; shift 2; curl -s -m 15 -X "$m" "$API$p" -H "X-Api-Key: $MKEY" -H 'Content-Type: application/json' "$@"; }
create_payment() { api POST /api/v1/payments -H "Idempotency-Key: $(uuidgen)" -d "{\"amountMinor\":$1,\"currency\":\"USD\",\"customerId\":\"${2:-cust_chaos}\"}" | jget "d['id']"; }
authorize() { api POST "/api/v1/payments/$1/authorize" >/dev/null; }
capture() { api POST "/api/v1/payments/$1/capture" -H "Idempotency-Key: $(uuidgen)" -d '{}'; }
status_of() { api GET "/api/v1/payments/$1" | jget "d['status']"; }
wait_sql() { # wait_sql <sql> <expected> <timeout-seconds>
  local end=$((SECONDS+$3)); local v
  while [ $SECONDS -lt $end ]; do v=$(sql "$1"); [ "$v" = "$2" ] && return 0; sleep 1; done; return 1
}

invariants() { # the financial invariants that must hold after ANY failure
  echo "  -- financial invariants (whole database)"
  check "unbalanced ledger journals" 0 "$(sql "SELECT count(*) FROM (SELECT journal_id FROM ledger_entries GROUP BY journal_id, currency HAVING sum(debit_minor) <> sum(credit_minor)) x")"
  check "duplicate ledger postings per capture/refund" 0 "$(sql "SELECT count(*) FROM (SELECT reference_type, reference_id FROM ledger_journals GROUP BY 1,2 HAVING count(*) > 1) x")"
  check "payments refunded beyond captured" 0 "$(sql "SELECT count(*) FROM payments WHERE refunded_minor > captured_minor")"
  check "payments with more than one live capture" 0 "$(sql "SELECT count(*) FROM (SELECT payment_id FROM captures WHERE status IN ('PENDING','SUCCEEDED') GROUP BY payment_id HAVING count(*) > 1) x")"
  check "CAPTURED payments without a capture journal" 0 "$(sql "SELECT count(*) FROM payments p JOIN captures c ON c.payment_id = p.id AND c.status='SUCCEEDED' LEFT JOIN ledger_journals j ON j.reference_type='CAPTURE' AND j.reference_id=c.id WHERE p.status='CAPTURED' AND j.id IS NULL")"
  check "journals whose capture/refund did not succeed" 0 "$(sql "SELECT count(*) FROM ledger_journals j WHERE (j.reference_type='CAPTURE' AND NOT EXISTS (SELECT 1 FROM captures c WHERE c.id=j.reference_id AND c.status='SUCCEEDED')) OR (j.reference_type='REFUND' AND NOT EXISTS (SELECT 1 FROM refunds r WHERE r.id=j.reference_id AND r.status='SUCCEEDED'))")"
}

wait_api() { for i in $(seq 1 90); do [ "$(curl -s -o /dev/null -w '%{http_code}' -m 3 http://localhost:8081/actuator/health/readiness)" = 200 ] && return 0; sleep 2; done; return 1; }

# ---------------------------------------------------------------------------------------------------------------- A
scenario_worker() {
  echo "== A. kill every payment worker with SIGKILL while payments are being processed"
  new_merchant; local N=24 ids=()
  for i in $(seq 1 $N); do p=$(create_payment $((1000+i)) cust_delayed); ids+=("$p"); authorize "$p"; done
  sleep 1; docker kill $(docker compose ps -q payment-worker) >/dev/null 2>&1
  echo "  workers killed; $(sql "SELECT count(*) FROM payments WHERE merchant_id='$MID' AND status='PROCESSING'") payments still PROCESSING, restarting workers"
  docker compose up -d payment-worker >/dev/null 2>&1
  wait_sql "SELECT count(*) FROM payments WHERE merchant_id='$MID' AND status='AUTHORIZED'" $N 120 || true
  check "payments AUTHORIZED after worker restart (none lost)" $N "$(sql "SELECT count(*) FROM payments WHERE merchant_id='$MID' AND status='AUTHORIZED'")"
  check "authorizations recorded (exactly one per payment)" $N "$(sql "SELECT count(*) FROM authorizations a JOIN payments p ON p.id=a.payment_id WHERE p.merchant_id='$MID'")"
  for p in "${ids[@]}"; do capture "$p" >/dev/null; done
  sleep 1; docker kill $(docker compose ps -q payment-worker) >/dev/null 2>&1; docker compose up -d payment-worker >/dev/null 2>&1
  wait_sql "SELECT count(*) FROM payments WHERE merchant_id='$MID' AND status='CAPTURED'" $N 150 || true
  check "payments CAPTURED after a second kill during capture" $N "$(sql "SELECT count(*) FROM payments WHERE merchant_id='$MID' AND status='CAPTURED'")"
  check "capture journals (exactly one per capture)" $N "$(sql "SELECT count(*) FROM ledger_journals WHERE merchant_id='$MID' AND reference_type='CAPTURE'")"
  check "provider-side captures (no double capture)" $N "$(sql "SELECT count(*) FROM provider_operations po JOIN captures c ON po.operation_id='cap-'||c.id JOIN payments p ON p.id=c.payment_id WHERE p.merchant_id='$MID'")"
  invariants
}

# ---------------------------------------------------------------------------------------------------------------- B
scenario_kafka() {
  echo "== B. stop Kafka after payment state is committed, then restart it"
  new_merchant; docker compose stop kafka >/dev/null 2>&1
  local N=10 codes=""
  for i in $(seq 1 $N); do p=$(create_payment $((2000+i))); [ -n "$p" ] && authorize "$p"; done
  check "payments accepted by the API while Kafka is down (state committed)" $N "$(sql "SELECT count(*) FROM payments WHERE merchant_id='$MID' AND status='PROCESSING'")"
  check_ge "outbox events waiting (committed, not lost)" $((N*2)) "$(sql "SELECT count(*) FROM outbox_events WHERE status='PENDING'")"
  docker compose start kafka >/dev/null 2>&1
  wait_sql "SELECT count(*) FROM payments WHERE merchant_id='$MID' AND status='AUTHORIZED'" $N 180 || true
  check "payments AUTHORIZED after Kafka returned (no event lost)" $N "$(sql "SELECT count(*) FROM payments WHERE merchant_id='$MID' AND status='AUTHORIZED'")"
  wait_sql "SELECT count(*) FROM outbox_events WHERE status='PENDING'" 0 60 || true
  check "outbox backlog drained" 0 "$(sql "SELECT count(*) FROM outbox_events WHERE status='PENDING'")"
  check "authorizations (exactly one per payment)" $N "$(sql "SELECT count(*) FROM authorizations a JOIN payments p ON p.id=a.payment_id WHERE p.merchant_id='$MID'")"
  invariants
}

# ---------------------------------------------------------------------------------------------------------------- C
scenario_postgres() {
  echo "== C. stop PostgreSQL temporarily"
  new_merchant; local p; p=$(create_payment 3000 cust_pg)
  local key; key=$(uuidgen)
  docker compose stop postgres >/dev/null 2>&1; sleep 3
  local code; code=$(curl -s -o /dev/null -w '%{http_code}' -m 15 -X POST "$API/api/v1/payments" -H "X-Api-Key: $MKEY" -H "Idempotency-Key: $key" -H 'Content-Type: application/json' -d '{"amountMinor":3001,"currency":"USD","customerId":"cust_pg"}')
  check "API answers 503 (not 500, not a false success) while the database is down" 503 "$code"
  check "readiness reports DOWN" 503 "$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://localhost:8081/actuator/health/readiness)"
  check "liveness stays UP (process is healthy, dependency is not)" 200 "$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://localhost:8081/actuator/health/liveness)"
  docker compose start postgres >/dev/null 2>&1; wait_api || echo "  (API did not become ready in time)"
  for i in $(seq 1 30); do sql "SELECT 1" >/dev/null && break; sleep 2; done
  local r; r=$(curl -s -o /dev/null -w '%{http_code}' -m 15 -X POST "$API/api/v1/payments" -H "X-Api-Key: $MKEY" -H "Idempotency-Key: $key" -H 'Content-Type: application/json' -d '{"amountMinor":3001,"currency":"USD","customerId":"cust_pg"}')
  check "retry with the SAME Idempotency-Key succeeds after recovery" 201 "$r"
  check "exactly one payment exists for that key (no double create)" 1 "$(sql "SELECT count(*) FROM payments WHERE merchant_id='$MID' AND idempotency_key='$key'")"
  authorize "$p"; wait_sql "SELECT status FROM payments WHERE id='$p'" AUTHORIZED 90 || true
  check "workers resumed: pre-outage payment authorized after recovery" AUTHORIZED "$(sql "SELECT status FROM payments WHERE id='$p'")"
  invariants
}

# ---------------------------------------------------------------------------------------------------------------- redis
scenario_redis() {
  echo "== I. stop Redis (only the rate limiter uses it; correctness must not depend on it)"
  new_merchant; docker compose stop redis >/dev/null 2>&1
  local codes=""; local ok=0
  for i in 1 2 3 4 5; do c=$(curl -s -o /dev/null -w '%{http_code}' -m 15 -X POST "$API/api/v1/payments" -H "X-Api-Key: $MKEY" -H "Idempotency-Key: $(uuidgen)" -H 'Content-Type: application/json' -d '{"amountMinor":500,"currency":"USD","customerId":"cust_redis"}'); [ "$c" = 201 ] && ok=$((ok+1)); done
  check "payments created while Redis is down (rate limiter fails open)" 5 "$ok"
  local p; p=$(create_payment 700 cust_redis); authorize "$p"; wait_sql "SELECT status FROM payments WHERE id='$p'" AUTHORIZED 60 || true; capture "$p" >/dev/null
  wait_sql "SELECT status FROM payments WHERE id='$p'" CAPTURED 60 || true
  check "full payment lifecycle completes with Redis down" CAPTURED "$(sql "SELECT status FROM payments WHERE id='$p'")"
  docker compose start redis >/dev/null 2>&1; invariants
}

# ---------------------------------------------------------------------------------------------------------------- F
scenario_timeout() {
  echo "== F. provider records the capture, then the response is lost (timeout after success)"
  new_merchant; local p; p=$(create_payment 4400 cust_timeout_after_success); authorize "$p"
  wait_sql "SELECT status FROM payments WHERE id='$p'" AUTHORIZED 60 || true
  capture "$p" >/dev/null; wait_sql "SELECT status FROM payments WHERE id='$p'" CAPTURED 90 || true
  local cap; cap=$(sql "SELECT id FROM captures WHERE payment_id='$p'")
  check "payment CAPTURED" CAPTURED "$(sql "SELECT status FROM payments WHERE id='$p'")"
  check "provider capture executed exactly once (looked up, not blindly retried)" 1 "$(sql "SELECT calls FROM provider_call_log WHERE operation_id='cap-$cap'")"
  check "exactly one ledger journal for the capture" 1 "$(sql "SELECT count(*) FROM ledger_journals WHERE reference_type='CAPTURE' AND reference_id='$cap'")"
  invariants
}

# ---------------------------------------------------------------------------------------------------------------- G
scenario_concurrent_captures() {
  echo "== G. 20 concurrent capture requests (different idempotency keys) for ONE payment"
  new_merchant; local p; p=$(create_payment 6000 cust_conc); authorize "$p"; wait_sql "SELECT status FROM payments WHERE id='$p'" AUTHORIZED 60 || true
  local tmp; tmp=$(mktemp -d)
  for i in $(seq 1 20); do ( curl -s -o /dev/null -w '%{http_code}\n' -m 20 -X POST "$API/api/v1/payments/$p/capture" -H "X-Api-Key: $MKEY" -H "Idempotency-Key: $(uuidgen)" -H 'Content-Type: application/json' -d '{}' > "$tmp/$i" ) & done; wait
  echo "  HTTP status histogram of the 20 requests: $(cat "$tmp"/* | sort | uniq -c | tr '\n' ' ')"
  local accepted; accepted=$(cat "$tmp"/* | grep -c '^202$'); local refused; refused=$(cat "$tmp"/* | grep -c '^409$'); rm -rf "$tmp"
  check "capture requests accepted (202)" 1 "$accepted"
  check "capture requests refused (409)" 19 "$refused"
  wait_sql "SELECT status FROM payments WHERE id='$p'" CAPTURED 60 || true
  check "live captures in the database" 1 "$(sql "SELECT count(*) FROM captures WHERE payment_id='$p' AND status IN ('PENDING','SUCCEEDED')")"
  check "ledger journals for the payment" 1 "$(sql "SELECT count(*) FROM ledger_journals j JOIN captures c ON c.id=j.reference_id WHERE c.payment_id='$p'")"
  check "merchant payable balance (6000 captured once, not 20 times)" 6000 "$(api GET /api/v1/ledger/balance | jget "d['merchantPayableMinor']")"
  invariants
}

# ---------------------------------------------------------------------------------------------------------------- H
scenario_concurrent_refunds() {
  echo "== H. 20 concurrent refund requests of 700 against a 5000 capture"
  new_merchant; local p; p=$(create_payment 5000 cust_conc2); authorize "$p"; wait_sql "SELECT status FROM payments WHERE id='$p'" AUTHORIZED 60 || true
  capture "$p" >/dev/null; wait_sql "SELECT status FROM payments WHERE id='$p'" CAPTURED 60 || true
  local tmp; tmp=$(mktemp -d)
  for i in $(seq 1 20); do ( curl -s -o /dev/null -w '%{http_code}\n' -m 20 -X POST "$API/api/v1/payments/$p/refunds" -H "X-Api-Key: $MKEY" -H "Idempotency-Key: $(uuidgen)" -H 'Content-Type: application/json' -d '{"amountMinor":700}' > "$tmp/$i" ) & done; wait
  echo "  HTTP status histogram of the 20 requests: $(cat "$tmp"/* | sort | uniq -c | tr '\n' ' ')"
  local accepted; accepted=$(cat "$tmp"/* | grep -c '^202$'); local refused; refused=$(cat "$tmp"/* | grep -c '^422$'); rm -rf "$tmp"
  check "refunds accepted (5000 / 700 = 7 whole refunds)" 7 "$accepted"
  check "refunds refused as exceeding the remaining amount" 13 "$refused"
  wait_sql "SELECT count(*) FROM refunds WHERE payment_id='$p' AND status='PENDING'" 0 90 || true
  check "refunded amount on the payment" 4900 "$(sql "SELECT refunded_minor FROM payments WHERE id='$p'")"
  check "refund journals" 7 "$(sql "SELECT count(*) FROM ledger_journals WHERE reference_type='REFUND' AND merchant_id='$MID'")"
  check "merchant payable balance from the ledger (5000 - 4900)" 100 "$(api GET /api/v1/ledger/balance | jget "d['merchantPayableMinor']")"
  invariants
}

# ---------------------------------------------------------------------------------------------------------------- J
scenario_api_kill() {
  echo "== J. kill -9 the API process while 200 payment-creation requests are in flight, then retry every request with its SAME Idempotency-Key"
  new_merchant; local N=200 tmp; tmp=$(mktemp -d)
  for i in $(seq 1 $N); do echo "$(uuidgen)" > "$tmp/key$i"; done
  for i in $(seq 1 $N); do ( curl -s -o /dev/null -w '%{http_code}\n' -m 8 -X POST "$API/api/v1/payments" -H "X-Api-Key: $MKEY" -H "Idempotency-Key: $(cat "$tmp/key$i")" -H 'Content-Type: application/json' -d "{\"amountMinor\":$((100+i)),\"currency\":\"USD\",\"customerId\":\"cust_apikill\"}" > "$tmp/first$i" 2>/dev/null ) & done
  # kill the API once about a quarter of the requests have committed, so the rest are genuinely in flight
  for t in $(seq 1 200); do [ "$(sql "SELECT count(*) FROM payments WHERE merchant_id='$MID'")" -ge $((N/4)) ] && break; sleep 0.05; done
  docker kill $(docker compose ps -q payment-api) >/dev/null 2>&1
  wait
  local before; before=$(sql "SELECT count(*) FROM payments WHERE merchant_id='$MID'")
  echo "  API killed mid-flight: $before of $N requests had committed before the kill; first-attempt statuses: $(cat "$tmp"/first* | sort | uniq -c | tr '\n' ' ')"
  docker compose up -d payment-api >/dev/null 2>&1; wait_api || echo "  (API did not become ready in time)"
  local created=0 replayed=0
  for i in $(seq 1 $N); do
    code=$(curl -s -o /dev/null -D "$tmp/h$i" -w '%{http_code}' -m 15 -X POST "$API/api/v1/payments" -H "X-Api-Key: $MKEY" -H "Idempotency-Key: $(cat "$tmp/key$i")" -H 'Content-Type: application/json' -d "{\"amountMinor\":$((100+i)),\"currency\":\"USD\",\"customerId\":\"cust_apikill\"}")
    [ "$code" = 201 ] && created=$((created+1)); grep -qi 'idempotent-replayed: true' "$tmp/h$i" && replayed=$((replayed+1))
  done
  echo "  retries: $replayed were replays of a request that had already committed; $((created-replayed)) were executed now"
  check "every retried request returned 201" $N "$created"
  check "payments for this merchant (exactly one per key, none duplicated)" $N "$(sql "SELECT count(*) FROM payments WHERE merchant_id='$MID'")"
  check "distinct idempotency keys among those payments" $N "$(sql "SELECT count(DISTINCT idempotency_key) FROM payments WHERE merchant_id='$MID'")"
  check "requests that had committed before the kill were replayed, not repeated" "$before" "$replayed"
  rm -rf "$tmp"; invariants
}

run() { case "$1" in
  worker) scenario_worker;; kafka) scenario_kafka;; postgres) scenario_postgres;; redis) scenario_redis;; timeout) scenario_timeout;;
  api-kill) scenario_api_kill;; concurrent-captures) scenario_concurrent_captures;; concurrent-refunds) scenario_concurrent_refunds;;
  all) for s in concurrent-captures concurrent-refunds timeout redis worker kafka postgres api-kill; do run $s; echo; done;;
  *) echo "unknown scenario: $1"; exit 2;; esac; }

run "${1:-all}"
echo "SUMMARY: $((CHECKS-FAILED)) of $CHECKS checks passed, $FAILED failed"
[ "$FAILED" -eq 0 ]
