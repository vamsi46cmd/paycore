#!/usr/bin/env bash
# Reproducible benchmark. Usage: scripts/benchmark.sh <label> <vus> <duration>      e.g. scripts/benchmark.sh light-10 10 30s
# Runs tests/performance/load.js against the running docker compose stack, samples container CPU/memory, DB connections,
# Kafka consumer lag and outbox backlog, then verifies the financial invariants for the payments the run created.
# Writes docs/benchmarks/<label>.json. Requires: docker compose stack up, k6, python3.
set -euo pipefail
cd "$(dirname "$0")/.."
label=${1:?label}; vus=${2:?vus}; duration=${3:-30s}
out=docs/benchmarks/$label.json; mkdir -p docs/benchmarks
psqlc() { docker compose exec -T postgres psql -U paycore -d paycore -Atc "$1" | tr -d '\r'; }
start_epoch=$(date +%s)
secs=${duration%s}
( sleep $((secs/2)); docker stats --no-stream --format '{"name":"{{.Name}}","cpu":"{{.CPUPerc}}","mem":"{{.MemUsage}}"}' > "docs/benchmarks/$label.stats.jsonl"; psqlc "select count(*) from pg_stat_activity where datname='paycore'" > "docs/benchmarks/$label.dbconns.txt" ) &
k6 run --quiet -e VUS=$vus -e DURATION=$duration -e OUT="$out.k6" tests/performance/load.js >/dev/null || true
wait
end_epoch=$(date +%s); window=$((end_epoch-start_epoch+30))
prom() { curl -s -G localhost:9090/api/v1/query --data-urlencode "query=$1" | python3 -c "import sys,json; r=json.load(sys.stdin)['data']['result']; print(max([float(x['value'][1]) for x in r], default=float('nan')))" 2>/dev/null || echo nan; }
python3 - "$out" "$label" "$vus" "$(prom "max_over_time(paycore_kafka_consumer_lag[${window}s])")" "$(prom "max_over_time(paycore_outbox_backlog[${window}s])")" \
  "$(psqlc "select count(*) from (select journal_id from ledger_entries group by journal_id, currency having sum(debit_minor) <> sum(credit_minor)) x")" \
  "$(psqlc "select count(*) from (select reference_type, reference_id from ledger_journals group by 1,2 having count(*) > 1) x")" \
  "$(psqlc "select count(*) from payments where refunded_minor > captured_minor")" \
  "$(psqlc "select count(*) from payments p join captures c on c.payment_id=p.id and c.status='SUCCEEDED' left join ledger_journals j on j.reference_type='CAPTURE' and j.reference_id=c.id where p.status='CAPTURED' and j.id is null")" <<'PY'
import json, sys
out, label, vus, lag, backlog, unbalanced, dups, overref, nojournal = sys.argv[1:10]
r = json.load(open(out + ".k6"))
stats = [json.loads(l) for l in open(f"docs/benchmarks/{label}.stats.jsonl")]
r.update({"label": label, "peakKafkaConsumerLagDuringRun": lag, "peakOutboxBacklogDuringRun": backlog,
          "dbConnectionsMidRun": int(open(f"docs/benchmarks/{label}.dbconns.txt").read().strip()),
          "containerSamplesMidRun": [s for s in stats if any(k in s["name"] for k in ("payment-api", "payment-worker", "webhook-worker", "postgres", "kafka", "redis"))],
          "invariantsAfterRun": {"unbalancedJournals": int(unbalanced), "duplicatePostings": int(dups), "overRefundedPayments": int(overref), "capturedWithoutJournal": int(nojournal)}})
json.dump(r, open(out, "w"), indent=2)
print(json.dumps({k: r[k] for k in ("label", "vus", "paymentsCaptured", "paymentsCapturedPerSecond", "httpRequestsPerSecond", "apiLatencyMs", "completionLatencyMs_createToCaptured", "httpErrorRatePercent", "flowsCompletedPercent", "duplicateChecks", "duplicateMismatches", "peakKafkaConsumerLagDuringRun", "peakOutboxBacklogDuringRun", "dbConnectionsMidRun", "invariantsAfterRun")}, indent=1))
PY
rm -f "$out.k6" "docs/benchmarks/$label.stats.jsonl" "docs/benchmarks/$label.dbconns.txt"
