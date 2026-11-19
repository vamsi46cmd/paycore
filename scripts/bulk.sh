#!/usr/bin/env bash
# Optional bulk run. Usage: scripts/bulk.sh <payments> [vus]     e.g. scripts/bulk.sh 10000 50
# Submits <payments> full lifecycles (create -> authorize -> capture), then waits for the asynchronous workers to drain, reports measured
# throughput, runs reconciliation and verifies the financial invariants. Writes docs/benchmarks/bulk-<payments>.json.
set -euo pipefail
cd "$(dirname "$0")/.."
count=${1:?payments}; vus=${2:-50}
psqlc() { docker compose exec -T postgres psql -U paycore -d paycore -Atc "$1" | tr -d '\r'; }
out=docs/benchmarks/bulk-$count.json
start_ts=$(psqlc "select now()")
t0=$(date +%s)
k6 run --quiet -e COUNT=$count -e VUS=$vus -e OUT="$out.k6" tests/performance/bulk.js >/dev/null || true
t_submit=$(date +%s)
want=$(python3 -c "import json;print(json.load(open('$out.k6'))['paymentsSubmitted'])")
deadline=$((t_submit+1800))
while [ $(date +%s) -lt $deadline ]; do
  done_n=$(psqlc "select count(*) from payments where created_at >= '$start_ts' and status='CAPTURED'")
  [ "$done_n" -ge "$want" ] && break
  sleep 2
done
t_done=$(date +%s)
recon=$(curl -s -X POST "http://localhost:8080/api/v1/reconciliation/runs" -H "X-Admin-Key: ${ADMIN_KEY:-dev-only-admin-key-change-me}" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
python3 - "$out" "$count" "$want" "$done_n" "$((t_submit-t0))" "$((t_done-t0))" \
  "$(psqlc "select count(*) from reconciliation_discrepancies d join payments p on p.id=d.payment_id where d.run_id='$recon' and p.created_at >= '$start_ts'")" \
  "$(psqlc "select count(*) from ledger_journals j join captures c on c.id=j.reference_id join payments p on p.id=c.payment_id where j.reference_type='CAPTURE' and p.created_at >= '$start_ts'")" \
  "$(psqlc "select count(*) from (select journal_id from ledger_entries group by journal_id, currency having sum(debit_minor) <> sum(credit_minor)) x")" \
  "$(psqlc "select count(*) from (select reference_type, reference_id from ledger_journals group by 1,2 having count(*) > 1) x")" \
  "$(psqlc "select count(*) from payments where created_at >= '$start_ts' and status not in ('CAPTURED')")" <<'PY'
import json, sys
out, count, want, done, sub_s, total_s, disc, journals, unbal, dups, notcaptured = sys.argv[1:12]
r = json.load(open(out + ".k6"))
done, total_s, sub_s = int(done), int(total_s), int(sub_s)
r.update({"requestedPayments": int(count), "capturedPayments": done, "submitPhaseSeconds": sub_s, "totalSecondsUntilAllCaptured": total_s,
          "endToEndThroughputPaymentsPerSecond": round(done / total_s, 2) if total_s else None,
          "reconciliationDiscrepanciesForThisRun": int(disc), "captureJournalsForThisRun": int(journals),
          "unbalancedJournalsAnywhere": int(unbal), "duplicatePostingsAnywhere": int(dups), "paymentsNotCapturedFromThisRun": int(notcaptured)})
json.dump(r, open(out, "w"), indent=2); print(json.dumps(r, indent=1))
PY
rm -f "$out.k6"
