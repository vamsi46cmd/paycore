// k6: full payment lifecycle per iteration, one merchant per virtual user.
//   k6 run -e VUS=50 -e DURATION=30s -e OUT=result.json tests/performance/load.js
// Per iteration: create payment -> replay the same request (must return the SAME payment: duplicate-prevention check) ->
// authorize -> wait AUTHORIZED -> capture -> replay capture (same captureId) -> wait CAPTURED. "completion_ms" is create -> CAPTURED.
// Only 2xx responses count as expected; anything else is an error.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const API = __ENV.API || 'http://localhost:8080';
const ADMIN = __ENV.ADMIN_KEY || 'dev-only-admin-key-change-me';
const VUS = Number(__ENV.VUS || 10);

export const options = {
  scenarios: { lifecycle: { executor: 'constant-vus', vus: VUS, duration: __ENV.DURATION || '30s', gracefulStop: '90s' } },
  thresholds: { duplicate_mismatches: ['count==0'], http_req_failed: ['rate<0.01'], flow_ok: ['rate>0.99'] },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }));

const createMs = new Trend('create_ms', true);
const completionMs = new Trend('completion_ms', true);
const duplicateMismatches = new Counter('duplicate_mismatches');
const duplicatesChecked = new Counter('duplicates_checked');
const paymentsCaptured = new Counter('payments_captured');
const flowOk = new Rate('flow_ok');

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

export function setup() {
  const keys = [];
  for (let i = 0; i < VUS; i++) {
    const r = http.post(`${API}/api/v1/merchants`, JSON.stringify({ name: `k6-${Date.now()}-${i}` }), { headers: { 'Content-Type': 'application/json', 'X-Admin-Key': ADMIN } });
    if (r.status !== 201) throw new Error(`merchant creation failed: ${r.status} ${r.body}`);
    keys.push(r.json('apiKey'));
  }
  return { keys };
}

function waitFor(id, status, headers, timeoutMs) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    const r = http.get(`${API}/api/v1/payments/${id}`, { headers, tags: { name: 'GET /payments/{id}' } });
    if (r.status === 200 && r.json('status') === status) return true;
    sleep(0.1);
  }
  return false;
}

export default function (data) {
  const apiKey = data.keys[(__VU - 1) % data.keys.length];
  const headers = { 'Content-Type': 'application/json', 'X-Api-Key': apiKey };
  const t0 = Date.now();
  let ok = false;

  const idem = uuid();
  const body = JSON.stringify({ amountMinor: 1000 + Math.floor(Math.random() * 9000), currency: 'USD', customerId: 'cust_load' });
  const created = http.post(`${API}/api/v1/payments`, body, { headers: { ...headers, 'Idempotency-Key': idem }, tags: { name: 'POST /payments' } });
  createMs.add(created.timings.duration);
  if (created.status === 201) {
    const id = created.json('id');
    // duplicate prevention: the same request again must return the same payment
    const replay = http.post(`${API}/api/v1/payments`, body, { headers: { ...headers, 'Idempotency-Key': idem }, tags: { name: 'POST /payments (replay)' } });
    duplicatesChecked.add(1);
    if (replay.status !== 201 || replay.json('id') !== id) duplicateMismatches.add(1);

    const auth = http.post(`${API}/api/v1/payments/${id}/authorize`, null, { headers, tags: { name: 'POST authorize' } });
    if (auth.status === 202 && waitFor(id, 'AUTHORIZED', headers, 30000)) {
      const capKey = uuid();
      const cap = http.post(`${API}/api/v1/payments/${id}/capture`, '{}', { headers: { ...headers, 'Idempotency-Key': capKey }, tags: { name: 'POST capture' } });
      if (cap.status === 202) {
        const capReplay = http.post(`${API}/api/v1/payments/${id}/capture`, '{}', { headers: { ...headers, 'Idempotency-Key': capKey }, tags: { name: 'POST capture (replay)' } });
        duplicatesChecked.add(1);
        if (capReplay.status !== 202 || capReplay.json('captureId') !== cap.json('captureId')) duplicateMismatches.add(1);
        if (waitFor(id, 'CAPTURED', headers, 30000)) {
          ok = true;
          paymentsCaptured.add(1);
          completionMs.add(Date.now() - t0);
        }
      }
    }
  }
  flowOk.add(ok);
  check(ok, { 'payment reached CAPTURED': (v) => v });
}

export function handleSummary(data) {
  const m = data.metrics;
  const t = (name) => (m[name] ? { avg: +m[name].values.avg.toFixed(1), p50: +m[name].values['p(50)'].toFixed(1), p95: +m[name].values['p(95)'].toFixed(1), p99: +m[name].values['p(99)'].toFixed(1), max: +m[name].values.max.toFixed(1) } : null);
  const out = {
    vus: VUS,
    duration: __ENV.DURATION || '30s',
    paymentsCaptured: m.payments_captured ? m.payments_captured.values.count : 0,
    paymentsCapturedPerSecond: m.payments_captured ? +m.payments_captured.values.rate.toFixed(2) : 0,
    httpRequests: m.http_reqs.values.count,
    httpRequestsPerSecond: +m.http_reqs.values.rate.toFixed(1),
    apiLatencyMs: t('http_req_duration'),
    paymentCreateLatencyMs: t('create_ms'),
    completionLatencyMs_createToCaptured: t('completion_ms'),
    httpErrorRatePercent: +(m.http_req_failed.values.rate * 100).toFixed(3),
    flowsCompletedPercent: +(m.flow_ok.values.rate * 100).toFixed(2),
    duplicateChecks: m.duplicates_checked ? m.duplicates_checked.values.count : 0,
    duplicateMismatches: m.duplicate_mismatches ? m.duplicate_mismatches.values.count : 0,
  };
  return { [__ENV.OUT || 'result.json']: JSON.stringify(out, null, 2), stdout: JSON.stringify(out) + '\n' };
}
