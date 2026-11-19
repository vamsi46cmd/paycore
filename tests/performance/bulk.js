// k6: bulk load for the optional 10,000 / 100,000 payment runs. Each iteration creates, authorizes and requests the capture of one payment; scripts/bulk.sh then measures how long the asynchronous workers take to drain the backlog.
//   k6 run -e COUNT=10000 -e VUS=50 -e OUT=bulk.json tests/performance/bulk.js
import http from 'k6/http';
import { sleep } from 'k6';
import { Counter } from 'k6/metrics';

const API = __ENV.API || 'http://localhost:8080';
const ADMIN = __ENV.ADMIN_KEY || 'dev-only-admin-key-change-me';
const VUS = Number(__ENV.VUS || 50);
const COUNT = Number(__ENV.COUNT || 10000);

export const options = {
  scenarios: { bulk: { executor: 'shared-iterations', vus: VUS, iterations: COUNT, maxDuration: '30m' } },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
};
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }));
const submitted = new Counter('payments_submitted');

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => { const r = (Math.random() * 16) | 0; return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16); });
}

export function setup() {
  const keys = [];
  for (let i = 0; i < VUS; i++) {
    const r = http.post(`${API}/api/v1/merchants`, JSON.stringify({ name: `bulk-${Date.now()}-${i}` }), { headers: { 'Content-Type': 'application/json', 'X-Admin-Key': ADMIN } });
    keys.push(r.json('apiKey'));
  }
  return { keys };
}

export default function (data) {
  const h = { 'Content-Type': 'application/json', 'X-Api-Key': data.keys[(__VU - 1) % data.keys.length] };
  const c = http.post(`${API}/api/v1/payments`, JSON.stringify({ amountMinor: 1000 + (__ITER % 5000), currency: 'USD', customerId: 'cust_bulk' }), { headers: { ...h, 'Idempotency-Key': uuid() }, tags: { name: 'POST /payments' } });
  if (c.status !== 201) return;
  const id = c.json('id');
  http.post(`${API}/api/v1/payments/${id}/authorize`, null, { headers: h, tags: { name: 'POST authorize' } });
  // wait for AUTHORIZED (asynchronous), then request the capture; the final CAPTURED state is awaited by scripts/bulk.sh
  for (let i = 0; i < 200; i++) {
    const g = http.get(`${API}/api/v1/payments/${id}`, { headers: h, tags: { name: 'GET /payments/{id}' } });
    if (g.status === 200 && g.json('status') === 'AUTHORIZED') {
      const cap = http.post(`${API}/api/v1/payments/${id}/capture`, '{}', { headers: { ...h, 'Idempotency-Key': uuid() }, tags: { name: 'POST capture' } });
      if (cap.status === 202) submitted.add(1);
      return;
    }
    sleep(0.1);
  }
}

export function handleSummary(data) {
  const m = data.metrics;
  const out = { vus: VUS, requestedPayments: COUNT, paymentsSubmitted: m.payments_submitted ? m.payments_submitted.values.count : 0,
    httpRequests: m.http_reqs.values.count, httpErrorRatePercent: +(m.http_req_failed.values.rate * 100).toFixed(3),
    apiLatencyMs: { p50: +m.http_req_duration.values['p(50)'].toFixed(1), p95: +m.http_req_duration.values['p(95)'].toFixed(1), p99: +m.http_req_duration.values['p(99)'].toFixed(1) },
    submitSeconds: +(data.state.testRunDurationMs / 1000).toFixed(1) };
  return { [__ENV.OUT || 'bulk.json']: JSON.stringify(out, null, 2), stdout: JSON.stringify(out) + '\n' };
}
