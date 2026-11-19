# ADR 0003: Idempotency enforced in PostgreSQL, in the same transaction as the effect

## Status
Accepted.

## Decision
Idempotency records live in `idempotency_keys`, keyed by (merchant, operation, key), with a canonical-JSON SHA-256 fingerprint of the request.
The key row is inserted in the same transaction as the business effect and then updated with the response:
- Success commits effect, key and replayable response together.
- Any failure or crash rolls everything back, so no key exists and a retry re-executes cleanly (verified).
- Two simultaneous requests with one key: the second `INSERT ... ON CONFLICT DO NOTHING` blocks on the first's uncommitted row, then sees the conflict and replays
  the stored response, or gets HTTP 409 if the fingerprint differs.
- Keys are scoped per merchant and operation. Expiry is 24 h (a purge job deletes expired rows). After expiry, a payment still carries its key (`UNIQUE (merchant_id, idempotency_key)`),
  so reusing the key creates nothing and returns 409 `idempotency_key_reused`.
- Only successful (2xx) outcomes are stored. Deterministic errors (for example 422 refund over limit) are re-evaluated on retry.

## Why not Redis or an in-memory cache
They lose state on restart or eviction and cannot be committed atomically with the payment. A duplicate charge after a cache miss is exactly the failure to prevent.

## Related layers (defence in depth)
Idempotency prevents duplicate API requests; separate guards prevent duplicates elsewhere: one live capture per payment (partial unique index), unique ledger reference per capture/refund,
processed-event markers for consumers, and stable provider operation ids so the provider never executes an operation twice.
