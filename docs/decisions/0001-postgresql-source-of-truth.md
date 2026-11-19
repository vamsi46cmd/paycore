# ADR 0001: PostgreSQL is the financial source of truth

## Status
Accepted.

## Context
Payment state, the ledger, idempotency records and the outbox all describe money. Several changes must be atomic: a capture
changes the payment, completes the capture row, posts a balanced ledger journal and queues an event, or none of those happen.

## Decision
Everything financial lives in one PostgreSQL database, and the rules that matter are enforced **by the database**, not only by
application code:
- `CHECK (refunded_minor <= captured_minor)`: over-refunding is impossible even with a buggy or racing application.
- A deferred constraint trigger refuses to commit a ledger journal that is unbalanced per currency or has fewer than two entries.
- Triggers make ledger rows immutable (update, delete, truncate are rejected) and validate payment state transitions.
- `UNIQUE (reference_type, reference_id)` on journals: a capture or refund can be posted once; replays and duplicate events collide.
- A partial unique index allows at most one live capture per payment.
Redis is never a source of truth: it only backs a courtesy rate limiter, and the system is correct with Redis down.
Kafka carries events but holds no authoritative state.

## Alternatives considered
- Event sourcing on Kafka: elegant, but financial invariants (balanced journals, refund ceilings) are then enforced in consumers,
  with weaker guarantees than a transactional database constraint.
- A separate ledger database: stronger isolation, but loses single-transaction atomicity between payment state and ledger postings.

## Consequences
- Strong, simple correctness arguments: invariants hold even if application code is wrong. They are tested directly with raw SQL (`SchemaInvariantIT`).
- Vertical scaling first; throughput is bounded by one primary database. Partitioning by merchant or read replicas are the next steps.
- Plain SQL (JdbcTemplate) was used instead of JPA to keep locking and conditional updates explicit.
