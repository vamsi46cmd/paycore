# ADR 0004: Double-entry ledger design

## Status
Accepted.

## Model (all amounts are integer minor units, USD, scale 2; no rounding is ever needed)
- Accounts: one `SETTLEMENT_CLEARING` (asset, debit-normal: balance = debits - credits) and one `MERCHANT_PAYABLE` per merchant (liability, credit-normal: balance = credits - debits).
- Capture of N: DEBIT clearing N, CREDIT merchant payable N. Refund of N: DEBIT merchant payable N, CREDIT clearing N.
  These are illustrative simulation conventions, not real settlement accounting.
- Corrections: a REVERSAL journal with every entry's debit/credit swapped and `reverses_journal_id` pointing at the original (unique, so a journal is reversed at most once).
  History is never edited.

## Enforcement in the database
- Deferred constraint trigger: at commit, every journal has at least two entries and debits = credits per currency. A journal that fails never becomes visible.
- Immutability triggers reject UPDATE, DELETE and TRUNCATE on journals and entries.
- `UNIQUE (reference_type, reference_id)`: a capture/refund posts once; replays return the original journal.
- Entries are one-sided and positive (CHECK), USD only.
- No stored balance column: balances are derived from entries, so concurrent postings never contend on a hot row. Cost: balance queries scan entries (index on account);
  a materialized per-account balance would be the next optimization.

## Consequences
Unbalanced or duplicate postings are structurally impossible, and reconciliation can independently re-verify them. Verified by raw-SQL attacks (`SchemaInvariantIT`) and by concurrent capture/refund stress tests.
