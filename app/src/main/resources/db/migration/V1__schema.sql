-- PayCore schema. Money is stored as integer minor units (USD cents) in bigint columns; there are no floating-point columns.
-- Financial invariants are enforced HERE (constraints and triggers), not only in application code.

CREATE TABLE merchants (
    id           uuid PRIMARY KEY,
    name         varchar(100) NOT NULL,
    api_key_hash char(64)     NOT NULL UNIQUE,
    created_at   timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE payments (
    id              uuid PRIMARY KEY,
    merchant_id     uuid         NOT NULL REFERENCES merchants (id),
    customer_id     varchar(64)  NOT NULL,
    amount_minor    bigint       NOT NULL CHECK (amount_minor > 0),
    currency        char(3)      NOT NULL CHECK (currency = 'USD'),
    status          varchar(12)  NOT NULL CHECK (status IN ('CREATED', 'PROCESSING', 'AUTHORIZED', 'CAPTURED', 'FAILED', 'CANCELED')),
    authorized_minor bigint      NOT NULL DEFAULT 0 CHECK (authorized_minor >= 0),
    captured_minor  bigint       NOT NULL DEFAULT 0 CHECK (captured_minor >= 0 AND captured_minor <= authorized_minor),
    -- refunded_minor = refunds that are PENDING or SUCCEEDED (a reservation). The CHECK makes over-refunding impossible,
    -- even under concurrent requests or application bugs.
    refunded_minor  bigint       NOT NULL DEFAULT 0 CHECK (refunded_minor >= 0 AND refunded_minor <= captured_minor),
    failure_code    varchar(50),
    idempotency_key varchar(100) NOT NULL,
    version         bigint       NOT NULL DEFAULT 0,
    created_at      timestamptz  NOT NULL DEFAULT now(),
    updated_at      timestamptz  NOT NULL DEFAULT now(),
    UNIQUE (merchant_id, idempotency_key)
);
CREATE INDEX payments_merchant_created_idx ON payments (merchant_id, created_at DESC, id);
CREATE INDEX payments_status_updated_idx ON payments (status, updated_at);

-- Valid transitions only; money-defining columns are immutable; captured_minor never decreases.
CREATE FUNCTION payments_guard() RETURNS trigger AS $$
BEGIN
    IF NEW.amount_minor <> OLD.amount_minor OR NEW.currency <> OLD.currency OR NEW.merchant_id <> OLD.merchant_id THEN
        RAISE EXCEPTION 'payment amount, currency and merchant are immutable' USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.captured_minor < OLD.captured_minor THEN
        RAISE EXCEPTION 'captured amount cannot decrease' USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.status <> OLD.status AND NOT (
           (OLD.status = 'CREATED'    AND NEW.status = 'PROCESSING')
        OR (OLD.status = 'PROCESSING' AND NEW.status IN ('AUTHORIZED', 'FAILED'))
        OR (OLD.status = 'AUTHORIZED' AND NEW.status IN ('CAPTURED', 'CANCELED'))) THEN
        RAISE EXCEPTION 'invalid payment transition % -> %', OLD.status, NEW.status USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER payments_guard BEFORE UPDATE ON payments FOR EACH ROW EXECUTE FUNCTION payments_guard();

CREATE TABLE payment_attempts (
    id                 uuid PRIMARY KEY,
    payment_id         uuid        NOT NULL REFERENCES payments (id),
    operation          varchar(12) NOT NULL,
    attempt_number     int         NOT NULL,
    provider_reference varchar(100),
    status             varchar(20) NOT NULL,
    error_code         varchar(50),
    created_at         timestamptz NOT NULL DEFAULT now(),
    completed_at       timestamptz,
    UNIQUE (payment_id, operation, attempt_number)
);

CREATE TABLE authorizations (
    id                 uuid PRIMARY KEY,
    payment_id         uuid        NOT NULL UNIQUE REFERENCES payments (id),
    authorized_minor   bigint      NOT NULL CHECK (authorized_minor > 0),
    status             varchar(12) NOT NULL CHECK (status IN ('AUTHORIZED', 'VOIDED')),
    provider_reference varchar(100),
    created_at         timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE captures (
    id                 uuid PRIMARY KEY,
    payment_id         uuid         NOT NULL REFERENCES payments (id),
    amount_minor       bigint       NOT NULL CHECK (amount_minor > 0),
    status             varchar(12)  NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED')),
    provider_reference varchar(100),
    failure_code       varchar(50),
    idempotency_key    varchar(100) NOT NULL,
    created_at         timestamptz  NOT NULL DEFAULT now(),
    completed_at       timestamptz
);
-- At most ONE live capture per payment: concurrent capture requests cannot both win.
CREATE UNIQUE INDEX captures_one_live_per_payment ON captures (payment_id) WHERE status IN ('PENDING', 'SUCCEEDED');

CREATE TABLE refunds (
    id                 uuid PRIMARY KEY,
    payment_id         uuid         NOT NULL REFERENCES payments (id),
    amount_minor       bigint       NOT NULL CHECK (amount_minor > 0),
    status             varchar(12)  NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED')),
    provider_reference varchar(100),
    failure_code       varchar(50),
    idempotency_key    varchar(100) NOT NULL,
    created_at         timestamptz  NOT NULL DEFAULT now(),
    completed_at       timestamptz,
    UNIQUE (payment_id, idempotency_key)
);

-- Idempotency: the record is written in the SAME transaction as the business effect and its response.
CREATE TABLE idempotency_keys (
    merchant_id     uuid         NOT NULL,
    operation       varchar(120) NOT NULL,
    idem_key        varchar(100) NOT NULL,
    fingerprint     char(64)     NOT NULL,
    response_status int          NOT NULL,
    response_body   text         NOT NULL,
    created_at      timestamptz  NOT NULL DEFAULT now(),
    expires_at      timestamptz  NOT NULL,
    PRIMARY KEY (merchant_id, operation, idem_key)
);
CREATE INDEX idempotency_expires_idx ON idempotency_keys (expires_at);

-- ============================== double-entry ledger ==============================
CREATE TABLE ledger_accounts (
    id           uuid PRIMARY KEY,
    account_type varchar(30) NOT NULL CHECK (account_type IN ('SETTLEMENT_CLEARING', 'MERCHANT_PAYABLE')),
    merchant_id  uuid REFERENCES merchants (id),
    currency     char(3)     NOT NULL CHECK (currency = 'USD'),
    created_at   timestamptz NOT NULL DEFAULT now(),
    CHECK ((account_type = 'MERCHANT_PAYABLE') = (merchant_id IS NOT NULL))
);
CREATE UNIQUE INDEX ledger_accounts_unique ON ledger_accounts (account_type, COALESCE(merchant_id, '00000000-0000-0000-0000-000000000000'::uuid), currency);

CREATE TABLE ledger_journals (
    id                 uuid PRIMARY KEY,
    reference_type     varchar(20) NOT NULL CHECK (reference_type IN ('CAPTURE', 'REFUND', 'REVERSAL')),
    reference_id       uuid        NOT NULL,
    merchant_id        uuid REFERENCES merchants (id),
    reverses_journal_id uuid UNIQUE REFERENCES ledger_journals (id),
    description        varchar(200),
    created_at         timestamptz NOT NULL DEFAULT now(),
    -- A financial event can be posted exactly once: replays and duplicate events collide here.
    UNIQUE (reference_type, reference_id)
);

CREATE TABLE ledger_entries (
    id           uuid PRIMARY KEY,
    journal_id   uuid        NOT NULL REFERENCES ledger_journals (id),
    account_id   uuid        NOT NULL REFERENCES ledger_accounts (id),
    debit_minor  bigint      NOT NULL DEFAULT 0 CHECK (debit_minor >= 0),
    credit_minor bigint      NOT NULL DEFAULT 0 CHECK (credit_minor >= 0),
    currency     char(3)     NOT NULL CHECK (currency = 'USD'),
    created_at   timestamptz NOT NULL DEFAULT now(),
    CHECK ((debit_minor > 0 AND credit_minor = 0) OR (credit_minor > 0 AND debit_minor = 0))
);
CREATE INDEX ledger_entries_journal_idx ON ledger_entries (journal_id);
CREATE INDEX ledger_entries_account_idx ON ledger_entries (account_id);

-- Posted history is immutable: corrections are new (reversing) journals, never edits.
CREATE FUNCTION ledger_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'ledger rows are immutable (% on %)', TG_OP, TG_TABLE_NAME USING ERRCODE = 'restrict_violation';
END $$ LANGUAGE plpgsql;
CREATE TRIGGER ledger_entries_immutable BEFORE UPDATE OR DELETE ON ledger_entries FOR EACH ROW EXECUTE FUNCTION ledger_immutable();
CREATE TRIGGER ledger_journals_immutable BEFORE UPDATE OR DELETE ON ledger_journals FOR EACH ROW EXECUTE FUNCTION ledger_immutable();
CREATE TRIGGER ledger_entries_no_truncate BEFORE TRUNCATE ON ledger_entries FOR EACH STATEMENT EXECUTE FUNCTION ledger_immutable();
CREATE TRIGGER ledger_journals_no_truncate BEFORE TRUNCATE ON ledger_journals FOR EACH STATEMENT EXECUTE FUNCTION ledger_immutable();

-- A journal cannot be committed unless it has at least two entries and debits = credits for every currency.
-- Deferred to COMMIT so all entries of a journal can be inserted first.
CREATE FUNCTION ledger_check_balanced() RETURNS trigger AS $$
DECLARE
    v_journal uuid;
    v_count   int;
    r         record;
BEGIN
    -- the same function serves both tables; to_jsonb avoids referencing a column the other table does not have
    v_journal := CASE WHEN TG_TABLE_NAME = 'ledger_journals' THEN (to_jsonb(NEW) ->> 'id')::uuid ELSE (to_jsonb(NEW) ->> 'journal_id')::uuid END;
    SELECT count(*) INTO v_count FROM ledger_entries WHERE journal_id = v_journal;
    IF v_count < 2 THEN
        RAISE EXCEPTION 'journal % must have at least 2 entries (has %)', v_journal, v_count USING ERRCODE = 'check_violation';
    END IF;
    FOR r IN SELECT currency, sum(debit_minor) AS d, sum(credit_minor) AS c FROM ledger_entries WHERE journal_id = v_journal GROUP BY currency LOOP
        IF r.d <> r.c THEN
            RAISE EXCEPTION 'journal % is unbalanced in %: debits % credits %', v_journal, r.currency, r.d, r.c USING ERRCODE = 'check_violation';
        END IF;
    END LOOP;
    RETURN NULL;
END $$ LANGUAGE plpgsql;
CREATE CONSTRAINT TRIGGER ledger_entries_balanced AFTER INSERT ON ledger_entries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_check_balanced();
CREATE CONSTRAINT TRIGGER ledger_journals_have_entries AFTER INSERT ON ledger_journals DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION ledger_check_balanced();

-- ============================== outbox, consumers, audit ==============================
CREATE TABLE outbox_events (
    id           uuid PRIMARY KEY,
    aggregate_id uuid        NOT NULL,
    topic        varchar(60) NOT NULL,
    event_type   varchar(60) NOT NULL,
    payload      jsonb       NOT NULL,
    status       varchar(10) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PUBLISHED')),
    attempts     int         NOT NULL DEFAULT 0,
    last_error   varchar(300),
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_pending_idx ON outbox_events (created_at, id) WHERE status = 'PENDING';

-- Idempotent consumers: the marker is inserted in the same transaction as the consumer's effects.
CREATE TABLE processed_events (
    consumer     varchar(60) NOT NULL,
    event_id     uuid        NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

CREATE TABLE audit_log (
    id          uuid PRIMARY KEY,
    merchant_id uuid,
    actor       varchar(40) NOT NULL,
    action      varchar(60) NOT NULL,
    entity_type varchar(30) NOT NULL,
    entity_id   uuid,
    request_id  varchar(64),
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX audit_entity_idx ON audit_log (entity_type, entity_id);

-- ============================== webhooks ==============================
CREATE TABLE webhook_endpoints (
    id          uuid PRIMARY KEY,
    merchant_id uuid         NOT NULL REFERENCES merchants (id),
    url         varchar(500) NOT NULL,
    secret      varchar(80)  NOT NULL,
    event_types text[]       NOT NULL,
    active      boolean      NOT NULL DEFAULT true,
    created_at  timestamptz  NOT NULL DEFAULT now()
);

CREATE TABLE webhook_deliveries (
    id              uuid PRIMARY KEY,
    event_id        uuid         NOT NULL,
    endpoint_id     uuid         NOT NULL REFERENCES webhook_endpoints (id),
    merchant_id     uuid         NOT NULL,
    event_type      varchar(60)  NOT NULL,
    payload         jsonb        NOT NULL,
    status          varchar(10)  NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'DELIVERED', 'DEAD')),
    attempt_count   int          NOT NULL DEFAULT 0,
    next_attempt_at timestamptz  NOT NULL DEFAULT now(),
    last_error      varchar(300),
    created_at      timestamptz  NOT NULL DEFAULT now(),
    delivered_at    timestamptz,
    UNIQUE (event_id, endpoint_id)
);
CREATE INDEX webhook_due_idx ON webhook_deliveries (next_attempt_at) WHERE status = 'PENDING';

CREATE TABLE webhook_attempts (
    id              uuid PRIMARY KEY,
    delivery_id     uuid        NOT NULL REFERENCES webhook_deliveries (id),
    attempt_number  int         NOT NULL,
    response_status int,
    error           varchar(300),
    duration_ms     bigint,
    created_at      timestamptz NOT NULL DEFAULT now()
);

-- Local test receiver (development only): records every delivery it gets and whether the HMAC verified.
CREATE TABLE test_webhook_receipts (
    id              uuid PRIMARY KEY,
    endpoint_id     uuid        NOT NULL,
    event_id        uuid,
    signature_valid boolean     NOT NULL,
    responded_status int        NOT NULL,
    received_at     timestamptz NOT NULL DEFAULT now()
);

-- ============================== reconciliation ==============================
CREATE TABLE reconciliation_runs (
    id                uuid PRIMARY KEY,
    started_at        timestamptz NOT NULL DEFAULT now(),
    completed_at      timestamptz,
    status            varchar(12) NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    payments_checked  int         NOT NULL DEFAULT 0,
    discrepancy_count int         NOT NULL DEFAULT 0
);

CREATE TABLE reconciliation_discrepancies (
    id           uuid PRIMARY KEY,
    run_id       uuid        NOT NULL REFERENCES reconciliation_runs (id),
    type         varchar(40) NOT NULL,
    payment_id   uuid,
    reference_id uuid,
    details      varchar(500),
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX recon_disc_run_idx ON reconciliation_discrepancies (run_id);

-- ============================== simulated provider (an "external" system's own durable record) ==============================
CREATE TABLE provider_operations (
    operation_id varchar(100) PRIMARY KEY,
    op_type      varchar(12) NOT NULL,
    payment_id   uuid        NOT NULL,
    amount_minor bigint      NOT NULL,
    outcome      varchar(12) NOT NULL CHECK (outcome IN ('SUCCEEDED', 'FAILED')),
    error_code   varchar(50),
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE provider_call_log (
    operation_id varchar(100) PRIMARY KEY,
    calls        int NOT NULL DEFAULT 0
);
