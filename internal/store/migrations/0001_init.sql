-- Customers and their current balance (smallest currency unit).
-- The CHECK constraint is a last line of defence: even a buggy query can
-- never leave a balance below zero.
CREATE TABLE customers (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       TEXT        NOT NULL,
    balance    BIGINT      NOT NULL DEFAULT 0 CHECK (balance >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only ledger. Every balance change writes one row, so the balance
-- can always be explained (and checked: balance = SUM(amount)).
CREATE TABLE balance_transactions (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id   BIGINT      NOT NULL REFERENCES customers (id),
    kind          TEXT        NOT NULL CHECK (kind IN ('topup', 'charge', 'refund')),
    amount        BIGINT      NOT NULL, -- positive = credit, negative = debit
    balance_after BIGINT      NOT NULL,
    message_count INT         NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX balance_transactions_customer_idx ON balance_transactions (customer_id, id DESC);

-- Every accepted SMS. This table is the source of truth for reports and
-- also acts as the transactional outbox for the dispatcher (status = 'pending').
CREATE TABLE messages (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id     BIGINT      NOT NULL REFERENCES customers (id),
    idempotency_key TEXT,
    phone           TEXT        NOT NULL,
    body            TEXT        NOT NULL,
    express         BOOLEAN     NOT NULL DEFAULT false,
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'queued', 'sending', 'sent', 'failed')),
    cost            BIGINT      NOT NULL CHECK (cost >= 0),
    attempts        INT         NOT NULL DEFAULT 0,
    operator_ref    TEXT,
    last_error      TEXT,
    deadline_at     TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at         TIMESTAMPTZ
);

-- Reports: one customer's messages, newest first (keyset pagination on id).
CREATE INDEX messages_customer_idx ON messages (customer_id, id DESC);

-- Client retries with the same Idempotency-Key must not create (or charge) twice.
CREATE UNIQUE INDEX messages_idempotency_idx ON messages (customer_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Relay: small partial index that only contains rows waiting to be dispatched.
CREATE INDEX messages_pending_idx ON messages (express, next_attempt_at, id)
    WHERE status = 'pending';

-- Reconciler: rows that are in flight between Redis and the operator.
CREATE INDEX messages_inflight_idx ON messages (status, updated_at)
    WHERE status IN ('queued', 'sending');

-- Small key/value table for coordination between worker instances.
CREATE TABLE system_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
