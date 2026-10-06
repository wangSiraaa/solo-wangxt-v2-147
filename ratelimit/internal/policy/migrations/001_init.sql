CREATE TABLE IF NOT EXISTS policies (
    id             BIGSERIAL PRIMARY KEY,
    scope          TEXT NOT NULL CHECK (scope IN ('tenant', 'user', 'endpoint')),
    scope_key      TEXT NOT NULL,
    capacity       DOUBLE PRECISION NOT NULL CHECK (capacity > 0),
    refill_per_sec DOUBLE PRECISION NOT NULL CHECK (refill_per_sec >= 0),
    fail_open      BOOLEAN NOT NULL DEFAULT FALSE,
    enabled        BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (scope, scope_key)
);
