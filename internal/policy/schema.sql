-- ratelimit-platform schema (PostgreSQL >= 13).
-- Policy state lives here; Redis holds only live bucket counters and bounded
-- idempotency/dedup windows.

CREATE TABLE IF NOT EXISTS rl_policies (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    -- Integer millitokens: 1 token = 1000 units. capacity = burst allowance.
    capacity_mt BIGINT NOT NULL CHECK (capacity_mt BETWEEN 1 AND 1000000000),
    -- Refill in millitokens per second (independent of capacity).
    refill_mtps BIGINT NOT NULL CHECK (refill_mtps BETWEEN 0 AND 1000000000),
    description TEXT NOT NULL DEFAULT '',
    version     BIGINT NOT NULL DEFAULT 1,
    disabled    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rl_endpoints (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL DEFAULT '',
    risk        TEXT NOT NULL DEFAULT 'normal',
    -- Redis failure posture per interface risk configuration.
    fail_policy TEXT NOT NULL DEFAULT 'closed' CHECK (fail_policy IN ('open', 'closed')),
    disabled    BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rl_bindings (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    layer     TEXT NOT NULL CHECK (layer IN ('tenant', 'user', 'api')),
    tenant_id TEXT,
    user_id   TEXT,
    api_id    TEXT,
    policy_id BIGINT NOT NULL REFERENCES rl_policies(id) ON DELETE CASCADE,
    -- Subject columns: exactly the column matching the layer is meaningful
    -- (it MAY be NULL to encode that layer's default policy); the other two
    -- must always be NULL.
    CONSTRAINT rl_bindings_one_subject CHECK (
        (layer = 'tenant' AND user_id IS NULL AND api_id IS NULL)
     OR (layer = 'user'   AND tenant_id IS NULL AND api_id IS NULL)
     OR (layer = 'api'    AND tenant_id IS NULL AND user_id IS NULL)
    ),
    -- Uniqueness per (layer, subject). In PostgreSQL NULLs are distinct in
    -- unique constraints, so the single default row per layer is enforced by
    -- the partial indexes below.
    CONSTRAINT rl_bindings_subject_key UNIQUE (layer, tenant_id, user_id, api_id)
);

-- Only one default (all-subject-NULL) row per layer.
CREATE UNIQUE INDEX IF NOT EXISTS rl_bindings_default_tenant
    ON rl_bindings (layer) WHERE layer = 'tenant' AND tenant_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS rl_bindings_default_user
    ON rl_bindings (layer) WHERE layer = 'user' AND user_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS rl_bindings_default_api
    ON rl_bindings (layer) WHERE layer = 'api' AND api_id IS NULL;

-- Bump policy version on every mutable update so running Redis buckets
-- re-clamp to new parameters on their next decision.
CREATE OR REPLACE FUNCTION rl_policy_touch() RETURNS trigger AS $$
BEGIN
    NEW.version := OLD.version + 1;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS rl_policy_touch ON rl_policies;
CREATE TRIGGER rl_policy_touch
    BEFORE UPDATE ON rl_policies
    FOR EACH ROW EXECUTE FUNCTION rl_policy_touch();

-- Emit one invalidation event after any policy config commit. The cache
-- layer LISTENs and reloads; a polling refresh remains as fallback.
CREATE OR REPLACE FUNCTION rl_table_notify() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('rl_policy_changed', '');
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS rl_policies_notify ON rl_policies;
CREATE TRIGGER rl_policies_notify
    AFTER INSERT OR UPDATE OR DELETE ON rl_policies
    FOR EACH STATEMENT EXECUTE FUNCTION rl_table_notify();
DROP TRIGGER IF EXISTS rl_endpoints_notify ON rl_endpoints;
CREATE TRIGGER rl_endpoints_notify
    AFTER INSERT OR UPDATE OR DELETE ON rl_endpoints
    FOR EACH STATEMENT EXECUTE FUNCTION rl_table_notify();
DROP TRIGGER IF EXISTS rl_bindings_notify ON rl_bindings;
CREATE TRIGGER rl_bindings_notify
    AFTER INSERT OR UPDATE OR DELETE ON rl_bindings
    FOR EACH STATEMENT EXECUTE FUNCTION rl_table_notify();
