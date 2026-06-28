BEGIN;

-- Phase state: current + completed phases per project
CREATE TABLE IF NOT EXISTS phase_state (
    project_id   TEXT PRIMARY KEY,
    current_phase INT DEFAULT 0,
    completed_phases INT[] DEFAULT '{}',
    state_json   JSONB DEFAULT '{}',
    title        TEXT DEFAULT '',
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Workflow events: audit log for all pipeline events
CREATE TABLE IF NOT EXISTS workflow_events (
    event_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id  TEXT NOT NULL,
    event_type  TEXT NOT NULL,  -- phase_start, phase_complete, gate_pass, gate_fail, error
    payload     JSONB DEFAULT '{}',
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_we_project ON workflow_events(project_id);
CREATE INDEX IF NOT EXISTS idx_we_type ON workflow_events(event_type);
CREATE INDEX IF NOT EXISTS idx_we_created ON workflow_events(created_at DESC);

-- Cost ledger: per-project token usage records
CREATE TABLE IF NOT EXISTS cost_ledger (
    ledger_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id  TEXT NOT NULL,
    phase       INT NOT NULL,
    tokens_used BIGINT DEFAULT 0,
    cost_usd    DOUBLE PRECISION DEFAULT 0.0,
    model       TEXT DEFAULT '',
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_cl_project ON cost_ledger(project_id);

COMMIT;
