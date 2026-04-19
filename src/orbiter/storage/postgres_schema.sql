CREATE TABLE IF NOT EXISTS dags (
    fingerprint TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    definition  JSONB NOT NULL,
    created_at  DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS dag_runs (
    id               TEXT PRIMARY KEY,
    dag_fingerprint  TEXT NOT NULL REFERENCES dags(fingerprint),
    state            TEXT NOT NULL,
    started_at       DOUBLE PRECISION,
    finished_at      DOUBLE PRECISION,
    trigger          TEXT,
    params_json      JSONB
);

CREATE INDEX IF NOT EXISTS idx_dag_runs_state ON dag_runs(state);

CREATE TABLE IF NOT EXISTS task_runs (
    id               TEXT PRIMARY KEY,
    dag_run_id       TEXT NOT NULL REFERENCES dag_runs(id),
    task_id          TEXT NOT NULL,
    attempt          INTEGER NOT NULL,
    state            TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    scheduled_at     DOUBLE PRECISION,
    started_at       DOUBLE PRECISION,
    finished_at      DOUBLE PRECISION,
    error            TEXT,
    output_json      JSONB,
    UNIQUE (dag_run_id, task_id, attempt),
    UNIQUE (idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_task_runs_state ON task_runs(state);
CREATE INDEX IF NOT EXISTS idx_task_runs_dag_run ON task_runs(dag_run_id);

CREATE TABLE IF NOT EXISTS checkpoints (
    dag_run_id  TEXT NOT NULL REFERENCES dag_runs(id),
    task_id     TEXT NOT NULL,
    key         TEXT NOT NULL,
    value_json  JSONB NOT NULL,
    written_at  DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (dag_run_id, task_id, key)
);

CREATE TABLE IF NOT EXISTS queue_items (
    id            BIGSERIAL PRIMARY KEY,
    task_run_id   TEXT NOT NULL REFERENCES task_runs(id),
    enqueued_at   DOUBLE PRECISION NOT NULL,
    available_at  DOUBLE PRECISION NOT NULL,
    lease_until   DOUBLE PRECISION,
    leased_by     TEXT
);

CREATE INDEX IF NOT EXISTS idx_queue_available ON queue_items(available_at);

CREATE TABLE IF NOT EXISTS schedules (
    id               TEXT PRIMARY KEY,
    dag_fingerprint  TEXT NOT NULL REFERENCES dags(fingerprint),
    name             TEXT NOT NULL UNIQUE,
    state            TEXT NOT NULL,
    overlap_policy   TEXT NOT NULL DEFAULT 'allow',
    interval_seconds INTEGER NOT NULL,
    next_run_at      DOUBLE PRECISION NOT NULL,
    last_run_at      DOUBLE PRECISION,
    last_run_id      TEXT REFERENCES dag_runs(id),
    params_json      JSONB,
    created_at       DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_schedules_state_next_run ON schedules(state, next_run_at);
