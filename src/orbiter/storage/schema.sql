-- Orbiter state store schema. Kept small on purpose.
-- The four tables in the README data-model section map 1:1 to these.

CREATE TABLE IF NOT EXISTS dags (
    fingerprint TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    definition  TEXT NOT NULL,
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS dag_runs (
    id               TEXT PRIMARY KEY,
    dag_fingerprint  TEXT NOT NULL REFERENCES dags(fingerprint),
    state            TEXT NOT NULL,
    started_at       REAL,
    finished_at      REAL,
    trigger          TEXT,
    params_json      TEXT
);

CREATE INDEX IF NOT EXISTS idx_dag_runs_state ON dag_runs(state);

CREATE TABLE IF NOT EXISTS task_runs (
    id               TEXT PRIMARY KEY,
    dag_run_id       TEXT NOT NULL REFERENCES dag_runs(id),
    task_id          TEXT NOT NULL,
    attempt          INTEGER NOT NULL,
    state            TEXT NOT NULL,
    idempotency_key  TEXT NOT NULL,
    scheduled_at     REAL,
    started_at       REAL,
    finished_at      REAL,
    error            TEXT,
    output_json      TEXT,
    UNIQUE (dag_run_id, task_id, attempt),
    UNIQUE (idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_task_runs_state     ON task_runs(state);
CREATE INDEX IF NOT EXISTS idx_task_runs_dag_run   ON task_runs(dag_run_id);

CREATE TABLE IF NOT EXISTS checkpoints (
    dag_run_id  TEXT NOT NULL REFERENCES dag_runs(id),
    task_id     TEXT NOT NULL,
    key         TEXT NOT NULL,
    value_json  TEXT NOT NULL,
    written_at  REAL NOT NULL,
    PRIMARY KEY (dag_run_id, task_id, key)
);

-- Work queue. We use a table here instead of a separate broker for the
-- single-node case. The `lease_until` column gives us a visibility timeout.
CREATE TABLE IF NOT EXISTS queue_items (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    task_run_id   TEXT NOT NULL REFERENCES task_runs(id),
    enqueued_at   REAL NOT NULL,
    available_at  REAL NOT NULL,
    lease_until   REAL,
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
    next_run_at      REAL NOT NULL,
    last_run_at      REAL,
    last_run_id      TEXT REFERENCES dag_runs(id),
    params_json      TEXT,
    created_at       REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_schedules_state_next_run ON schedules(state, next_run_at);

CREATE TABLE IF NOT EXISTS activity_events (
    id           TEXT PRIMARY KEY,
    event_type   TEXT NOT NULL,
    summary      TEXT NOT NULL,
    actor        TEXT NOT NULL,
    dag_run_id   TEXT REFERENCES dag_runs(id),
    schedule_id  TEXT REFERENCES schedules(id),
    metadata_json TEXT,
    created_at   REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_activity_events_created_at
    ON activity_events(created_at DESC);
