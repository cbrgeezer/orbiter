# Data model

The state store owns five tables. All of them live in a single SQLite file
(or in a Postgres schema in multi-node mode). This document describes the
contract; the exact DDL is in `src/orbiter/storage/schema.sql`.

## `dags`

A DAG is identified by its content-fingerprint (SHA256 of its serialised
definition). Two DAGs with identical tasks and edges share a row. When a
user redeploys a changed DAG, the fingerprint moves and a new row appears.

| column       | type  | note                               |
| ------------ | ----- | ---------------------------------- |
| fingerprint  | TEXT  | primary key, content hash          |
| name         | TEXT  | human-readable                     |
| definition   | JSON  | full serialised DAG                |
| created_at   | REAL  | first seen                         |

## `dag_runs`

One row per invocation of a DAG. The `state` column moves through:
`pending -> running -> {succeeded, failed, cancelled}`.

| column          | type | note                                   |
| --------------- | ---- | -------------------------------------- |
| id              | TEXT | primary key, uuid4                     |
| dag_fingerprint | TEXT | FK → dags                              |
| state           | TEXT | enum                                   |
| started_at      | REAL | set when scheduler enters RUNNING      |
| finished_at     | REAL | set at terminal transition             |
| trigger         | TEXT | `manual`, `api`, `schedule`, ...       |
| params_json     | JSON | user-supplied params                   |

## `task_runs`

One row per **attempt** of a task inside a DAG run. The three key invariants:

1. `UNIQUE (dag_run_id, task_id, attempt)` — no two concurrent creators can
   race on the same attempt number.
2. `UNIQUE (idempotency_key)` — the deduplication index. The scheduler
   attempts to insert; a failure is treated as "this attempt already exists"
   and execution is skipped.
3. State transitions are monotonic in a single direction:
   `pending -> queued -> running -> {succeeded | failed | retrying | dead_letter}`.

| column          | type | note                                        |
| --------------- | ---- | ------------------------------------------- |
| id              | TEXT | primary key, uuid4                          |
| dag_run_id      | TEXT | FK → dag_runs                               |
| task_id         | TEXT | matches DAG task id                         |
| attempt         | INT  | 1-indexed                                   |
| state           | TEXT | enum                                        |
| idempotency_key | TEXT | UNIQUE — see execution-semantics            |
| scheduled_at    | REAL | insertion time                              |
| started_at      | REAL | running transition                          |
| finished_at     | REAL | terminal transition                         |
| error           | TEXT | traceback, if any                           |
| output_json     | JSON | user-returned value                         |

## `checkpoints`

User-visible key/value store written *during* a task run. Intended for
resumable long-running tasks: write progress here, and on retry the user
reads the last checkpoint back and skips completed chunks.

| column      | type | note                                  |
| ----------- | ---- | ------------------------------------- |
| dag_run_id  | TEXT | FK → dag_runs                         |
| task_id     | TEXT |                                       |
| key         | TEXT |                                       |
| value_json  | JSON |                                       |
| written_at  | REAL |                                       |

Primary key is `(dag_run_id, task_id, key)`. Writes are upserts.

## `queue_items`

The work queue, colocated with the state store for single-node deployments.
A queue row carries a `lease_until` timestamp; if a worker does not ack
before that time, the item is reclaimable by another worker. This is the
visibility-timeout pattern used by SQS and friends.

| column       | type | note                                   |
| ------------ | ---- | -------------------------------------- |
| id           | INT  | primary key                            |
| task_run_id  | TEXT | FK → task_runs                         |
| enqueued_at  | REAL |                                        |
| available_at | REAL | deferred delivery (retries, schedules) |
| lease_until  | REAL | NULL if unleased                       |
| leased_by    | TEXT | worker id                              |
