# Orbiter

> Distributed workflow / data processing engine — DAGs with retries, backoff,
> idempotency, checkpointing, and scheduling. Somewhere between a lightweight
> Airflow, Prefect, and Celery, with an opinionated architecture.

Orbiter is a Python engine for defining and running workflows as directed
acyclic graphs (DAGs). It is intentionally small enough to read in an
afternoon and honest about what it does and does not guarantee.

---

## Why another one?

Airflow, Prefect, Dagster, Celery, Luigi — the space is crowded. The point of
Orbiter is not to compete with them but to make the *execution semantics*
legible. Most workflow engines hide their at-least-once vs exactly-once
assumptions behind marketing language. This project writes them down.

If you are looking for a production scheduler, run Prefect. If you want to
understand *how* a scheduler is built and what choices it makes, read this.

---

## Feature summary

- Declarative DAG definition (Python decorators or YAML).
- Async scheduler with pluggable queue backends.
- Worker pool based on `asyncio` with configurable concurrency.
- Retry policies: exponential backoff with jitter, max attempts, dead-letter.
- Task idempotency via deterministic run keys and deduplication.
- Checkpointing: task state persisted in SQL after every transition.
- REST API (FastAPI) for submission and inspection.
- CLI for local runs, DAG validation, and queue inspection.
- Plugin architecture for custom operators and queue backends.
- Fault injection harness for testing recovery paths.

---

## Architecture diagram

```
                   +-----------------+
                   |     CLI / API   |
                   +--------+--------+
                            |
                            v
  +---------+       +---------------+        +-----------------+
  |  DAG    | ----> |  Scheduler    | -----> |    Queue        |
  | parser  |       |  (async loop) |        | (memory / SQL)  |
  +---------+       +-------+-------+        +--------+--------+
                            |                         |
                            v                         v
                   +-----------------+        +-----------------+
                   |   State Store   | <----> |   Worker Pool   |
                   |   (SQLite/PG)   |        |   (asyncio)     |
                   +--------+--------+        +--------+--------+
                            ^                         |
                            |                         v
                            +---- checkpoints --------+
```

See [`docs/architecture.md`](docs/architecture.md) for the long form.

---

## Execution semantics

This is the part most engines paper over. Orbiter is explicit:

- **Default delivery:** at-least-once. A worker crash after task execution but
  before the state transition is persisted results in a re-run.
- **Exactly-once illusion:** available for user code that declares an
  idempotency key. The scheduler deduplicates re-deliveries against the
  `task_runs` table before dispatch, but the user-code side-effect is only
  as idempotent as the user makes it. We do not lie about this.
- **Checkpointing boundary:** the state transition write is the transactional
  boundary. Task output is written first, then the state row, then the ack.
  A crash between steps is recovered by the scheduler on restart.

Full discussion: [`docs/execution-semantics.md`](docs/execution-semantics.md).

---

## Data model

Four core tables:

| table         | purpose                                                |
| ------------- | ------------------------------------------------------ |
| `dags`        | DAG definitions, versioned by content hash             |
| `dag_runs`    | A single invocation of a DAG                           |
| `task_runs`   | Per-task attempt rows, keyed by (dag_run, task, try)   |
| `checkpoints` | User-visible key/value state written during a run      |

Full schema: [`docs/data-model.md`](docs/data-model.md).

---

## Failure scenarios

Covered in tests and documented with recovery behavior in
[`docs/failure-scenarios.md`](docs/failure-scenarios.md):

- worker crash mid-task,
- scheduler restart with in-flight tasks,
- queue redelivery under broker failure,
- DB transient errors,
- infinite-retry poison messages,
- DAG upgrade while a run is in flight.

---

## Benchmarks

See `benchmarks/` for a load script that enqueues N no-op tasks across M
workers and measures throughput and end-to-end latency. Results on the
reference machine (see `docs/architecture.md`):

- ~6k tasks/sec sustained on a single node with 64 workers and SQLite WAL.
- Switching to Postgres halves throughput but unlocks multi-node scheduling.
- Event-driven dispatch has ~8x lower tail latency than 1s polling.

---

## Roadmap and limitations

- No true exactly-once delivery. See `docs/execution-semantics.md`.
- SQLite backend is single-node. Multi-node requires Postgres.
- Web UI is intentionally minimal — this is a backend project.
- No DAG backfill or time-range catchup in the first cut.

Full list in [`docs/roadmap.md`](docs/roadmap.md).

---

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Validate an example DAG
orbiter validate examples/example_dag.py

# Run it locally
orbiter run examples/example_dag.py

# Start the API
orbiter serve --host 0.0.0.0 --port 8000
```

---

## License

MIT.
