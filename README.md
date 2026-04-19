**Orbiter**

Overview

Orbiter is a Python workflow engine for defining and running directed acyclic graphs. It focuses on legible execution semantics, small enough code to study in an afternoon, and honest guarantees around retries, checkpointing, idempotency, and scheduling.

Why this exists

The workflow space is crowded, but many tools hide their real guarantees behind marketing language. Orbiter exists to make those choices explicit and readable.

Feature summary

1. Declarative DAG definition in Python or YAML.
2. Async scheduler with pluggable queue backends.
3. Worker pool built on `asyncio` with configurable concurrency.
4. Retry policies with backoff, jitter, and maximum attempt limits.
5. Idempotency support through deterministic run keys and deduplication.
6. Task context with run metadata, input params, checkpoints, and per task logging.
7. Checkpointing with SQL backed task state.
8. Recurring schedules with pause, resume, run-now, and overlap policies.
9. Run inspection, cancellation, and lightweight metrics.
10. SQLite and PostgreSQL state stores behind a shared runtime interface.
11. In memory and durable store backed queue modes behind the same queue interface.
12. REST API built with FastAPI.
13. CLI for local execution, DAG validation, and queue inspection.
14. Plugin support for operators and queue backends.
15. Fault injection tooling for recovery testing.

Architecture

The system is organised around a DAG parser, an async scheduler, a queue layer, a state store, and a worker pool. See [`docs/architecture.md`](docs/architecture.md) for the full design.

Execution semantics

1. Default delivery is at least once. A worker crash after execution but before persistence can trigger a retry.
2. Apparent exactly once behaviour depends on user supplied idempotency keys and idempotent side effects.
3. The checkpoint boundary is the persisted state transition. Recovery logic handles crashes between output writing, state persistence, and acknowledgement.

Current runtime capabilities

1. Tasks can receive a `TaskContext` object for run metadata, params, checkpoints, and structured logging.
2. CLI validation warns when tasks are missing timeouts.
3. The API exposes run listing, run cancellation, recurring schedules, and a simple Prometheus style metrics endpoint.
4. The same runtime can use SQLite for local work or PostgreSQL for a more production shaped deployment.
5. Queue mode can be chosen between low latency in memory execution and a durable store backed queue.

Quick example

```python
from orbiter import DAG, RetryPolicy, TaskContext

dag = DAG("example")

@dag.task(retry=RetryPolicy(max_attempts=3), timeout_seconds=30)
async def fetch(context: TaskContext) -> dict:
    rows = context.param("rows", 100)
    context.checkpoint("source", {"rows": rows})
    return {"rows": rows}

@dag.task(depends_on=["fetch"], timeout_seconds=30)
async def load(context: TaskContext) -> str:
    source = context.get_checkpoint("source", task_id="fetch", default={"rows": 0})
    return f"loaded {source['rows']} rows"
```

Run it locally:

```bash
orbiter run examples/example_dag.py --params '{"rows": 64}'
```

Operator console

When the API is running, open:

```text
http://localhost:8000/ui/
```

The console provides:

1. runtime metrics summary
2. run inspection and cancellation
3. schedule creation, pause, resume, and run-now controls
4. a lightweight operator view without a separate front end build step

API key auth

Set `ORBITER_API_KEY` to protect the API and operator console.

```bash
export ORBITER_API_KEY='replace-this-with-a-strong-key'
orbiter serve-api examples/example_dag.py --db orbiter.db
```

Public routes remain limited to:

1. `/`
2. `/healthz`
3. `/auth/config`
4. `/ui/*`

Operational API routes require the key through:

1. `X-Orbiter-Key`
2. `Authorization: Bearer <key>`

Recurring schedules

```bash
orbiter create-schedule examples/example_dag.py \
  --name hourly-import \
  --every-seconds 3600 \
  --overlap-policy forbid \
  --db orbiter.db \
  --params '{"rows": 500}'

orbiter list-schedules orbiter.db
orbiter pause-schedule orbiter.db <schedule_id>
orbiter resume-schedule orbiter.db <schedule_id>
orbiter run-schedule orbiter.db <schedule_id> examples/example_dag.py
```

See [`docs/scheduling.md`](docs/scheduling.md) for the schedule model and semantics.

Storage backends

SQLite remains the default for local development:

```bash
orbiter run examples/example_dag.py --db orbiter.db --queue-backend auto
```

PostgreSQL is selected automatically from the connection string:

```bash
orbiter run examples/example_dag.py \
  --db 'postgresql://orbiter:orbiter@localhost:5432/orbiter' \
  --queue-backend store \
  --params '{"rows": 64}'
```

See [`docs/postgres.md`](docs/postgres.md) for the PostgreSQL backend notes.
See [`docs/queue-backends.md`](docs/queue-backends.md) for queue mode selection.

Docker Compose quick start

```bash
docker compose up --build
```

Then open:

```text
http://localhost:8000/ui/
```

The compose bundle enables API key auth by default with:

```text
orbiter-demo-key
```

Override it in your shell before startup:

```bash
export ORBITER_API_KEY='replace-this-in-real-use'
docker compose up --build
```

This starts:

1. PostgreSQL 16
2. an API service for control and inspection
3. a scheduler service for dispatch and recurring schedules
4. a worker service for task execution
5. the built in operator console

Split runtime roles

Run the services separately when you want a more production shaped setup:

```bash
orbiter serve-api examples/example_dag.py \
  --db 'postgresql://orbiter:orbiter@localhost:5432/orbiter' \
  --queue-backend store

orbiter run-scheduler examples/example_dag.py \
  --db 'postgresql://orbiter:orbiter@localhost:5432/orbiter' \
  --queue-backend store

orbiter run-worker examples/example_dag.py \
  --db 'postgresql://orbiter:orbiter@localhost:5432/orbiter' \
  --queue-backend store \
  --concurrency 4
```

Positioning

Use Orbiter if you want to understand how a workflow engine is built and what guarantees it really provides. Use a larger platform if you want a broader production feature set out of the box.
