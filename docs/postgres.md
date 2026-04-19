# PostgreSQL backend

Orbiter now supports both SQLite and PostgreSQL state stores.

## When to use PostgreSQL

Use PostgreSQL when you want:

1. a real server backed state store rather than a local file
2. stronger concurrent coordination semantics
3. cleaner separation between API, scheduler, and workers
4. a more production shaped deployment story

SQLite remains a good default for local development and small single node
setups. PostgreSQL is the better choice once you want a more durable service
boundary.

## Connection string

Any `--db` or `db_path` value starting with `postgres://` or
`postgresql://` uses `PostgresStateStore`.

Example:

```bash
orbiter serve examples/example_dag.py \
  --db 'postgresql://orbiter:orbiter@localhost:5432/orbiter'
```

The same applies to local runs:

```bash
orbiter run examples/example_dag.py \
  --db 'postgresql://orbiter:orbiter@localhost:5432/orbiter' \
  --params '{"rows": 64}'
```

## Implementation notes

The PostgreSQL backend keeps the same state model as SQLite:

1. `dags`
2. `dag_runs`
3. `task_runs`
4. `checkpoints`
5. `queue_items`
6. `schedules`

The important upgrade is in coordination behaviour:

1. queue leasing uses `FOR UPDATE SKIP LOCKED`
2. due schedule dispatch also uses `FOR UPDATE SKIP LOCKED`
3. JSON payloads are stored as `JSONB`
4. the durable store backed queue works directly against the PostgreSQL
   `queue_items` table

That means PostgreSQL is not just a persistence swap. It is the first step
towards a cleaner multi process runtime.
