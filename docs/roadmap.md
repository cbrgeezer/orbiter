# Roadmap and limitations

## Known limitations

- **No cross-process queue backend.** The in-memory queue is an
  `asyncio.Event` primitive; it cannot be shared. The SQLite queue works
  across processes on one host but degrades to polling.
- **No Postgres backend yet.** `SQLiteStateStore` is the only implementation.
  The `StateStore` protocol is small enough that a Postgres version is a
  clear win, it just is not written.
- **No backfill / catchup.** Scheduled DAGs only run forward. Running "last
  Tuesday's run" is not supported.
- **No DAG versioning UX.** The fingerprint changes on any definition edit;
  there is no concept of a compatible migration.
- **Web UI is a placeholder.** Only `/healthz`, `/dag`, `/runs` exist.
- **No authentication on the API.** Intended behind a reverse proxy.
- **No distributed tracing.** Structured logs only.

## Near-term

- [ ] Postgres state store (keeps the same protocol).
- [ ] Redis queue backend with pub/sub wakeups (removes polling tax
      across processes).
- [ ] Cron-like scheduler for recurring DAG runs.
- [ ] CLI `validate` warnings for tasks without a timeout.
- [ ] `task.resources` hint for the worker pool so CPU-bound tasks can pin
      to a subset of workers.

## Mid-term

- [ ] Sensors / event-driven triggers (wait for a file, a DB row, a Kafka
      offset).
- [ ] Pluggable secrets provider for task params.
- [ ] OpenTelemetry spans around every state transition.
- [ ] Structured per-task logs streamed into the state store.

## Out of scope, by choice

- Visual DAG designer. There are better tools for this.
- Multi-tenant RBAC. If you need it, put this behind a reverse proxy with
  an auth layer; Orbiter is a library, not a platform.
- Arbitrary distributed transactions. See `docs/execution-semantics.md`.

## Profiling notes

Informal profiling with `py-spy` on the benchmark script:
- Dispatch loop CPU is dominated by `json.dumps` for task_run output
  serialisation (~30%).
- Queue `get()` sleeps on the event; effectively zero CPU at idle.
- SQLite writes on the hot path take ~40 µs each on M-series hardware,
  which caps us to ~25k transitions/sec on a single node before contention.

Memory usage scales linearly with the number of in-flight task_runs (one
row per attempt plus a small QueueMessage). At 100k outstanding tasks the
working set is ~40 MB.
