# Architecture

## One-screen summary

Orbiter splits the problem into five roles. Each role is a module; each
module does one thing.

| Role         | Responsibility                                                 |
| ------------ | -------------------------------------------------------------- |
| DAG parser   | Turn Python definitions (decorators or YAML) into a DAG object |
| Scheduler    | Decide which tasks are *ready*; enqueue them                   |
| Queue        | Buffer ready tasks with delayed-delivery support               |
| Worker pool  | Pull from queue, execute user code, write state transitions    |
| State store  | Durable log of every task transition, plus the work queue      |
| Task context | Expose params, checkpoints, run metadata, and logger bindings to task code |

The reason for the split is blast radius. A bug in the scheduler should not
corrupt execution; a crashed worker should not stall scheduling. In the
reference implementation every component is a plain Python class that can be
composed or replaced.

## Data flow

```
user DAG -> scheduler.submit() -> store.create_dag_run()
                                -> for each ready task:
                                      store.create_task_run()  (IDEMPOTENT)
                                      queue.put(msg)
worker loop:
     msg = queue.get()
     store.set_task_state(RUNNING)
     context = TaskContext(...)
     output = await fn(context / params)
     store.set_task_state(SUCCEEDED)
     queue.ack(msg)
```

Every step writes through the store first. The queue is never authoritative.
The store's `task_runs.idempotency_key` UNIQUE index is the single source of
truth for "has this attempt already been created".

## Task invocation model

Tasks can be written in three practical styles:

1. No arguments.
2. A `context` argument typed as `TaskContext`.
3. Named arguments resolved from submitted run params.

This gives users a straightforward path from simple local tasks to more
serious workflows that need checkpoints, run metadata, and structured logs
without introducing a separate SDK object graph.

## Schedule loop

Recurring schedules are handled by a separate background service:

1. poll for active schedules whose `next_run_at` is due
2. create a pending DAG run with a schedule trigger
3. advance the schedule's `next_run_at`
4. hand the new run to the normal scheduler and worker flow

This keeps scheduling distinct from task dispatch. A bug in recurring run
creation should not corrupt worker execution semantics, and the same runtime
path handles both manually submitted and schedule generated runs.

## Event-driven vs polling

We support both. The default `InMemoryQueue` is event-driven: when the
scheduler calls `put()` it sets an `asyncio.Event`, and any worker blocked in
`get()` unblocks immediately. Under load this removes the polling tax.

The SQLite-backed queue is poll-driven by necessity — there is no native
notification channel. It polls every `poll_interval` (default 100 ms).

Relative trade-offs in practice:

- Event-driven: sub-millisecond dispatch latency; tight coupling between
  scheduler and workers (they must share an event loop or process group).
- Polling: higher tail latency (half the interval on average), but workers
  can live in any process and survive scheduler restarts without callbacks.

A Redis-backed queue using pub/sub for wakeups and a sorted set for the
visibility timeout gets you event-driven semantics across processes. We did
not implement this in the first cut because it is the single largest source
of complexity and the SQLite path is enough to validate the model.

The current runtime now has two practical queue paths:

1. `InMemoryQueue` for single process low latency runs
2. `StoreBackedQueue` using `queue_items` in SQLite or PostgreSQL for
   durability and cleaner runtime separation

## Concurrency model

Workers are cooperative asyncio coroutines. Sync task bodies run in the
default executor (thread pool). This gives us CPU-bound tolerance without a
separate process pool and without forcing users to write async code.

A worker handles exactly one task at a time. Increasing concurrency means
creating more workers. This is intentional: per-task isolation is easier to
reason about than structured concurrency around a single task.

## Reference numbers

Reference machine: MacBook Pro M-series, Python 3.11, SQLite WAL.
Numbers are indicative, taken from `benchmarks/benchmark.py`:

- 1000 no-op tasks, 16 workers:  ~0.6 s end-to-end.
- 5000 no-op tasks, 64 workers:  ~0.9 s end-to-end.
- 20000 no-op tasks, 128 workers: ~4.1 s end-to-end.

These exercise the scheduler's dispatch loop and queue wakeup path, not
serious user work. Real tasks will bottleneck on whatever they are doing.
