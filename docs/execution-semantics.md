# Execution semantics

Most of the value of writing down execution semantics comes from being
willing to admit what your system does *not* guarantee. This document is
that admission.

## At-least-once is the default

Every worker loop looks like this:

```
1. lease a queue item
2. transition task_run to RUNNING
3. call the user function
4. transition task_run to SUCCEEDED (write output)
5. ack the queue item (delete queue row)
```

A crash anywhere between steps 3 and 5 means:
- the user function ran,
- its side effects may be visible to the outside world,
- but the system does not yet know the task succeeded.

On restart, the lease expires, another worker leases the same queue item,
and **the user function runs again**. This is at-least-once delivery, full
stop.

We do not try to hide this with clever framing. Exactly-once in the face of
arbitrary side effects is impossible without a distributed transaction
spanning your database and the outside world, which does not exist for
almost anything you care about.

## What "exactly-once illusion" means

When a user declares an idempotency key on a task (explicitly, or by
accepting the default which is `<dag_run_id>:<task_id>`), the scheduler
does the following extra work on redelivery:

- Before enqueuing, it attempts to insert a `task_runs` row with that key.
- The `idempotency_key` column is a UNIQUE index.
- If the insert fails, the scheduler knows an attempt is already in flight
  or complete and skips the dispatch.

This gives you *exactly-once enqueue*, not exactly-once execution. If the
worker crashes mid-execution, the retry path creates a *new* attempt row
with a new key (derived from attempt number) and the user function runs
again. The user code must be idempotent to get actual exactly-once
behaviour.

The illusion is real: for tasks whose only side effect is a database write
guarded by the idempotency key, the observable behaviour is indistinguishable
from exactly-once. The implementation just makes no claim beyond that.

## The checkpointing boundary

Checkpoints (`store.checkpoint()`) are the only way to make partial progress
survive a crash. A long-running task should:

1. Do a chunk of work.
2. `store.checkpoint(dag_run_id, task_id, key="cursor", value=X)`.
3. Move on.

On a retry the task can read the checkpoint back and skip done chunks.
Checkpoint writes happen in the same SQLite file as state transitions, so
ordering is consistent: a checkpoint written before a crash is visible on
restart.

Checkpoints are namespaced by `(dag_run_id, task_id, key)`. A task reads its
own checkpoints by default. If downstream code needs an upstream checkpoint,
it should request it explicitly through `TaskContext.get_checkpoint(..., task_id="upstream_task")`.

## Deduplication: where the key comes from

The idempotency key is `SHA256(base | k1=v1 | k2=v2 | ...)` where:

- `base` is either the user-provided `idempotency_key` attribute of the
  task, or the default `<dag_run_id>:<task_id>`.
- the k=v pairs are sorted input parameters supplied by the scheduler
  (currently just `attempt`).

The key is deterministic given the same (dag_run, task, attempt). Two
schedulers racing to dispatch the same attempt will produce the same key,
and the UNIQUE index ensures only one wins.

Users who want a stronger guarantee — for example, "the same logical input
should never run twice even across DAG runs" — can pass
`idempotency_key="my-natural-key"` to the decorator. The scheduler will use
that as the base and skip re-dispatch across runs.

## Why the scheduler writes the state, not the worker

Two schedulers may try to dispatch the same task; only one will win the
UNIQUE insert. Two workers may consume the same queue row; only one holds
the lease. The state store is the arbiter in both cases. If we let the
worker be the source of truth, a partitioned worker could report success
without the scheduler noticing, and the DAG would never converge.

## Cancellation semantics

Cancelling a DAG run is cooperative at the runtime boundary:

1. The DAG run transitions to `cancelled`.
2. The scheduler stops dispatching new work for that run.
3. Workers skip queued messages they pick up after cancellation.
4. A task that is already running is not forcibly interrupted by the runtime.

This is deliberate. Hard killing arbitrary user code is not safe in a
generic Python workflow engine. If a task needs stronger cancellation
behaviour, the task body should poll `context.is_cancelled` and stop its own
work cleanly.

## Schedule semantics

Recurring schedules create normal DAG runs. After a run exists, there is no
special execution path for "scheduled" work. The scheduler, queue, workers,
state transitions, retries, and checkpoints behave exactly as they do for a
manually submitted run.

Today the schedule model is intentionally simple:

1. interval based timing only
2. overlap policies are `allow`, `forbid`, and `replace`
3. no catchup
4. no hard real-time guarantees

This keeps the semantics legible while the runtime remains single node and
SQLite backed.

## Summary table

| Event                                       | Observable effect                     |
| ------------------------------------------- | ------------------------------------- |
| Worker crash before task body               | Retry; body runs once                 |
| Worker crash after task body, before ack    | Retry; body runs twice                |
| Two workers leasing after expiry            | Body runs twice                       |
| Scheduler restart with queued tasks         | Queue survives; workers resume        |
| Scheduler restart with running tasks        | Workers finish; acks may be re-tried  |
| User-declared idempotency key + DB side-eff | Effectively exactly-once              |
