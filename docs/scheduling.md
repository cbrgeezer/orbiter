# Scheduling

Orbiter now supports recurring schedules for the single node runtime.

## What a schedule is

A schedule is a durable record that says:

1. which DAG fingerprint to run
2. how often to run it
3. what params to submit with each run
4. which overlap policy to apply
5. when the next run should fire
6. whether the schedule is active or paused

Schedules are stored in the same SQLite database as DAG runs, task runs,
checkpoints, and queue items. This keeps the single node operator story
simple: one file, one runtime, one source of truth.

## Current semantics

1. Schedules are interval based, using `interval_seconds`.
2. The background schedule loop polls for due schedules and creates pending
   DAG runs with a `trigger` value of `schedule:<schedule_id>`.
3. Manual triggers create runs with `trigger` set to
   `schedule:<schedule_id>:manual`.
4. Pausing a schedule stops new scheduled runs from being created.
5. Resuming a schedule re-enables future runs from the current `next_run_at`.
6. Overlap policies control what happens when a due schedule already has an
   active run.

## Overlap policies

Orbiter currently supports three overlap policies:

1. `allow`
   Always create a new run, even if an earlier run from the same schedule is
   still pending or running.
2. `forbid`
   Do not create a new run while an earlier run from the same schedule is
   still pending or running. Advance `next_run_at` and wait for the next slot.
3. `replace`
   Cancel pending or running runs from the same schedule, then create a fresh
   run for the current slot.

`replace` uses Orbiter's existing cancellation semantics. It does not hard
kill already running user code. It marks the old run as cancelled and lets
the runtime stop dispatching new work for it.

## What it does not do yet

1. No cron expressions.
2. No catchup or backfill.
3. No overlap policies such as forbid, replace, or queue-one.
4. No calendar semantics or timezone aware scheduling.

Those are deliberate next steps rather than hidden limitations.

## CLI flow

Create a schedule:

```bash
orbiter create-schedule examples/example_dag.py \
  --name hourly-import \
  --every-seconds 3600 \
  --overlap-policy forbid \
  --db orbiter.db \
  --params '{"rows": 500}'
```

List schedules:

```bash
orbiter list-schedules orbiter.db
```

Pause or resume:

```bash
orbiter pause-schedule orbiter.db <schedule_id>
orbiter resume-schedule orbiter.db <schedule_id>
```

Run one immediately:

```bash
orbiter run-schedule orbiter.db <schedule_id> examples/example_dag.py
```

## API flow

Create a schedule:

```http
POST /schedules
{
  "name": "hourly-import",
  "interval_seconds": 3600,
  "overlap_policy": "forbid",
  "params": { "rows": 500 }
}
```

Pause, resume, or run now:

```http
POST /schedules/{id}/pause
POST /schedules/{id}/resume
POST /schedules/{id}/run
```
