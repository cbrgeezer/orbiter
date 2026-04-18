# Failure scenarios

A catalogue of the bad things that can happen and what the system does
about them. Everything listed here has a corresponding test in `tests/`.

## 1. Worker crash mid-task

**What happens.** A worker picks up a task, starts running it, and the
process dies (OOM, segfault, SIGKILL from the scheduler's parent, infra
preemption).

**Recovery.** The queue row's lease expires. `reclaim_expired_leases()`
returns it to the unleased pool on the next scheduler tick. Another worker
leases it. The task runs again.

**Consequence.** At-least-once: the body may execute twice.

**Test.** `tests/test_fault_injection.py::test_crash_before_task_recovers_via_retry`.

## 2. Scheduler restart with in-flight tasks

**What happens.** The scheduler process dies while workers are busy.

**Recovery.** Workers do not depend on the scheduler during execution; they
only talk to the queue and the store. They finish their current task,
write SUCCEEDED, and ack. On restart the scheduler reads task_runs and
continues dispatching.

**Consequence.** No duplicate execution. The DAG run may stall if the
scheduler is the only producer of new work and it takes time to come back.

## 3. Queue redelivery under broker failure

**What happens.** The queue backend double-delivers a message (SQS-style,
the broker itself has at-least-once semantics).

**Recovery.** The scheduler's `create_task_run()` call for the attempt will
fail the UNIQUE index on `idempotency_key` the second time around; the
duplicate is dropped before the worker runs anything.

**Consequence.** Safe.

**Test.** `tests/test_idempotency.py::test_duplicate_idempotency_key_is_skipped`.

## 4. DB transient errors

**What happens.** The store's write fails with a retryable error (SQLite
BUSY, Postgres connection dropped).

**Recovery.** The scheduler does not have its own retry layer for these
today. The surrounding asyncio task will raise; the scheduler loop catches
it and retries on the next tick. If the DB is persistently unavailable the
whole pipeline stops, which is the correct failure mode — we would rather
stall than silently drop state.

**Consequence.** Head-of-line blocking, not data loss.

## 5. Poison messages (infinite retry)

**What happens.** A task fails every single attempt. Could be a bug, could
be a bad input, could be a deleted upstream resource.

**Recovery.** The retry policy has a hard `max_attempts`. After that the
task transitions to `dead_letter`, the DAG run fails, and an operator can
inspect the error from the store. There is no infinite retry loop by
design.

**Consequence.** The DAG run becomes terminal with state FAILED. Upstream
retry (re-submitting the run) is the user's call.

**Test.** `tests/test_executor.py::test_dead_letter_after_exhausting_retries`.

## 6. DAG upgrade while a run is in flight

**What happens.** The user deploys a new version of a DAG while an old run
is still progressing.

**Recovery.** The old run is keyed by the old DAG's fingerprint. The new
DAG gets a new fingerprint and is stored alongside the old one. Old runs
continue against the old definition. There is no in-place upgrade.

**Consequence.** No task-graph confusion. The cost is that you cannot
"upgrade" a running pipeline to a new definition mid-flight — if that is
needed, cancel and restart.

## 7. Clock skew

**What happens.** Worker host clock jumps.

**Recovery.** The only code that cares about wall-clock time is the queue's
delayed-delivery and lease-expiry. Moderate skew (seconds) is absorbed by
the lease duration default (60 s). Large skew can cause premature lease
reclaim; workers then double-process. This is identical to case 1.

**Consequence.** At-least-once, same as a worker crash. We do not try to
synchronise clocks across nodes.

## 8. Runaway task (no timeout)

**What happens.** A task without a `timeout_seconds` hangs indefinitely.

**Recovery.** The lease still expires, a second worker picks it up, and you
now have two copies of the task running.

**Consequence.** Resource doubling. *This is why every production task
should set a timeout.* The CLI `validate` command surfaces tasks without a
timeout as a warning (roadmap item, see `docs/roadmap.md`).
