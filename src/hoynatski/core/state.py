from enum import Enum


class TaskState(str, Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead_letter"
    SKIPPED = "skipped"


class DagRunState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_TASK_STATES = {
    TaskState.SUCCEEDED,
    TaskState.FAILED,
    TaskState.DEAD_LETTER,
    TaskState.SKIPPED,
}

TERMINAL_DAG_STATES = {
    DagRunState.SUCCEEDED,
    DagRunState.FAILED,
    DagRunState.CANCELLED,
}
