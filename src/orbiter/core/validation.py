from __future__ import annotations

from orbiter.core.dag import DAG


def validation_warnings(dag: DAG) -> list[str]:
    warnings: list[str] = []
    for task in dag.tasks.values():
        if task.timeout_seconds is None:
            warnings.append(
                f"task '{task.id}' has no timeout; a hung task can be re-delivered after lease expiry"
            )
    return warnings
