from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Literal


BackoffKind = Literal["fixed", "linear", "exponential"]


@dataclass(frozen=True)
class RetryPolicy:
    """Retry configuration for a task.

    The scheduler consults this on every failed attempt to decide:
    (a) should we retry?, (b) when should the next attempt be scheduled?.
    """

    max_attempts: int = 3
    backoff: BackoffKind = "exponential"
    base_seconds: float = 1.0
    max_seconds: float = 60.0
    jitter: float = 0.25

    def should_retry(self, attempt: int) -> bool:
        return attempt < self.max_attempts


def compute_delay(policy: RetryPolicy, attempt: int, rng: random.Random | None = None) -> float:
    """Return the delay in seconds before the next attempt.

    `attempt` is 1-indexed: after the first failure, attempt=1.
    """
    if attempt < 1:
        raise ValueError("attempt must be >= 1")

    r = rng or random

    if policy.backoff == "fixed":
        base = policy.base_seconds
    elif policy.backoff == "linear":
        base = policy.base_seconds * attempt
    elif policy.backoff == "exponential":
        base = policy.base_seconds * (2 ** (attempt - 1))
    else:
        raise ValueError(f"unknown backoff kind: {policy.backoff}")

    base = min(base, policy.max_seconds)

    if policy.jitter > 0:
        spread = base * policy.jitter
        base = base + r.uniform(-spread, spread)

    return max(0.0, base)
