import random

from hoynatski.retry.backoff import RetryPolicy, compute_delay


def test_exponential_backoff_grows():
    p = RetryPolicy(base_seconds=1, max_seconds=100, jitter=0)
    assert compute_delay(p, 1) == 1
    assert compute_delay(p, 2) == 2
    assert compute_delay(p, 3) == 4
    assert compute_delay(p, 4) == 8


def test_backoff_clamped_by_max():
    p = RetryPolicy(base_seconds=1, max_seconds=5, jitter=0)
    assert compute_delay(p, 10) == 5


def test_linear_backoff():
    p = RetryPolicy(backoff="linear", base_seconds=2, jitter=0, max_seconds=999)
    assert compute_delay(p, 1) == 2
    assert compute_delay(p, 3) == 6


def test_fixed_backoff():
    p = RetryPolicy(backoff="fixed", base_seconds=3, jitter=0)
    assert compute_delay(p, 1) == 3
    assert compute_delay(p, 5) == 3


def test_jitter_bounded():
    p = RetryPolicy(base_seconds=1, max_seconds=100, jitter=0.5)
    rng = random.Random(42)
    for attempt in range(1, 6):
        d = compute_delay(p, attempt, rng=rng)
        base = min(p.base_seconds * (2 ** (attempt - 1)), p.max_seconds)
        assert 0 <= d <= base * 1.5 + 1e-9


def test_should_retry():
    p = RetryPolicy(max_attempts=3)
    assert p.should_retry(1)
    assert p.should_retry(2)
    assert not p.should_retry(3)
