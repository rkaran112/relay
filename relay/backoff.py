"""Retry backoff. Full jitter, per the AWS "Exponential Backoff and Jitter" post.

Full jitter (uniform over ``[0, exp]``) rather than fixed exponential: a batch of
jobs that all failed against the same downstream dependency must not retry in
lockstep and stampede it the instant it recovers. Spreading retries uniformly is
the whole point.
"""

from __future__ import annotations

import random


def full_jitter(attempts: int, *, base: float = 1.0, cap: float = 300.0) -> float:
    """Seconds to wait before the next attempt, drawn uniformly from ``[0, window]``
    where ``window = min(cap, base * 2**attempts)``.
    """
    window = min(cap, base * (2.0**attempts))
    return random.uniform(0, window)
