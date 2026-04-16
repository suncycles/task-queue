# Retry timing with full jitter.
#
# Why full jitter instead of plain exponential?
# With plain exponential: all N workers that fail at the same time retry at
# the same time -> thundering herd -> they all fail again.
# With full jitter: retries spread randomly across [0, cap] -> load smooths out.
#
# Reference: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/

import random


def backoff_seconds(
    attempt: int,
    base: float = 2.0,
    cap: float = 300.0,
) -> float:
    """
    Full jitter backoff.

    attempt=0 -> uniform(0, 2)
    attempt=1 -> uniform(0, 4)
    attempt=5 -> uniform(0, 64)
    attempt=10+ -> uniform(0, 300)  # capped
    """
    ceiling = min(cap, base * (2 ** attempt))
    return random.uniform(0, ceiling)


def should_retry(retries: int, max_retries: int) -> bool:
    return retries < max_retries