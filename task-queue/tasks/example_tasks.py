import time
import random
from worker.registry import task


@task
def noop() -> str:
    """Fastest possible task — useful for throughput benchmarking."""
    return "ok"


@task
def sleep_task(seconds: float = 0.05) -> str:
    """Simulates I/O-bound work."""
    time.sleep(seconds)
    return f"slept {seconds}s"


@task
def add(x: float, y: float) -> float:
    """Simple computation — sanity-check that args/kwargs round-trip correctly."""
    return x + y


@task
def flaky_task(fail_rate: float = 0.5) -> str:
    """
    Fails randomly — useful for testing retry/DLQ logic.
    Run with: task-queue enqueue default tasks.example_tasks.flaky_task
    Then watch it retry and eventually die.
    """
    if random.random() < fail_rate:
        raise RuntimeError(f"Flaky task failed (rate={fail_rate})")
    return "survived"


@task
def cpu_burn(iterations: int = 1_000_000) -> int:
    """CPU-bound work — demonstrates why we use threads."""
    return sum(i * i for i in range(iterations))