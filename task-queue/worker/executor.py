# Runs the actual user function.
#
# Why ThreadPoolExecutor?
# - The BRPOP loop runs in asyncio. Blocking the event loop would freeze
#   heartbeats and prevent picking up new jobs.
# - asyncio.to_thread() puts the function in a thread pool, keeping the
#   event loop free.
# - If tasks are CPU-bound, use ProcessPoolExecutor instead — but then you
#   lose shared state, so think carefully.

import asyncio
import time
import traceback

from broker.client import BrokerClient
from broker.queue_names import dlq_key, delayed_key, stats_failed_key
from broker.serializer import encode
from worker.registry import get_task
from worker.retry import backoff_seconds, should_retry


async def execute_job(
    job_id: str,
    client: BrokerClient,
    worker_id: str,
) -> None:
    """
    Load the job, run the function, write the result back.
    Handles retries and DLQ promotion on failure.
    """
    job = await client.get_job(job_id)
    if job is None:
        # Job disappeared (manually deleted, expired) — skip silently.
        return

    if job["status"] == "cancelled":
        return

    # Mark as running
    await client.update_job(job_id, {
        "status": "running",
        "started_at": time.time(),
        "worker_id": worker_id,
    })

    try:
        fn = get_task(job["fn"])
        # Run in thread pool so the event loop stays unblocked.
        result = await asyncio.to_thread(fn, *job["args"], **job["kwargs"])

        await client.update_job(job_id, {
            "status": "done",
            "result": result,
            "finished_at": time.time(),
        })

        # Increment throughput counter
        queue_name = _infer_queue(job)  # best-effort
        if queue_name:
            await client.redis.incr(f"stats:{queue_name}:processed")

    except Exception:
        error_text = traceback.format_exc()
        retries = job["retries"]
        max_retries = job["max_retries"]

        if should_retry(retries, max_retries):
            delay = backoff_seconds(attempt=retries)
            scheduled_at = time.time() + delay
            new_retries = retries + 1

            await client.update_job(job_id, {
                "status": "retrying",
                "retries": new_retries,
                "error": error_text,
            })

            queue_name = job.get("queue", "default")
            # ZADD score = unix timestamp when the job becomes runnable
            await client.redis.zadd(
                delayed_key(queue_name),
                {job_id: scheduled_at},
            )
        else:
            # Exhausted retries — send to dead-letter queue
            queue_name = job.get("queue", "default")
            await client.update_job(job_id, {
                "status": "dead",
                "error": error_text,
                "finished_at": time.time(),
            })
            await client.redis.lpush(dlq_key(queue_name), job_id)
            await client.redis.incr(stats_failed_key(queue_name))


def _infer_queue(job: dict) -> str | None:
    """Jobs store their queue name in the payload if we put it there."""
    return job.get("queue")  # may be None for jobs enqueued before this field existed