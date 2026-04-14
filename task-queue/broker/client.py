# The public API surface for producers.
# Consumers (workers) use this too for reading job state and writing results.
#
# Design: only the job ID is stored in the queue list — not the full payload.
# This means:
#   - The queue list stays lean (just UUIDs)
#   - Job state (retries, status, result) lives in one place: job:{id} Hash
#   - Updates don't require touching queue structure

import asyncio
import time
import uuid
from typing import Any

import redis.asyncio as aioredis

from broker.queue_names import queue_key, job_key, ALL_QUEUES
from broker.serializer import encode, decode


class BrokerClient:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self._redis: aioredis.Redis | None = None

    async def connect(self) -> None:
        self._redis = aioredis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=False,  # We handle bytes ourselves via msgpack
        )

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()

    @property
    def redis(self) -> aioredis.Redis:
        if self._redis is None:
            raise RuntimeError("Call await client.connect() first")
        return self._redis

    # ------------------------------------------------------------------
    # Enqueue
    # ------------------------------------------------------------------

    async def enqueue(
        self,
        queue: str,
        fn_name: str,
        args: list = None,
        kwargs: dict = None,
        max_retries: int = 3,
    ) -> str:
        """
        Add a job to the named queue.

        Returns the job_id so callers can poll for results.
        """
        if queue not in ALL_QUEUES:
            raise ValueError(f"Unknown queue '{queue}'. Add it to queue_names.py first.")

        job_id = str(uuid.uuid4())
        payload = {
            "id": job_id,
            "fn": fn_name,
            "args": args or [],
            "kwargs": kwargs or {},
            "status": "queued",
            "retries": 0,
            "max_retries": max_retries,
            "result": None,
            "error": None,
            "enqueued_at": time.time(),
            "started_at": None,
            "finished_at": None,
            "worker_id": None,
        }

        pipe = self.redis.pipeline()
        # Store full payload in the Hash
        pipe.hset(job_key(job_id), mapping={"data": encode(payload)})
        # Push only the ID onto the queue list (LPUSH = newest at left, BRPOP pops from right)
        pipe.lpush(queue_key(queue), job_id)
        await pipe.execute()

        return job_id

    # ------------------------------------------------------------------
    # Read job state
    # ------------------------------------------------------------------

    async def get_job(self, job_id: str) -> dict | None:
        """Return the full job dict, or None if not found."""
        raw = await self.redis.hget(job_key(job_id), "data")
        if raw is None:
            return None
        return decode(raw)

    async def get_result(
        self,
        job_id: str,
        timeout: float = 30.0,
        poll_interval: float = 0.25,
    ) -> Any:
        """
        Poll until the job is done or failed, then return its result.

        Raises TimeoutError if the job doesn't finish within `timeout` seconds.
        Raises RuntimeError if the job failed.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            job = await self.get_job(job_id)
            if job is None:
                raise KeyError(f"Job {job_id} not found")
            if job["status"] == "done":
                return job["result"]
            if job["status"] in ("failed", "dead"):
                raise RuntimeError(f"Job {job_id} failed: {job['error']}")
            await asyncio.sleep(poll_interval)
        raise TimeoutError(f"Job {job_id} did not complete within {timeout}s")

    async def cancel(self, job_id: str) -> bool:
        """
        Mark a job cancelled. If it's already running, the worker will
        notice on next heartbeat check and abandon it.

        Returns True if the job existed and was updated.
        """
        job = await self.get_job(job_id)
        if job is None:
            return False
        job["status"] = "cancelled"
        await self.redis.hset(job_key(job_id), mapping={"data": encode(job)})
        return True

    # ------------------------------------------------------------------
    # Write helpers (used by workers — kept here to colocate schema logic)
    # ------------------------------------------------------------------

    async def update_job(self, job_id: str, updates: dict) -> None:
        """Merge `updates` into the job Hash. Always re-encodes the full payload."""
        job = await self.get_job(job_id)
        if job is None:
            raise KeyError(f"Job {job_id} not found")
        job.update(updates)
        await self.redis.hset(job_key(job_id), mapping={"data": encode(job)})


# ------------------------------------------------------------------
# Module-level singleton for convenience
# ------------------------------------------------------------------
# Workers and the CLI use get_client() so they share connection setup logic.

_default_client: BrokerClient | None = None


async def get_client(redis_url: str = "redis://localhost:6379") -> BrokerClient:
    global _default_client
    if _default_client is None:
        _default_client = BrokerClient(redis_url)
        await _default_client.connect()
    return _default_client