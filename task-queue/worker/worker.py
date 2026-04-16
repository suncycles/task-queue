# The core worker: a single asyncio event loop that:
#   1. BRPOPs jobs from one or more queues
#   2. Atomically moves each job to the :processing list (lease)
#   3. Executes the job in a thread pool
#   4. Heartbeats every 5s so the reaper knows we're alive
#   5. Runs a scheduler that re-enqueues delayed (retry) jobs
#   6. Runs a reaper that recovers jobs from crashed workers
#
# One WorkerProcess = one OS process = one event loop.
# Parallelism comes from running multiple WorkerProcess instances
# (see worker/pool.py).

import asyncio
import os
import time
import uuid
import logging

import redis.asyncio as aioredis

from broker.client import BrokerClient
from broker.queue_names import (
    queue_key, processing_key, delayed_key,
    heartbeat_key, reaper_lock_key, WORKERS_SET,
)
from worker.executor import execute_job

log = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 5       # seconds between heartbeat writes
HEARTBEAT_TTL = 10           # seconds before key expires (must be > HEARTBEAT_INTERVAL)
REAPER_INTERVAL = 15         # seconds between reaper scans
SCHEDULER_INTERVAL = 1       # seconds between delayed-job scans
BRPOP_TIMEOUT = 2            # seconds to block on BRPOP before looping


class WorkerProcess:
    def __init__(
        self,
        queues: list[str],
        redis_url: str = "redis://localhost:6379",
        concurrency: int = 4,
    ):
        self.queues = queues
        self.redis_url = redis_url
        self.concurrency = concurrency
        self.worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        self._semaphore: asyncio.Semaphore | None = None
        self._client: BrokerClient | None = None
        self._running = True

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Called from a multiprocessing.Process — sets up and runs the event loop."""
        logging.basicConfig(
            level=logging.INFO,
            format=f"%(asctime)s [{self.worker_id}] %(levelname)s %(message)s",
        )
        asyncio.run(self._main())

    async def _main(self) -> None:
        self._client = BrokerClient(self.redis_url)
        await self._client.connect()
        self._semaphore = asyncio.Semaphore(self.concurrency)

        # Announce ourselves
        await self._client.redis.sadd(WORKERS_SET, self.worker_id)
        log.info(f"Started. Watching queues: {self.queues}")

        try:
            await asyncio.gather(
                self._brpop_loop(),
                self._heartbeat_loop(),
                self._scheduler_loop(),
                self._reaper_loop(),
            )
        finally:
            # Clean shutdown: remove from the registered set
            await self._client.redis.srem(WORKERS_SET, self.worker_id)
            await self._client.close()
            log.info("Shut down cleanly.")

    # ------------------------------------------------------------------
    # BRPOP loop
    # ------------------------------------------------------------------

    async def _brpop_loop(self) -> None:
        """Continuously pop jobs and dispatch them."""
        # BRPOP checks queues left-to-right, so put highest-priority first.
        queue_keys = [queue_key(q) for q in self.queues]

        while self._running:
            try:
                result = await self._client.redis.brpop(
                    queue_keys, timeout=BRPOP_TIMEOUT
                )
            except Exception as e:
                log.error(f"BRPOP error: {e}")
                await asyncio.sleep(1)
                continue

            if result is None:
                # Timeout — loop again (allows checking self._running)
                continue

            raw_queue_key, job_id_bytes = result
            queue_name = raw_queue_key.decode().removeprefix("queue:")
            job_id = job_id_bytes.decode()

            # Atomically move to :processing — proves we hold the lease
            await self._client.redis.lmove(
                queue_key(queue_name),
                processing_key(queue_name),
                "RIGHT",  # source direction
                "LEFT",   # dest direction
            )

            # Store queue on the job so executor can update stats correctly
            job = await self._client.get_job(job_id)
            if job:
                job["queue"] = queue_name
                await self._client.update_job(job_id, {"queue": queue_name})

            # Acquire concurrency slot, then run without blocking the loop
            await self._semaphore.acquire()
            asyncio.create_task(self._run_job(job_id, queue_name))

    async def _run_job(self, job_id: str, queue_name: str) -> None:
        try:
            await execute_job(job_id, self._client, self.worker_id)
        except Exception as e:
            log.exception(f"Unhandled error executing {job_id}: {e}")
        finally:
            # Remove from processing list on completion (success or failure/dlq)
            await self._client.redis.lrem(
                processing_key(queue_name), 1, job_id
            )
            self._semaphore.release()

    # ------------------------------------------------------------------
    # Heartbeat loop
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Write a TTL key every HEARTBEAT_INTERVAL seconds."""
        key = heartbeat_key(self.worker_id)
        while self._running:
            try:
                await self._client.redis.set(key, "alive", ex=HEARTBEAT_TTL)
            except Exception as e:
                log.warning(f"Heartbeat write failed: {e}")
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    # ------------------------------------------------------------------
    # Scheduler loop — re-enqueues delayed jobs whose time has come
    # ------------------------------------------------------------------

    async def _scheduler_loop(self) -> None:
        """Move jobs from :delayed sorted sets back to the main queue when ready."""
        while self._running:
            try:
                await self._tick_scheduler()
            except Exception as e:
                log.warning(f"Scheduler error: {e}")
            await asyncio.sleep(SCHEDULER_INTERVAL)

    async def _tick_scheduler(self) -> None:
        now = time.time()
        for queue_name in self.queues:
            key = delayed_key(queue_name)
            # Get all jobs whose score (scheduled_at timestamp) <= now
            due = await self._client.redis.zrangebyscore(key, 0, now)
            for job_id_bytes in due:
                job_id = job_id_bytes.decode() if isinstance(job_id_bytes, bytes) else job_id_bytes
                # Atomically remove from delayed + push to queue
                removed = await self._client.redis.zrem(key, job_id)
                if removed:
                    await self._client.redis.lpush(queue_key(queue_name), job_id)
                    log.info(f"Re-enqueued delayed job {job_id} on {queue_name}")

    # ------------------------------------------------------------------
    # Reaper — recovers jobs from dead workers
    # ------------------------------------------------------------------

    async def _reaper_loop(self) -> None:
        while self._running:
            await asyncio.sleep(REAPER_INTERVAL)
            try:
                await self._reap_dead_workers()
            except Exception as e:
                log.warning(f"Reaper error: {e}")

    async def _reap_dead_workers(self) -> None:
        """
        For each :processing list, find jobs whose owning worker is dead.
        Re-enqueue them with an incremented retry count.

        Uses a distributed lock (SET NX EX) to prevent multiple workers
        from racing to requeue the same job.
        """
        for queue_name in self.queues:
            lock_key = reaper_lock_key(queue_name)
            # Acquire lock: only one reaper runs per queue at a time
            acquired = await self._client.redis.set(
                lock_key, self.worker_id, nx=True, ex=30
            )
            if not acquired:
                continue  # another worker is reaping this queue

            try:
                proc_key = processing_key(queue_name)
                # Read all job IDs in the processing list (don't pop)
                job_ids = await self._client.redis.lrange(proc_key, 0, -1)

                for job_id_bytes in job_ids:
                    job_id = job_id_bytes.decode() if isinstance(job_id_bytes, bytes) else job_id_bytes
                    job = await self._client.get_job(job_id)
                    if job is None:
                        # Orphaned ID — remove it
                        await self._client.redis.lrem(proc_key, 1, job_id)
                        continue

                    worker_id = job.get("worker_id")
                    if worker_id is None:
                        continue

                    hb_key = heartbeat_key(worker_id)
                    alive = await self._client.redis.exists(hb_key)
                    if alive:
                        continue  # worker is fine

                    # Worker is dead — requeue
                    log.warning(
                        f"Worker {worker_id} appears dead. "
                        f"Requeueing job {job_id} (retry {job['retries']+1}/{job['max_retries']})"
                    )
                    await self._client.redis.lrem(proc_key, 1, job_id)
                    await self._client.update_job(job_id, {
                        "status": "queued",
                        "worker_id": None,
                        "retries": job["retries"] + 1,
                    })
                    await self._client.redis.lpush(queue_key(queue_name), job_id)

            finally:
                await self._client.redis.delete(lock_key)