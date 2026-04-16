"""
Smoke tests. Requires a live Redis on localhost:6379.
Run: pytest tests/ -v
"""
import asyncio
import pytest
from broker.client import BrokerClient
from broker.queue_names import ALL_QUEUES


@pytest.fixture
async def client():
    c = BrokerClient("redis://localhost:6379")
    await c.connect()
    # Flush test queues before each test
    for q in ALL_QUEUES:
        await c.redis.delete(f"queue:{q}", f"queue:{q}:processing", f"queue:{q}:dlq")
    yield c
    await c.close()


@pytest.mark.asyncio
async def test_enqueue_and_get_job(client):
    job_id = await client.enqueue("default", "tasks.example_tasks.noop", [], {})
    job = await client.get_job(job_id)
    assert job is not None
    assert job["fn"] == "tasks.example_tasks.noop"
    assert job["status"] == "queued"


@pytest.mark.asyncio
async def test_queue_depth_increments(client):
    for _ in range(5):
        await client.enqueue("default", "tasks.example_tasks.noop", [], {})
    depth = await client.redis.llen("queue:default")
    assert depth == 5


@pytest.mark.asyncio
async def test_cancel(client):
    job_id = await client.enqueue("default", "tasks.example_tasks.noop", [], {})
    cancelled = await client.cancel(job_id)
    assert cancelled is True
    job = await client.get_job(job_id)
    assert job["status"] == "cancelled"


@pytest.mark.asyncio
async def test_full_pipeline(client):
    """Enqueue → worker picks up → result available."""
    import tasks.example_tasks  # populate registry
    from worker.worker import WorkerProcess

    job_id = await client.enqueue("default", "tasks.example_tasks.add",
                                   [3, 4], {})

    # Run one tick of the worker inline (not a real process)
    worker = WorkerProcess(["default"], concurrency=1)
    worker._client = client
    worker._semaphore = asyncio.Semaphore(1)

    result_task = asyncio.create_task(
        client.get_result(job_id, timeout=10)
    )

    # Simulate one BRPOP pop
    raw = await client.redis.brpop(["queue:default"], timeout=2)
    assert raw is not None
    _, jid = raw
    await asyncio.create_task(worker._run_job(jid.decode(), "default"))

    result = await result_task
    assert result == 7