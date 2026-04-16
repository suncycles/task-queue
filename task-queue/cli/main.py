import asyncio
import json
import sys

import click
from rich.console import Console
from rich.table import Table

from broker.client import BrokerClient
from broker.queue_names import (
    ALL_QUEUES, queue_key, processing_key, dlq_key,
    heartbeat_key, WORKERS_SET,
)

console = Console()


def get_redis_url(ctx) -> str:
    return ctx.obj.get("redis_url", "redis://localhost:6379")


async def _make_client(redis_url: str) -> BrokerClient:
    c = BrokerClient(redis_url)
    await c.connect()
    return c


@click.group()
@click.option("--redis-url", default="redis://localhost:6379", envvar="REDIS_URL")
@click.pass_context
def cli(ctx, redis_url):
    """Distributed task queue CLI."""
    ctx.ensure_object(dict)
    ctx.obj["redis_url"] = redis_url


# ------------------------------------------------------------------
# Enqueue
# ------------------------------------------------------------------

@cli.command()
@click.argument("queue")
@click.argument("task_name")
@click.option("--args", default="[]", help="JSON array of positional args")
@click.option("--kwargs", default="{}", help="JSON object of keyword args")
@click.option("--max-retries", default=3)
@click.pass_context
def enqueue(ctx, queue, task_name, args, kwargs, max_retries):
    """Enqueue a task onto QUEUE."""
    try:
        args_list = json.loads(args)
        kwargs_dict = json.loads(kwargs)
    except json.JSONDecodeError as e:
        console.print(f"[red]Invalid JSON: {e}[/red]")
        sys.exit(1)

    async def _run():
        client = await _make_client(get_redis_url(ctx))
        job_id = await client.enqueue(queue, task_name, args_list, kwargs_dict, max_retries)
        await client.close()
        return job_id

    job_id = asyncio.run(_run())
    console.print(f"[green]Enqueued[/green] {job_id}")


# ------------------------------------------------------------------
# Status
# ------------------------------------------------------------------

@cli.command()
@click.argument("job_id")
@click.pass_context
def status(ctx, job_id):
    """Show the status of a job."""
    async def _run():
        client = await _make_client(get_redis_url(ctx))
        job = await client.get_job(job_id)
        await client.close()
        return job

    job = asyncio.run(_run())
    if job is None:
        console.print(f"[red]Job {job_id} not found[/red]")
        sys.exit(1)

    # Pretty-print as a two-column table
    t = Table(show_header=False, box=None, padding=(0, 1))
    for k, v in job.items():
        t.add_row(f"[bold]{k}[/bold]", str(v))
    console.print(t)


# ------------------------------------------------------------------
# Workers
# ------------------------------------------------------------------

@cli.command()
@click.pass_context
def workers(ctx):
    """List live workers (those with an active heartbeat)."""
    async def _run():
        client = await _make_client(get_redis_url(ctx))
        worker_ids = await client.redis.smembers(WORKERS_SET)
        rows = []
        for wid_bytes in worker_ids:
            wid = wid_bytes.decode() if isinstance(wid_bytes, bytes) else wid_bytes
            alive = await client.redis.exists(heartbeat_key(wid))
            rows.append((wid, "● alive" if alive else "✗ dead"))
        await client.close()
        return rows

    rows = asyncio.run(_run())
    t = Table("Worker ID", "Status")
    for wid, status_str in rows:
        color = "green" if "alive" in status_str else "red"
        t.add_row(wid, f"[{color}]{status_str}[/{color}]")
    console.print(t)


# ------------------------------------------------------------------
# Purge
# ------------------------------------------------------------------

@cli.command()
@click.argument("queue")
@click.pass_context
def purge(ctx, queue):
    """Delete all pending and processing jobs on QUEUE."""
    async def _run():
        client = await _make_client(get_redis_url(ctx))
        pipe = client.redis.pipeline()
        pipe.delete(queue_key(queue))
        pipe.delete(processing_key(queue))
        results = await pipe.execute()
        await client.close()
        return results

    asyncio.run(_run())
    console.print(f"[yellow]Purged[/yellow] queue:{queue} and queue:{queue}:processing")


# ------------------------------------------------------------------
# DLQ
# ------------------------------------------------------------------

@cli.group()
def dlq():
    """Dead-letter queue operations."""
    pass


@dlq.command("list")
@click.argument("queue")
@click.pass_context
def dlq_list(ctx, queue):
    """Inspect jobs in the dead-letter queue."""
    async def _run():
        client = await _make_client(get_redis_url(ctx))
        job_ids = await client.redis.lrange(dlq_key(queue), 0, -1)
        jobs = []
        for jid_bytes in job_ids:
            jid = jid_bytes.decode() if isinstance(jid_bytes, bytes) else jid_bytes
            job = await client.get_job(jid)
            if job:
                jobs.append(job)
        await client.close()
        return jobs

    jobs = asyncio.run(_run())
    if not jobs:
        console.print("[dim]DLQ is empty[/dim]")
        return

    t = Table("Job ID", "Function", "Retries", "Error")
    for job in jobs:
        err = (job.get("error") or "")[:60]
        t.add_row(job["id"], job["fn"], str(job["retries"]), err)
    console.print(t)


@dlq.command("retry")
@click.argument("queue")
@click.pass_context
def dlq_retry(ctx, queue):
    """Re-enqueue all dead-letter jobs on QUEUE."""
    async def _run():
        client = await _make_client(get_redis_url(ctx))
        key = dlq_key(queue)
        count = 0
        while True:
            job_id_bytes = await client.redis.rpop(key)
            if job_id_bytes is None:
                break
            job_id = job_id_bytes.decode() if isinstance(job_id_bytes, bytes) else job_id_bytes
            await client.update_job(job_id, {"status": "queued", "retries": 0, "error": None})
            await client.redis.lpush(queue_key(queue), job_id)
            count += 1
        await client.close()
        return count

    n = asyncio.run(_run())
    console.print(f"[green]Re-enqueued {n} jobs from DLQ[/green]")


# ------------------------------------------------------------------
# Start workers
# ------------------------------------------------------------------

@cli.command()
@click.option("--queues", default="default", help="Comma-separated queue names")
@click.option("--workers", "num_workers", default=2, type=int)
@click.option("--concurrency", default=4, type=int)
@click.pass_context
def start(ctx, queues, num_workers, concurrency):
    """Start a worker pool."""
    from worker.pool import WorkerPool
    queue_list = [q.strip() for q in queues.split(",")]
    pool = WorkerPool(
        queues=queue_list,
        num_workers=num_workers,
        concurrency=concurrency,
        redis_url=get_redis_url(ctx),
    )
    pool.start()


# ------------------------------------------------------------------
# Dashboard
# ------------------------------------------------------------------

@cli.command()
@click.pass_context
def dashboard(ctx):
    """Launch the live Rich dashboard."""
    from monitor.dashboard import run_dashboard
    asyncio.run(run_dashboard(get_redis_url(ctx)))