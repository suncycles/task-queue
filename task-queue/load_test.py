#!/usr/bin/env python3
"""
Benchmark: enqueue N jobs and measure throughput.

Usage:
    # Make sure Redis is running first:
    docker run -d -p 6379:6379 redis:7-alpine

    python load_test.py --count 10000 --queue default
"""
import asyncio
import time
import click
from broker.client import BrokerClient


@click.command()
@click.option("--count", default=1_000, type=int, help="Number of jobs to enqueue")
@click.option("--queue", default="default")
@click.option("--redis-url", default="redis://localhost:6379", envvar="REDIS_URL")
@click.option("--wait/--no-wait", default=False, help="Wait for all jobs to complete")
async def main(count, queue, redis_url, wait):
    client = BrokerClient(redis_url)
    await client.connect()

    print(f"Enqueueing {count:,} jobs on '{queue}'...")
    t0 = time.perf_counter()

    # Batch enqueue using asyncio.gather for max throughput
    tasks = [
        client.enqueue(queue, "tasks.example_tasks.noop", [], {})
        for _ in range(count)
    ]
    job_ids = await asyncio.gather(*tasks)

    enqueue_elapsed = time.perf_counter() - t0
    enqueue_rate = count / enqueue_elapsed

    print(f"  Enqueued {len(job_ids):,} jobs in {enqueue_elapsed:.2f}s")
    print(f"  Enqueue rate: {enqueue_rate:,.0f} jobs/s")

    if wait:
        print(f"Waiting for all {count:,} jobs to complete...")
        t1 = time.perf_counter()
        completed = 0
        for job_id in job_ids:
            try:
                await client.get_result(job_id, timeout=120)
                completed += 1
            except Exception:
                pass
        total_elapsed = time.perf_counter() - t1
        print(f"  Completed {completed:,}/{count:,} in {total_elapsed:.2f}s")
        print(f"  Throughput: {completed/total_elapsed:,.0f} jobs/s")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main.main(standalone_mode=False))