# Rich live dashboard. All Redis reads are pipelined — one round-trip per refresh.
#
# Layout:
#   ┌─ Queue Status ─────────────────────────────────┐
#   │ Queue    Pending  Processing  DLQ   Rate/s      │
#   └────────────────────────────────────────────────┘
#   ┌─ Workers ──────────────────────────────────────┐
#   │ ID       Status   Queues      ...               │
#   └────────────────────────────────────────────────┘

import asyncio
import time
from collections import deque

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from broker.client import BrokerClient
from broker.queue_names import (
    ALL_QUEUES, queue_key, processing_key, dlq_key,
    heartbeat_key, stats_processed_key, WORKERS_SET,
)

REFRESH_INTERVAL = 1.0   # seconds between dashboard updates
RATE_WINDOW = 10         # number of samples to use for rate calculation


async def run_dashboard(redis_url: str = "redis://localhost:6379") -> None:
    client = BrokerClient(redis_url)
    await client.connect()

    # Rolling window of (timestamp, {queue: processed_count})
    history: deque = deque(maxlen=RATE_WINDOW + 1)
    console = Console()

    with Live(console=console, refresh_per_second=2, screen=True) as live:
        while True:
            try:
                snapshot = await _gather_snapshot(client)
                history.append((time.time(), snapshot["processed_counts"]))
                rates = _compute_rates(history)
                renderable = _build_renderable(snapshot, rates)
                live.update(renderable)
            except Exception as e:
                live.update(Text(f"Dashboard error: {e}", style="red"))
            await asyncio.sleep(REFRESH_INTERVAL)


# ------------------------------------------------------------------
# Data gathering — everything in one pipeline call
# ------------------------------------------------------------------

async def _gather_snapshot(client: BrokerClient) -> dict:
    """
    Fetch all queue depths, processed counters, and worker list
    in a single Redis pipeline (one TCP round-trip).
    """
    pipe = client.redis.pipeline(transaction=False)

    # Queue depths
    for q in ALL_QUEUES:
        pipe.llen(queue_key(q))
        pipe.llen(processing_key(q))
        pipe.llen(dlq_key(q))
        pipe.get(stats_processed_key(q))

    # Worker IDs
    pipe.smembers(WORKERS_SET)

    results = await pipe.execute()

    queue_data = {}
    idx = 0
    for q in ALL_QUEUES:
        pending     = results[idx];     idx += 1
        processing  = results[idx];     idx += 1
        dlq         = results[idx];     idx += 1
        processed   = results[idx];     idx += 1
        queue_data[q] = {
            "pending": pending or 0,
            "processing": processing or 0,
            "dlq": dlq or 0,
            "processed": int(processed or 0),
        }

    worker_ids_raw = results[idx]

    # Check heartbeats for each worker (separate pipeline)
    worker_ids = [
        w.decode() if isinstance(w, bytes) else w
        for w in (worker_ids_raw or set())
    ]

    if worker_ids:
        pipe2 = client.redis.pipeline(transaction=False)
        for wid in worker_ids:
            pipe2.exists(heartbeat_key(wid))
        heartbeats = await pipe2.execute()
    else:
        heartbeats = []

    workers = [
        {"id": wid, "alive": bool(hb)}
        for wid, hb in zip(worker_ids, heartbeats)
    ]

    return {
        "queue_data": queue_data,
        "workers": workers,
        "processed_counts": {q: queue_data[q]["processed"] for q in ALL_QUEUES},
        "timestamp": time.time(),
    }


def _compute_rates(history: deque) -> dict[str, float]:
    """Jobs/sec over the last RATE_WINDOW samples."""
    if len(history) < 2:
        return {q: 0.0 for q in ALL_QUEUES}

    oldest_ts, oldest_counts = history[0]
    newest_ts, newest_counts = history[-1]
    elapsed = newest_ts - oldest_ts
    if elapsed <= 0:
        return {q: 0.0 for q in ALL_QUEUES}

    return {
        q: max(0.0, (newest_counts.get(q, 0) - oldest_counts.get(q, 0)) / elapsed)
        for q in ALL_QUEUES
    }


# ------------------------------------------------------------------
# Rendering
# ------------------------------------------------------------------

def _build_renderable(snapshot: dict, rates: dict):
    layout = Layout()
    layout.split_column(
        Layout(name="queues", ratio=1),
        Layout(name="workers", ratio=1),
        Layout(name="footer", size=1),
    )

    layout["queues"].update(Panel(_queue_table(snapshot, rates), title="Queue Status"))
    layout["workers"].update(Panel(_worker_table(snapshot["workers"]), title="Workers"))
    layout["footer"].update(
        Text(f"  Last refresh: {time.strftime('%H:%M:%S')}  |  Press Ctrl+C to quit",
             style="dim")
    )
    return layout


def _queue_table(snapshot: dict, rates: dict) -> Table:
    t = Table(show_header=True, header_style="bold cyan", expand=True)
    t.add_column("Queue", style="bold")
    t.add_column("Pending",    justify="right")
    t.add_column("Processing", justify="right")
    t.add_column("DLQ",        justify="right", style="red")
    t.add_column("Rate/s",     justify="right", style="green")

    for q, data in snapshot["queue_data"].items():
        t.add_row(
            q,
            str(data["pending"]),
            str(data["processing"]),
            str(data["dlq"]),
            f"{rates.get(q, 0):.1f}",
        )
    return t


def _worker_table(workers: list[dict]) -> Table:
    t = Table(show_header=True, header_style="bold cyan", expand=True)
    t.add_column("Worker ID")
    t.add_column("Status")

    if not workers:
        t.add_row("[dim]no workers registered[/dim]", "")
        return t

    for w in sorted(workers, key=lambda x: x["id"]):
        if w["alive"]:
            t.add_row(w["id"], "[green]● alive[/green]")
        else:
            t.add_row(w["id"], "[red]✗ dead[/red]")
    return t