# Spawns N worker processes, each running their own asyncio event loop.
# Uses multiprocessing so each process gets its own GIL — true parallelism
# for I/O-bound tasks without fighting asyncio's single-threaded model.

import multiprocessing
import signal
import sys
import logging

from worker.worker import WorkerProcess

log = logging.getLogger(__name__)


class WorkerPool:
    def __init__(
        self,
        queues: list[str],
        num_workers: int = 2,
        concurrency: int = 4,
        redis_url: str = "redis://localhost:6379",
    ):
        self.queues = queues
        self.num_workers = num_workers
        self.concurrency = concurrency
        self.redis_url = redis_url
        self._processes: list[multiprocessing.Process] = []

    def start(self) -> None:
        """Spawn worker processes and wait until they all exit."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [pool] %(levelname)s %(message)s",
        )

        # Graceful shutdown on Ctrl+C or SIGTERM
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        log.info(f"Starting {self.num_workers} workers × {self.concurrency} concurrency")

        for i in range(self.num_workers):
            p = multiprocessing.Process(
                target=self._worker_target,
                name=f"worker-{i}",
                daemon=True,
            )
            p.start()
            self._processes.append(p)
            log.info(f"Spawned {p.name} (pid={p.pid})")

        # Block until all children exit
        for p in self._processes:
            p.join()

    def _worker_target(self) -> None:
        """Target function for each subprocess."""
        # Must import tasks here so the registry is populated in this process.
        import tasks.example_tasks  # noqa: F401

        w = WorkerProcess(
            queues=self.queues,
            redis_url=self.redis_url,
            concurrency=self.concurrency,
        )
        w.run()

    def _handle_signal(self, sig, frame) -> None:
        log.info(f"Received signal {sig} — terminating workers")
        for p in self._processes:
            p.terminate()
        sys.exit(0)