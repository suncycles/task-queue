# Queue name constants and configuration schema.
# Centralizing these prevents typos across the codebase.

from dataclasses import dataclass, field

DEFAULT = "default"
EMAILS = "emails"
REPORTS = "reports"

ALL_QUEUES = [DEFAULT, EMAILS, REPORTS]


@dataclass
class QueueConfig:
    name: str
    max_retries: int = 3
    priority: int = 0          # higher = checked first by workers
    ttl_seconds: int = 86_400  # how long to keep completed job hashes


# Registry used by the dashboard and CLI to know which queues exist.
QUEUE_CONFIGS: dict[str, QueueConfig] = {
    name: QueueConfig(name=name) for name in ALL_QUEUES
}


# --- Redis key helpers ---
# All key construction lives here so a rename is a one-line change.

def queue_key(name: str) -> str:
    return f"queue:{name}"

def processing_key(name: str) -> str:
    return f"queue:{name}:processing"

def dlq_key(name: str) -> str:
    return f"queue:{name}:dlq"

def delayed_key(name: str) -> str:
    return f"queue:{name}:delayed"

def job_key(job_id: str) -> str:
    return f"job:{job_id}"

def heartbeat_key(worker_id: str) -> str:
    return f"worker:{worker_id}:heartbeat"

def stats_processed_key(name: str) -> str:
    return f"stats:{name}:processed"

def stats_failed_key(name: str) -> str:
    return f"stats:{name}:failed"

def reaper_lock_key(name: str) -> str:
    return f"lock:reaper:{name}"

WORKERS_SET = "workers:registered"