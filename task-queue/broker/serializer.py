# msgpack is used instead of JSON:
#   - ~30% smaller payloads for typical job data
#   - handles bytes natively
#   - faster encode/decode
#
# Tradeoff: not human-readable in redis-cli.
# To inspect a job: use `task-queue status <id>` — the CLI decodes it for you.

import msgpack


def encode(data: dict) -> bytes:
    """Serialize a dict to msgpack bytes."""
    return msgpack.packb(data, use_bin_type=True)


def decode(data: bytes) -> dict:
    """Deserialize msgpack bytes to a dict."""
    return msgpack.unpackb(data, raw=False)