"""Shared constants and environment configuration."""
import os

CHUNK_SIZE = 1024 * 1024  # Exactly 1 MiB (1,048,576 bytes)
REPLICATION_FACTOR = 2
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "3"))
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "2"))
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "8"))
RECONCILE_INTERVAL = float(os.getenv("RECONCILE_INTERVAL", "4"))

WORKER_URLS = {
    f"worker{i}": os.getenv(f"WORKER{i}_URL", f"http://worker{i}:5000")
    for i in range(1, 5)
}
