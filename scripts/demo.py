#!/usr/bin/env python3
"""One-command walkthrough of the assignment's six mandatory demonstrations.

Usage, from project root:
    docker compose up --build -d
    python -m pip install requests
    python scripts/demo.py

This script temporarily stops and restarts worker2 and intentionally corrupts
one stored chunk. It uses a separate temporary object and deletes it at the end.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
BASE = os.getenv("BASE_URL", "http://127.0.0.1:5000")
CHUNK = 1024 * 1024


def api(method, path, **kwargs):
    response = requests.request(method, BASE + path, timeout=40, **kwargs)
    if not response.ok:
        raise RuntimeError(f"{method} {path}: {response.status_code}: {response.text[:300]}")
    return response


def compose(*args):
    cmd = ["docker", "compose", *args]
    print(" $", " ".join(cmd), flush=True)
    subprocess.run(cmd, cwd=ROOT, check=True)


def wait_for(predicate, description, limit=35):
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (requests.RequestException, RuntimeError):
            pass
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {description}")


def is_online(worker_id):
    status = api("GET", "/internal/workers").json()["workers"]
    return status[worker_id]["online"]


def assert_replication(object_id):
    meta = api("GET", f"/api/v1/objects/{object_id}/metadata").json()
    for chunk in meta["chunks"]:
        assert chunk["healthy_replica_count"] == 2, chunk
        assert len({r["worker_id"] for r in chunk["replicas"] if r["online"]}) == 2
    return meta


def main():
    print("Checking controller and 4 workers...")
    wait_for(lambda: all(api("GET", "/internal/workers").json()["workers"][f"worker{i}"]["online"]
                         for i in range(1, 5)), "all four worker heartbeats")
    original = os.urandom(2 * CHUNK + 12_345)
    object_id = None
    stopped = False
    try:
        print("\n[1] Uploading random 2+ MiB binary file with custom metadata...")
        response = api("POST", "/api/v1/objects",
                       files={"file": ("sample.bin", original, "application/octet-stream")},
                       data={"metadata": json.dumps({"course": "Cloud Computing", "owner": "student"})})
        obj = response.json()
        object_id = obj["object_id"]
        print("Object ID:", object_id)
        assert obj["num_chunks"] == 3, obj
        assert obj["chunks"][-1]["size"] == 12_345
        meta = assert_replication(object_id)
        print("[2] Chunk placement and replication factor 2:")
        for chunk in meta["chunks"]:
            print("   chunk", chunk["index"], "size", chunk["size"], "copies:",
                  [r["worker_id"] for r in chunk["replicas"]])

        downloaded = api("GET", f"/api/v1/objects/{object_id}").content
        assert downloaded == original
        print("[3] Original and downloaded SHA256 match:", hashlib.sha256(original).hexdigest())

        patched = api("PATCH", f"/api/v1/objects/{object_id}/metadata",
                      json={"owner": "updated-student", "assignment": "distributed-store"}).json()
        assert patched["custom_metadata"]["owner"] == "updated-student"
        assert patched["custom_metadata"]["course"] == "Cloud Computing"
        assert api("GET", "/api/v1/objects").json()["count"] >= 1
        print("[4] Metadata updated without uploading the file again.")

        print("\n[5] Stopping worker2 and waiting for failure detection...")
        compose("stop", "worker2")
        stopped = True
        wait_for(lambda: not is_online("worker2"), "worker2 to be marked offline", limit=35)
        print("Reconciliation:", api("POST", "/internal/admin/reconcile").json())
        assert_replication(object_id)
        assert api("GET", f"/api/v1/objects/{object_id}").content == original
        print("Object remains downloadable, with two online replicas per chunk.")
        compose("start", "worker2")
        stopped = False
        wait_for(lambda: is_online("worker2"), "worker2 to re-register", limit=35)
        print("Restart reconciliation:", api("POST", "/internal/admin/reconcile").json())
        assert_replication(object_id)

        print("\n[6] Intentionally corrupting a chunk on a worker...")
        meta = assert_replication(object_id)
        first = meta["chunks"][0]
        worker_id = next(r["worker_id"] for r in first["replicas"] if r["online"])
        chunk_id = first["chunk_id"]
        corruption_script = (
            "from pathlib import Path; "
            f"Path('/data/chunks/{chunk_id}.bin').write_bytes(b'INTENTIONALLY-CORRUPTED')"
        )
        compose("exec", "-T", worker_id, "python", "-c", corruption_script)
        assert api("GET", f"/api/v1/objects/{object_id}").content == original
        print("Downloaded intact content from a verified replica.")
        print("Integrity reconciliation:", api("POST", "/internal/admin/reconcile").json())
        assert_replication(object_id)
        print("Corrupted data not returned; replication restored.")

        print("\n[7] Deleting object and confirming 404...")
        assert api("DELETE", f"/api/v1/objects/{object_id}").status_code == 204
        object_id = None
        print("ALL SIX ASSIGNMENT DEMONSTRATIONS PASSED")
    finally:
        if stopped:
            print("Restarting worker2 during cleanup...")
            compose("start", "worker2")
        if object_id:
            try:
                api("DELETE", f"/api/v1/objects/{object_id}")
            except Exception:
                print("Cleanup warning: temporary object still exists:", object_id)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("DEMONSTRATION FAILED:", error, file=sys.stderr)
        sys.exit(1)
