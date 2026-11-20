#!/usr/bin/env python3
"""End-to-end durability + edge-case checks for the Flask object store.

Run from anywhere AFTER bringing up the project with Docker Compose:
    python scripts/reliability_check.py
    python scripts/reliability_check.py --no-disruption

The default test intentionally restarts all five project services, and briefly
stops two storage workers. It never uses `docker compose down -v` and only
creates/deletes its own objects. Avoid running against an important live demo.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import subprocess
import struct
import sys
import time
import wave
import zipfile
import zlib
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
BASE = os.environ.get("BASE_URL", "http://127.0.0.1:5000").rstrip("/")
CHUNK_SIZE = 1024 * 1024
WORKERS = tuple(f"worker{i}" for i in range(1, 5))


def ensure(condition, message):
    if not condition:
        raise AssertionError(message)


def request(method, path, **kwargs):
    return requests.request(method, BASE + path, timeout=(3, 60), **kwargs)


def want(method, path, expected, **kwargs):
    response = request(method, path, **kwargs)
    ensure(response.status_code == expected,
           f"{method} {path}: expected HTTP {expected}, got {response.status_code}: "
           f"{response.text[:300]}")
    return response


def compose(*args):
    command = ["docker", "compose", *args]
    print("  $", " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def until(predicate, description, timeout=60):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (requests.RequestException, AssertionError, ValueError, KeyError) as error:
            last_error = error
        time.sleep(1)
    raise RuntimeError(f"Timeout waiting for {description}; last error: {last_error}")


def status():
    return want("GET", "/internal/workers", 200).json()["workers"]


def all_workers_online():
    statuses = status()
    return all(statuses.get(worker, {}).get("online") for worker in WORKERS)


def get_metadata(object_id):
    return want("GET", f"/api/v1/objects/{object_id}/metadata", 200).json()


def check_replicas(object_id):
    metadata = get_metadata(object_id)
    for chunk in metadata["chunks"]:
        online_locations = {replica["worker_id"] for replica in chunk["replicas"]
                            if replica["online"]}
        ensure(len(online_locations) == 2 and chunk["replication_met"],
               f"Chunk {chunk['index']} not replicated to two online workers: {chunk}")


def pdf_bytes():
    # Small, valid single-page PDF, without external dependencies.
    body = [b"%PDF-1.4\n"]
    objects = (
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] /Contents 4 0 R >>",
        b"<< /Length 0 >>\nstream\n\nendstream",
    )
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(sum(map(len, body)))
        body.append(f"{index} 0 obj\n".encode() + obj + b"\nendobj\n")
    start_xref = sum(map(len, body))
    body.append(f"xref\n0 {len(offsets)}\n".encode() + b"0000000000 65535 f \n")
    body.extend(f"{offset:010} 00000 n \n".encode() for offset in offsets[1:])
    body.append(f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\n"
                f"startxref\n{start_xref}\n%%EOF\n".encode())
    return b"".join(body)


def zip_bytes():
    memory = io.BytesIO()
    with zipfile.ZipFile(memory, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("hello.txt", "Persistent object storage test\n")
    return memory.getvalue()


def wav_bytes():
    memory = io.BytesIO()
    with wave.open(memory, "wb") as audio:
        audio.setnchannels(1)
        audio.setsampwidth(1)
        audio.setframerate(8000)
        audio.writeframes(b"\x80" * 800)
    return memory.getvalue()


def png_bytes():
    # Valid 1x1 red RGBA PNG made entirely with the standard library.
    def chunk(tag, payload):
        return (struct.pack(">I", len(payload)) + tag + payload
                + struct.pack(">I", zlib.crc32(tag + payload) & 0xffffffff))
    header = struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0)
    pixels = zlib.compress(b"\x00\xff\x00\x00\xff")
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", header) + chunk(b"IDAT", pixels) + chunk(b"IEND", b"")


def test_objects(records):
    print("\n[1/5] Upload, download and verify several file types and boundaries")
    png = png_bytes()
    samples = [
        ("notes.txt", b"Flask distributed store\n", "text/plain"),
        ("pixel.png", png, "image/png"),
        ("document.pdf", pdf_bytes(), "application/pdf"),
        ("archive.zip", zip_bytes(), "application/zip"),
        ("audio.wav", wav_bytes(), "audio/wav"),
        ("empty.bin", b"", "application/octet-stream"),
        ("boundary-1m.bin", os.urandom(CHUNK_SIZE), "application/octet-stream"),
        ("boundary-plus1.bin", os.urandom(CHUNK_SIZE + 1), "application/octet-stream"),
        ("multi-chunk.bin", os.urandom(2 * CHUNK_SIZE + 12345), "application/octet-stream"),
    ]
    for name, content, mime in samples:
        response = want("POST", "/api/v1/objects", 201,
                        files={"file": (name, content, mime)},
                        data={"metadata": json.dumps({"test_suite": "reliability", "name": name})})
        metadata = response.json()
        object_id = metadata["object_id"]
        records[object_id] = (name, content, mime)
        expected_chunks = (len(content) + CHUNK_SIZE - 1) // CHUNK_SIZE
        ensure(metadata["num_chunks"] == expected_chunks,
               f"{name}: expected {expected_chunks} chunks, got {metadata['num_chunks']}")
        ensure(metadata["checksum"] == hashlib.sha256(content).hexdigest(),
               f"{name}: wrong checksum")
        ensure(metadata["content_type"] == mime, f"{name}: wrong MIME type")
        ensure(metadata["custom_metadata"]["name"] == name, f"{name}: missing custom metadata")
        downloaded = want("GET", f"/api/v1/objects/{object_id}", 200)
        ensure(downloaded.content == content, f"{name}: downloaded data differs")
        ensure(downloaded.headers["Content-Type"].startswith(mime),
               f"{name}: incorrect returned Content-Type")
        check_replicas(object_id)
        print(f"  PASS {name:22s} {len(content):>8} bytes / {expected_chunks} chunks")

    print("\n[2/5] Check list, metadata update, and invalid requests")
    listed = want("GET", "/api/v1/objects", 200).json()["objects"]
    all_ids = {entry["object_id"] for entry in listed}
    ensure(set(records).issubset(all_ids), "Object listing omitted one or more uploaded objects")
    object_id = next(iter(records))
    before = get_metadata(object_id)
    after = want("PATCH", f"/api/v1/objects/{object_id}/metadata", 200,
                 json={"round": 2}).json()
    ensure(after["checksum"] == before["checksum"], "Metadata update changed object checksum")
    ensure(after["custom_metadata"]["round"] == 2, "Metadata update not persisted")
    ensure(after["custom_metadata"]["test_suite"] == "reliability", "Metadata replaced old keys")
    missing = "0" * 32
    want("GET", f"/api/v1/objects/{missing}", 404)
    want("GET", f"/api/v1/objects/{missing}/metadata", 404)
    want("PATCH", f"/api/v1/objects/{missing}/metadata", 404, json={"x": 1})
    want("DELETE", f"/api/v1/objects/{missing}", 404)
    want("POST", "/api/v1/objects", 400, data={"metadata": "{}"})
    want("POST", "/api/v1/objects", 400,
         files={"file": ("bad.txt", b"data", "text/plain")},
         data={"metadata": "{not-valid-json"})
    print("  PASS listing, metadata merge, 404 responses, and malformed upload requests")


def test_restart(records):
    print("\n[3/5] Restart the controller and all four workers (volumes preserved)")
    compose("restart", "controller", *WORKERS)
    until(all_workers_online, "all four workers to register after restart", timeout=75)
    # Wait for all chunks to become readable again; this also tests durable SQLite
    # controller metadata + durable worker volumes, not just worker heartbeats.
    for object_id, (name, content, mime) in records.items():
        def retrievable():
            response = request("GET", f"/api/v1/objects/{object_id}")
            return response.status_code == 200 and response.content == content
        until(retrievable, f"{name} to remain downloadable after restart", timeout=60)
        check_replicas(object_id)
    print(f"  PASS all {len(records)} objects survived container restarts")


def test_two_worker_failure(records, stopped):
    print("\n[4/5] Simulate two workers holding the same chunk going offline")
    target_id = next(object_id for object_id, data in records.items()
                     if data[0] == "multi-chunk.bin")
    original = records[target_id][1]
    chunk = get_metadata(target_id)["chunks"][0]
    selected = [replica["worker_id"] for replica in chunk["replicas"]]
    ensure(len(selected) == 2, "Expected exactly two tracked replicas")
    print(f"  Stopping both workers holding chunk 0: {selected}")
    stopped.update(selected)
    compose("stop", "-t", "1", *selected)
    until(lambda: all(not status()[name]["online"] for name in selected),
          "both failed workers to be marked offline", timeout=45)

    response = request("GET", f"/api/v1/objects/{target_id}")
    # The normal outcome is 503: RF=2 cannot survive loss of both copies.
    # A previous background recovery could have created another verified copy.
    ensure(response.status_code in (200, 503),
           f"Unsafe/error response: HTTP {response.status_code}: {response.text[:200]}")
    if response.status_code == 200:
        ensure(response.content == original, "Returned damaged content under double failure")
        print("  PASS downloads still work with independently repaired replicas")
    else:
        print("  PASS HTTP 503: no corrupted or partial download returned")

    print("  Starting the stopped workers again")
    compose("start", *selected)
    stopped.difference_update(selected)
    until(all_workers_online, "all workers to re-register", timeout=75)
    def restored():
        reconciliation = request("POST", "/internal/admin/reconcile")
        if reconciliation.status_code != 200:
            return False
        data = request("GET", f"/api/v1/objects/{target_id}")
        return data.status_code == 200 and data.content == original
    until(restored, "full file to be retrievable after workers return", timeout=75)
    check_replicas(target_id)
    print("  PASS original contents restored and two healthy replicas verified")


def cleanup(records):
    print("\n[5/5] Delete temporary test objects")
    problems = []
    for object_id, (name, _content, _mime) in records.items():
        try:
            response = request("DELETE", f"/api/v1/objects/{object_id}")
            if response.status_code not in (204, 404):
                problems.append(f"{name}: DELETE returned {response.status_code}")
                continue
            check = request("GET", f"/api/v1/objects/{object_id}")
            if check.status_code != 404:
                problems.append(f"{name}: still accessible (HTTP {check.status_code})")
        except requests.RequestException as exc:
            problems.append(f"{name}: {exc}")
    if problems:
        print("  WARNING: some test objects may need cleanup:")
        for problem in problems:
            print("  -", problem)
        return False
    print("  PASS temporary objects removed")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--no-disruption", action="store_true",
                        help="only run file type and API checks; do not restart/stop containers")
    args = parser.parse_args()
    records = {}
    stopped = set()
    success = False
    cleanup_ok = False
    try:
        print("Checking controller and four workers at", BASE)
        until(all_workers_online, "all workers to be online", timeout=45)
        test_objects(records)
        if not args.no_disruption:
            test_restart(records)
            test_two_worker_failure(records, stopped)
        else:
            print("\n[3-4/5] Skipping disruption tests (--no-disruption)")
        success = True
    finally:
        if stopped:
            print("\nSafety cleanup: restarting stopped workers", sorted(stopped))
            try:
                compose("start", *sorted(stopped))
                until(all_workers_online, "all workers online for cleanup", timeout=75)
            except Exception as error:
                print("WARNING: Could not restart all workers:", error, file=sys.stderr)
        cleanup_ok = cleanup(records)
    if not cleanup_ok:
        raise RuntimeError("Some temporary test objects were not cleaned up")
    if success:
        print("\nPASS: ALL RELIABILITY CHECKS COMPLETED")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("\nRELIABILITY CHECK FAILED:", exc, file=sys.stderr)
        sys.exit(1)
