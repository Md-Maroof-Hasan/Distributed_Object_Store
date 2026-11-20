"""Distributed object-store coordinator, independent of the Flask API layer."""
from __future__ import annotations

import hashlib
import json
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone

import requests

from common.config import (
    CHUNK_SIZE, HEARTBEAT_TIMEOUT, REPLICATION_FACTOR, REQUEST_TIMEOUT,
)
from controller.database import db_session, init_db


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class StoreError(Exception):
    status_code = 500


class BadRequest(StoreError):
    status_code = 400


class NotFound(StoreError):
    status_code = 404


class Unavailable(StoreError):
    status_code = 503


class ObjectStore:
    def __init__(self, db_path: str, worker_urls: dict[str, str], http=None):
        self.db_path = db_path
        self.worker_urls = {k: v.rstrip("/") for k, v in worker_urls.items()}
        self.http = http or requests
        self.lock = threading.RLock()
        init_db(db_path, self.worker_urls)

    def register(self, worker_id: str) -> None:
        if worker_id not in self.worker_urls:
            raise BadRequest("unknown worker_id")
        with db_session(self.db_path) as db:
            db.execute("UPDATE workers SET last_seen=? WHERE worker_id=?",
                       (time.time(), worker_id))

    def online_workers(self) -> list[str]:
        with db_session(self.db_path) as db:
            rows = db.execute(
                "SELECT worker_id FROM workers WHERE last_seen >= ? ORDER BY worker_id",
                (time.time() - HEARTBEAT_TIMEOUT,),
            ).fetchall()
        return [r["worker_id"] for r in rows]

    def worker_status(self) -> dict:
        now = time.time()
        with db_session(self.db_path) as db:
            rows = db.execute("SELECT * FROM workers ORDER BY worker_id").fetchall()
        return {r["worker_id"]: {
            "url": r["url"], "online": r["last_seen"] >= now - HEARTBEAT_TIMEOUT,
            "last_seen": r["last_seen"] if r["last_seen"] else None,
        } for r in rows}

    def _put(self, worker_id: str, chunk_id: str, data: bytes, checksum: str) -> bool:
        try:
            response = self.http.put(
                f"{self.worker_urls[worker_id]}/internal/chunks/{chunk_id}",
                data=data, headers={"Content-Type": "application/octet-stream",
                                    "X-Chunk-SHA256": checksum},
                timeout=REQUEST_TIMEOUT,
            )
            return response.status_code in (200, 201)
        except requests.RequestException:
            return False

    def _get(self, worker_id: str, chunk_id: str, checksum: str, size: int) -> bytes | None:
        try:
            response = self.http.get(
                f"{self.worker_urls[worker_id]}/internal/chunks/{chunk_id}",
                timeout=REQUEST_TIMEOUT,
            )
            if response.status_code != 200:
                return None
            data = response.content
            if len(data) != size or hashlib.sha256(data).hexdigest() != checksum:
                return None
            return data
        except requests.RequestException:
            return None

    def _delete_remote(self, worker_id: str, chunk_id: str) -> bool:
        try:
            response = self.http.delete(
                f"{self.worker_urls[worker_id]}/internal/chunks/{chunk_id}",
                timeout=REQUEST_TIMEOUT,
            )
            return response.status_code in (200, 204, 404)
        except requests.RequestException:
            return False

    def upload(self, stream, filename: str, content_type: str, custom_metadata: dict) -> dict:
        if not filename:
            raise BadRequest("file must have a filename")
        if not isinstance(custom_metadata, dict):
            raise BadRequest("metadata must be a JSON object")
        workers = self.online_workers()
        if len(workers) < REPLICATION_FACTOR:
            raise Unavailable("at least two healthy workers are required for uploads")

        object_id = uuid.uuid4().hex
        chunks = []
        uploaded = []
        object_hash = hashlib.sha256()
        total_size = 0
        try:
            chunk_index = 0
            while True:
                block = stream.read(CHUNK_SIZE)
                if not block:
                    break
                total_size += len(block)
                object_hash.update(block)
                chunk_id = uuid.uuid4().hex
                checksum = hashlib.sha256(block).hexdigest()
                # Rotating placements distribute chunks over the four workers.
                candidates = workers[chunk_index % len(workers):] + workers[:chunk_index % len(workers)]
                copies = []
                for worker_id in candidates:
                    if self._put(worker_id, chunk_id, block, checksum):
                        uploaded.append((worker_id, chunk_id))
                        copies.append(worker_id)
                        if len(copies) == REPLICATION_FACTOR:
                            break
                if len(copies) != REPLICATION_FACTOR:
                    raise Unavailable(f"unable to store two replicas of chunk {chunk_index}")
                chunks.append((chunk_id, chunk_index, len(block), checksum, copies))
                chunk_index += 1

            with self.lock, db_session(self.db_path) as db:
                now = utc_now()
                db.execute("INSERT INTO objects VALUES (?,?,?,?,?,?,?,?)", (
                    object_id, filename, total_size, content_type or "application/octet-stream",
                    object_hash.hexdigest(), now, now, json.dumps(custom_metadata),
                ))
                for chunk_id, idx, size, checksum, copies in chunks:
                    db.execute("INSERT INTO chunks VALUES (?,?,?,?,?)",
                               (chunk_id, object_id, idx, size, checksum))
                    db.executemany("INSERT INTO replicas VALUES (?,?)",
                                   [(chunk_id, w) for w in copies])
        except Exception:
            # No object is committed if any mandatory replica failed.
            for worker_id, chunk_id in uploaded:
                self._delete_remote(worker_id, chunk_id)
            raise
        return self.metadata(object_id)

    def _object(self, db, object_id: str):
        row = db.execute("SELECT * FROM objects WHERE object_id=?", (object_id,)).fetchone()
        if row is None:
            raise NotFound("object not found")
        return row

    def metadata(self, object_id: str) -> dict:
        with db_session(self.db_path) as db:
            row = self._object(db, object_id)
            chunk_rows = db.execute(
                "SELECT * FROM chunks WHERE object_id=? ORDER BY chunk_index", (object_id,)
            ).fetchall()
            chunk_info = []
            alive = set(self.online_workers())
            for chunk in chunk_rows:
                replica_rows = db.execute(
                    "SELECT worker_id FROM replicas WHERE chunk_id=? ORDER BY worker_id",
                    (chunk["chunk_id"],),
                ).fetchall()
                replicas = [{"worker_id": r["worker_id"],
                             "online": r["worker_id"] in alive} for r in replica_rows]
                healthy_count = sum(r["online"] for r in replicas)
                chunk_info.append({
                    "chunk_id": chunk["chunk_id"], "index": chunk["chunk_index"],
                    "size": chunk["size"], "checksum": chunk["checksum"],
                    "replicas": replicas, "healthy_replica_count": healthy_count,
                    "replication_met": healthy_count >= REPLICATION_FACTOR,
                })
            return {
                "object_id": row["object_id"], "name": row["name"],
                "size": row["size"], "content_type": row["content_type"],
                "checksum": row["checksum"], "created_at": row["created_at"],
                "last_modified": row["modified_at"],
                "num_chunks": len(chunk_info), "chunks": chunk_info,
                "custom_metadata": json.loads(row["custom_metadata"]),
            }

    def list_objects(self) -> list[dict]:
        with db_session(self.db_path) as db:
            rows = db.execute(
                "SELECT object_id,name,size,content_type,created_at "
                "FROM objects ORDER BY created_at DESC"
            ).fetchall()
        return [{"object_id": r["object_id"], "name": r["name"],
                 "size": r["size"], "content_type": r["content_type"],
                 "created_at": r["created_at"]} for r in rows]

    def update_metadata(self, object_id: str, changes: dict) -> dict:
        if not isinstance(changes, dict):
            raise BadRequest("metadata must be a JSON object")
        with self.lock, db_session(self.db_path) as db:
            row = self._object(db, object_id)
            updated = json.loads(row["custom_metadata"])
            updated.update(changes)
            db.execute("UPDATE objects SET custom_metadata=?, modified_at=? "
                       "WHERE object_id=?", (json.dumps(updated), utc_now(), object_id))
        return self.metadata(object_id)

    def _discard_bad_replica(self, chunk_id: str, worker_id: str) -> None:
        # A verified-bad copy must never count towards replication.
        with self.lock, db_session(self.db_path) as db:
            db.execute("DELETE FROM replicas WHERE chunk_id=? AND worker_id=?",
                       (chunk_id, worker_id))
        self._delete_remote(worker_id, chunk_id)

    def download(self, object_id: str):
        """Validate all chunks *before* sending HTTP 200; spool large files to disk."""
        with db_session(self.db_path) as db:
            row = self._object(db, object_id)
            chunks = db.execute("SELECT * FROM chunks WHERE object_id=? ORDER BY chunk_index",
                                (object_id,)).fetchall()
            replicas = {
                chunk["chunk_id"]: [r["worker_id"] for r in db.execute(
                    "SELECT worker_id FROM replicas WHERE chunk_id=?", (chunk["chunk_id"],))]
                for chunk in chunks
            }
            name, content_type, expected_size, expected_checksum = (
                row["name"], row["content_type"], row["size"], row["checksum"])

        output = tempfile.SpooledTemporaryFile(max_size=4 * CHUNK_SIZE, mode="w+b")
        digest = hashlib.sha256()
        actual_size = 0
        alive = set(self.online_workers())
        try:
            for chunk in chunks:
                chunk_id = chunk["chunk_id"]
                candidates = sorted(replicas[chunk_id], key=lambda wid: wid not in alive)
                data = None
                for worker_id in candidates:
                    data = self._get(worker_id, chunk_id, chunk["checksum"], chunk["size"])
                    if data is not None:
                        break
                    # A timeout is not proof of corruption. Check the worker separately
                    # before dropping its replica record.
                    try:
                        probe = self.http.get(
                            f"{self.worker_urls[worker_id]}/internal/chunks/{chunk_id}",
                            timeout=REQUEST_TIMEOUT,
                        )
                        if probe.status_code == 200:
                            self._discard_bad_replica(chunk_id, worker_id)
                        elif probe.status_code == 404:
                            with self.lock, db_session(self.db_path) as db:
                                db.execute("DELETE FROM replicas WHERE chunk_id=? AND worker_id=?",
                                           (chunk_id, worker_id))
                    except requests.RequestException:
                        pass
                if data is None:
                    raise Unavailable(f"no valid copy of chunk {chunk['chunk_index']} available")
                output.write(data)
                digest.update(data)
                actual_size += len(data)
            if actual_size != expected_size or digest.hexdigest() != expected_checksum:
                raise Unavailable("download failed whole-object checksum verification")
            output.seek(0)
            return output, name, content_type
        except Exception:
            output.close()
            raise

    def delete(self, object_id: str) -> None:
        with self.lock, db_session(self.db_path) as db:
            self._object(db, object_id)
            rows = db.execute(
                "SELECT r.chunk_id,r.worker_id FROM replicas r "
                "JOIN chunks c ON c.chunk_id=r.chunk_id WHERE c.object_id=?", (object_id,)
            ).fetchall()
            db.executemany("INSERT OR IGNORE INTO pending_deletions VALUES (?,?)",
                           [(r["chunk_id"], r["worker_id"]) for r in rows])
            db.execute("DELETE FROM objects WHERE object_id=?", (object_id,))
        self.process_pending_deletions()

    def process_pending_deletions(self) -> int:
        with db_session(self.db_path) as db:
            rows = db.execute("SELECT * FROM pending_deletions").fetchall()
        completed = 0
        for row in rows:
            if self._delete_remote(row["worker_id"], row["chunk_id"]):
                with db_session(self.db_path) as db:
                    db.execute("DELETE FROM pending_deletions WHERE chunk_id=? AND worker_id=?",
                               (row["chunk_id"], row["worker_id"]))
                completed += 1
        return completed

    def _inventories(self, workers: list[str]) -> dict[str, dict]:
        inventories = {}
        for worker_id in workers:
            try:
                response = self.http.get(
                    f"{self.worker_urls[worker_id]}/internal/workers/{worker_id}/chunks",
                    timeout=REQUEST_TIMEOUT,
                )
                if response.status_code != 200:
                    continue
                inventories[worker_id] = {
                    item["chunk_id"]: item for item in response.json()["chunks"]
                }
            except (requests.RequestException, ValueError, KeyError):
                continue
        return inventories

    def reconcile(self) -> dict:
        """Repair missing/corrupt replicas and remove surplus copies after worker restart.

        Inventory checksums are computed from on-disk bytes on the worker.
        This intentionally favors correctness over performance for the course demo.
        """
        result = {"repaired": 0, "corrupt_or_missing": 0, "trimmed": 0,
                  "under_replicated": 0, "pending_deletions_cleared": 0}
        with self.lock:
            workers = self.online_workers()
            inventories = self._inventories(workers)
            with db_session(self.db_path) as db:
                chunks = db.execute("SELECT * FROM chunks ORDER BY object_id,chunk_index").fetchall()
            for chunk in chunks:
                chunk_id = chunk["chunk_id"]
                expected = {"size": chunk["size"], "checksum": chunk["checksum"]}
                with db_session(self.db_path) as db:
                    rows = db.execute("SELECT worker_id FROM replicas WHERE chunk_id=?",
                                      (chunk_id,)).fetchall()
                tracked = {r["worker_id"] for r in rows}
                # Adopt a surviving old copy, even if it was previously untracked.
                healthy = [wid for wid, inventory in inventories.items()
                           if chunk_id in inventory and
                           all(inventory[chunk_id].get(k) == v for k, v in expected.items())]
                for wid in tracked & inventories.keys():
                    if wid not in healthy:
                        with db_session(self.db_path) as db:
                            db.execute("DELETE FROM replicas WHERE chunk_id=? AND worker_id=?",
                                       (chunk_id, wid))
                        # If the file exists but has wrong checksum, erase it.
                        if chunk_id in inventories[wid]:
                            self._delete_remote(wid, chunk_id)
                            inventories[wid].pop(chunk_id, None)
                        result["corrupt_or_missing"] += 1
                for wid in healthy:
                    if wid not in tracked:
                        with db_session(self.db_path) as db:
                            db.execute("INSERT OR IGNORE INTO replicas VALUES (?,?)",
                                       (chunk_id, wid))
                # If a previously failed node returned, converge to exactly two
                # live copies; don't trim when a chunk is under-replicated.
                while len(healthy) > REPLICATION_FACTOR:
                    surplus = healthy.pop()
                    if self._delete_remote(surplus, chunk_id):
                        with db_session(self.db_path) as db:
                            db.execute("DELETE FROM replicas WHERE chunk_id=? AND worker_id=?",
                                       (chunk_id, surplus))
                        inventories[surplus].pop(chunk_id, None)
                        result["trimmed"] += 1
                    else:
                        healthy.append(surplus)
                        break
                # Before repairing, fetch a verified source; never copy corrupt bytes.
                while len(healthy) < REPLICATION_FACTOR:
                    source_data = None
                    for source in list(healthy):
                        source_data = self._get(source, chunk_id, chunk["checksum"], chunk["size"])
                        if source_data is not None:
                            break
                        self._discard_bad_replica(chunk_id, source)
                        inventories[source].pop(chunk_id, None)
                        healthy.remove(source)
                        result["corrupt_or_missing"] += 1
                    if source_data is None:
                        break
                    candidates = [wid for wid in sorted(inventories) if wid not in healthy]
                    created = False
                    for target in candidates:
                        if self._put(target, chunk_id, source_data, chunk["checksum"]):
                            with db_session(self.db_path) as db:
                                db.execute("INSERT OR IGNORE INTO replicas VALUES (?,?)",
                                           (chunk_id, target))
                            inventories[target][chunk_id] = {
                                "chunk_id": chunk_id, "size": chunk["size"],
                                "checksum": chunk["checksum"]}
                            healthy.append(target)
                            result["repaired"] += 1
                            created = True
                            break
                    if not created:
                        break
                if len(healthy) < REPLICATION_FACTOR:
                    result["under_replicated"] += 1
            result["pending_deletions_cleared"] = self.process_pending_deletions()
        return result
