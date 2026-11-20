import hashlib
import io

import pytest

from common.config import CHUNK_SIZE
from controller.service import NotFound, ObjectStore, Unavailable
from controller.database import db_session
from tests.fake_http import FakeHTTP


@pytest.fixture
def setup_store(tmp_path):
    http = FakeHTTP()
    worker_urls = {f"worker{i}": f"http://worker{i}:5000" for i in range(1, 5)}
    store = ObjectStore(str(tmp_path / "data" / "store.sqlite3"), worker_urls, http=http)
    for wid in worker_urls:
        store.register(wid)
    return store, http


def test_large_binary_upload_download_and_distribution(setup_store):
    store, http = setup_store
    original = bytes(range(256)) * (CHUNK_SIZE // 256 * 2) + b"last-chunk"
    meta = store.upload(io.BytesIO(original), "sample.bin", "application/octet-stream",
                        {"course": "Cloud"})
    assert meta["size"] == len(original)
    assert meta["num_chunks"] == 3
    assert [c["size"] for c in meta["chunks"]] == [CHUNK_SIZE, CHUNK_SIZE, 10]
    assert all(len(c["replicas"]) == 2 for c in meta["chunks"])
    assert all(len({r["worker_id"] for r in c["replicas"]}) == 2
               for c in meta["chunks"])
    assert len(set(r["worker_id"] for c in meta["chunks"] for r in c["replicas"])) == 4
    output, name, kind = store.download(meta["object_id"])
    assert output.read() == original
    output.close()
    assert name == "sample.bin"
    assert kind == "application/octet-stream"
    assert meta["checksum"] == hashlib.sha256(original).hexdigest()
    assert store.metadata(meta["object_id"])["custom_metadata"]["course"] == "Cloud"


def test_metadata_patch_and_list_and_delete(setup_store):
    store, http = setup_store
    m = store.upload(io.BytesIO(b"hello"), "notes.txt", "text/plain", {"owner": "A"})
    obj_id = m["object_id"]
    updated = store.update_metadata(obj_id, {"owner": "B", "semester": "6"})
    assert updated["custom_metadata"] == {"owner": "B", "semester": "6"}
    assert updated["checksum"] == m["checksum"]
    assert store.list_objects()[0]["object_id"] == obj_id
    store.delete(obj_id)
    assert all(not chunks for chunks in http.chunks.values())
    with pytest.raises(NotFound):
        store.metadata(obj_id)


def test_worker_failure_repair_and_restart_trim(setup_store):
    store, http = setup_store
    m = store.upload(io.BytesIO(b"hello-world"), "x.bin", "application/octet-stream", {})
    chunk_id = m["chunks"][0]["chunk_id"]
    # Initially worker1 + worker2 own the chunk. Worker2 becomes unavailable.
    assert chunk_id in http.chunks["worker2"]
    http.offline.add("worker2")
    with db_session(store.db_path) as db:
        db.execute("UPDATE workers SET last_seen=0 WHERE worker_id='worker2'")
    fixed = store.reconcile()
    assert fixed["repaired"] >= 1
    after = store.metadata(m["object_id"])
    assert after["chunks"][0]["healthy_replica_count"] == 2
    f, _, _ = store.download(m["object_id"])
    assert f.read() == b"hello-world"
    f.close()
    # Rejoining worker's volume still has its previous persistent chunk.
    http.offline.remove("worker2")
    store.register("worker2")
    again = store.reconcile()
    assert again["trimmed"] >= 1
    assert sum(chunk_id in chunks for chunks in http.chunks.values()) == 2


def test_corruption_detected_and_repaired(setup_store):
    store, http = setup_store
    raw = b"reliable content" * 500
    m = store.upload(io.BytesIO(raw), "file", "application/octet-stream", {})
    chunk = m["chunks"][0]
    bad_worker = chunk["replicas"][0]["worker_id"]
    http.chunks[bad_worker][chunk["chunk_id"]] = b"CORRUPTED!"
    f, _, _ = store.download(m["object_id"])
    assert f.read() == raw  # Surviving replica gives original bytes.
    f.close()
    result = store.reconcile()
    assert result["repaired"] >= 1
    meta2 = store.metadata(m["object_id"])
    assert meta2["chunks"][0]["healthy_replica_count"] == 2
    for r in meta2["chunks"][0]["replicas"]:
        if r["online"]:
            stored = http.chunks[r["worker_id"]][chunk["chunk_id"]]
            assert hashlib.sha256(stored).hexdigest() == chunk["checksum"]


def test_no_valid_copy_never_returns_corruption(setup_store):
    store, http = setup_store
    m = store.upload(io.BytesIO(b"data"), "file", "application/octet-stream", {})
    chunk = m["chunks"][0]
    for r in chunk["replicas"]:
        http.chunks[r["worker_id"]][chunk["chunk_id"]] = b"BAD"
    with pytest.raises(Unavailable):
        store.download(m["object_id"])
    result = store.reconcile()
    assert result["under_replicated"] == 1


def test_delete_retried_when_worker_returns(setup_store):
    store, http = setup_store
    m = store.upload(io.BytesIO(b"xyz"), "file", "application/octet-stream", {})
    cid = m["chunks"][0]["chunk_id"]
    http.offline.add("worker1")
    store.delete(m["object_id"])
    assert cid in http.chunks["worker1"]
    with db_session(store.db_path) as db:
        pending = db.execute("SELECT * FROM pending_deletions").fetchall()
    assert pending
    http.offline.remove("worker1")
    store.process_pending_deletions()
    assert cid not in http.chunks["worker1"]
    with db_session(store.db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM pending_deletions").fetchone()[0] == 0


def test_upload_without_healthy_workers_fails(setup_store):
    store, http = setup_store
    with db_session(store.db_path) as db:
        db.execute("UPDATE workers SET last_seen=0 WHERE worker_id IN ('worker2','worker3','worker4')")
    with pytest.raises(Unavailable):
        store.upload(io.BytesIO(b"data"), "x", "text/plain", {})


def test_controller_sqlite_metadata_survives_restart(setup_store):
    store, http = setup_store
    meta = store.upload(io.BytesIO(b"persist"), "persist.txt", "text/plain", {"x": "y"})
    restarted = ObjectStore(store.db_path, store.worker_urls, http=http)
    after = restarted.metadata(meta["object_id"])
    assert after["checksum"] == meta["checksum"]
    f, _, _ = restarted.download(meta["object_id"])
    assert f.read() == b"persist"
    f.close()
