import hashlib

import pytest

from worker.storage import ChunkStore


def test_atomic_chunk_store_and_persistence(tmp_path):
    store = ChunkStore(str(tmp_path))
    cid = "a" * 32
    blob = b"hello\x00world"
    checksum = hashlib.sha256(blob).hexdigest()
    assert store.put(cid, blob, checksum)["size"] == len(blob)
    assert store.get(cid).read_bytes() == blob
    assert ChunkStore(str(tmp_path)).inventory()[0]["checksum"] == checksum
    store.delete(cid)
    assert store.get(cid) is None


def test_rejects_path_traversal_and_checksum_mismatch(tmp_path):
    store = ChunkStore(str(tmp_path))
    with pytest.raises(ValueError):
        store.get("../../anything")
    with pytest.raises(ValueError):
        store.put("b" * 32, b"hi", "incorrect")
    assert not store.inventory()
