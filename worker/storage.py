"""One directory of durable, content-verified chunk files per worker."""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path

from common.config import CHUNK_SIZE

CHUNK_ID_RE = re.compile(r"^[0-9a-f]{32}$")


class ChunkStore:
    def __init__(self, path: str):
        self.root = Path(path)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, chunk_id: str) -> Path:
        if not CHUNK_ID_RE.fullmatch(chunk_id):
            raise ValueError("chunk_id must be a 32-character lowercase hex ID")
        return self.root / f"{chunk_id}.bin"

    def put(self, chunk_id: str, data: bytes, expected_checksum: str) -> dict:
        target = self._path(chunk_id)
        if len(data) > CHUNK_SIZE:
            raise ValueError("chunk exceeds 1 MiB")
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected_checksum:
            raise ValueError("checksum mismatch")
        # Atomic replace prevents readers from observing a partially written chunk.
        fd, temp_name = tempfile.mkstemp(dir=self.root, prefix=".upload-")
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(data)
                out.flush()
                os.fsync(out.fileno())
            os.replace(temp_name, target)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
        return {"chunk_id": chunk_id, "size": len(data), "checksum": actual}

    def get(self, chunk_id: str) -> Path | None:
        path = self._path(chunk_id)
        return path if path.is_file() else None

    def delete(self, chunk_id: str) -> None:
        self._path(chunk_id).unlink(missing_ok=True)

    def inventory(self) -> list[dict]:
        result = []
        for path in self.root.glob("*.bin"):
            chunk_id = path.stem
            if not CHUNK_ID_RE.fullmatch(chunk_id):
                continue
            try:
                with path.open("rb") as f:
                    digest = hashlib.file_digest(f, "sha256").hexdigest()
                result.append({
                    "chunk_id": chunk_id,
                    "size": path.stat().st_size,
                    "checksum": digest,
                })
            except FileNotFoundError:
                continue  # Chunk deleted while we were scanning.
        return result
