"""In-memory fake network so recovery logic can be unit-tested without Docker."""
import hashlib
from urllib.parse import urlparse

import requests


class FakeResponse:
    def __init__(self, status_code, content=b"", json_body=None):
        self.status_code = status_code
        self.content = content
        self._json_body = json_body

    def json(self):
        return self._json_body


class FakeHTTP:
    def __init__(self):
        self.chunks = {f"worker{i}": {} for i in range(1, 5)}
        self.offline = set()

    def _decode(self, url):
        address = urlparse(url)
        worker_id = address.hostname
        if worker_id in self.offline:
            raise requests.ConnectionError(f"{worker_id} is offline")
        return worker_id, address.path

    def put(self, url, data, headers, timeout):
        wid, path = self._decode(url)
        chunk_id = path.split("/")[-1]
        checksum = hashlib.sha256(data).hexdigest()
        if headers["X-Chunk-SHA256"] != checksum:
            return FakeResponse(400)
        self.chunks[wid][chunk_id] = bytes(data)
        return FakeResponse(201)

    def get(self, url, timeout):
        wid, path = self._decode(url)
        if path.endswith("/chunks"):
            manifest = [{"chunk_id": cid, "size": len(blob),
                         "checksum": hashlib.sha256(blob).hexdigest()}
                        for cid, blob in self.chunks[wid].items()]
            return FakeResponse(200, json_body={"chunks": manifest, "worker_id": wid})
        chunk_id = path.split("/")[-1]
        if chunk_id not in self.chunks[wid]:
            return FakeResponse(404)
        return FakeResponse(200, content=self.chunks[wid][chunk_id])

    def delete(self, url, timeout):
        wid, path = self._decode(url)
        self.chunks[wid].pop(path.split("/")[-1], None)
        return FakeResponse(204)
