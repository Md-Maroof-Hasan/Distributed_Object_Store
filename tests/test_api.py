"""Flask end-to-end route checks (install requirements-dev.txt before running)."""
import io

import pytest
pytest.importorskip("flask")

from controller.main import create_app
from controller.service import ObjectStore
from tests.fake_http import FakeHTTP


def test_client_api_contract(tmp_path):
    transport = FakeHTTP()
    urls = {f"worker{i}": f"http://worker{i}:5000" for i in range(1, 5)}
    store = ObjectStore(str(tmp_path / "state.db"), urls, http=transport)
    for wid in urls:
        store.register(wid)
    app = create_app(store)
    app.testing = True
    client = app.test_client()

    assert client.get("/api/v1/objects").status_code == 200
    assert client.post("/api/v1/objects", data={}).status_code == 400
    upload = client.post("/api/v1/objects", data={
        "file": (io.BytesIO(b"hello-world"), "hello.txt"),
        "metadata": '{"course": "cloud"}',
    }, content_type="multipart/form-data")
    assert upload.status_code == 201
    oid = upload.get_json()["object_id"]
    assert client.get(f"/api/v1/objects/{oid}").data == b"hello-world"
    assert client.get(f"/api/v1/objects/{oid}/metadata").status_code == 200
    patched = client.patch(f"/api/v1/objects/{oid}/metadata", json={"owner": "test"})
    assert patched.get_json()["custom_metadata"]["owner"] == "test"
    assert client.get("/api/v1/objects").get_json()["count"] == 1
    assert client.delete(f"/api/v1/objects/{oid}").status_code == 204
    assert client.get(f"/api/v1/objects/{oid}").status_code == 404
    assert client.patch(f"/api/v1/objects/{oid}/metadata", json={"a": 1}).status_code == 404
