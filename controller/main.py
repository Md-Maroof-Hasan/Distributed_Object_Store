"""Client-facing Flask REST API plus internal registration and maintenance APIs."""
from __future__ import annotations

import json
import os
import threading
import time

from flask import Flask, jsonify, request, send_file
from werkzeug.exceptions import HTTPException

from common.config import RECONCILE_INTERVAL, WORKER_URLS
from controller.service import BadRequest, ObjectStore, StoreError


def create_app(store=None):
    app = Flask(__name__)
    storage = store or ObjectStore(os.getenv("DB_PATH", "/data/controller.sqlite3"), WORKER_URLS)
    app.config["STORE"] = storage

    @app.errorhandler(StoreError)
    def store_error(exc):
        return jsonify(error=str(exc)), exc.status_code

    @app.errorhandler(HTTPException)
    def http_error(exc):
        return jsonify(error=exc.description), exc.code

    @app.errorhandler(Exception)
    def internal_error(exc):
        app.logger.exception("Unexpected server error: %s", exc)
        return jsonify(error="internal server error"), 500

    @app.get("/health")
    def health():
        return jsonify(status="ok", service="controller")

    @app.post("/api/v1/objects")
    def upload():
        if "file" not in request.files:
            raise BadRequest("multipart/form-data must include a 'file' field")
        file = request.files["file"]
        raw_metadata = request.form.get("metadata", "{}")
        try:
            user_metadata = json.loads(raw_metadata)
        except json.JSONDecodeError:
            raise BadRequest("metadata must contain valid JSON") from None
        if not isinstance(user_metadata, dict):
            raise BadRequest("metadata must be a JSON object")
        info = storage.upload(file.stream, file.filename or "", file.mimetype, user_metadata)
        return jsonify(info), 201

    @app.get("/api/v1/objects")
    def list_objects():
        objects = storage.list_objects()
        return jsonify(objects=objects, count=len(objects))

    @app.get("/api/v1/objects/<object_id>")
    def download(object_id):
        buffered_file, name, content_type = storage.download(object_id)
        response = send_file(buffered_file, mimetype=content_type,
                             as_attachment=True, download_name=name)
        response.call_on_close(buffered_file.close)
        return response

    @app.get("/api/v1/objects/<object_id>/metadata")
    def get_metadata(object_id):
        return jsonify(storage.metadata(object_id))

    @app.patch("/api/v1/objects/<object_id>/metadata")
    def update_metadata(object_id):
        changes = request.get_json(silent=True)
        if not isinstance(changes, dict):
            raise BadRequest("request body must be a JSON object")
        # Direct JSON key-value updates are canonical. Also support {"metadata": {...}}.
        if set(changes) == {"metadata"} and isinstance(changes["metadata"], dict):
            changes = changes["metadata"]
        return jsonify(storage.update_metadata(object_id, changes))

    @app.delete("/api/v1/objects/<object_id>")
    def delete_object(object_id):
        storage.delete(object_id)
        return "", 204

    @app.post("/internal/workers/register")
    def register():
        body = request.get_json(silent=True) or {}
        storage.register(body.get("worker_id", ""))
        return jsonify(registered=True, worker_id=body["worker_id"]), 200

    @app.post("/internal/workers/heartbeat")
    def heartbeat():
        body = request.get_json(silent=True) or {}
        storage.register(body.get("worker_id", ""))
        return jsonify(accepted=True), 200

    @app.get("/internal/workers")
    def worker_status():
        return jsonify(workers=storage.worker_status())

    # Manual one-shot reconciliation makes the failure/corruption demo deterministic.
    # Not exposed to public networks in the supplied Docker Compose configuration.
    @app.post("/internal/admin/reconcile")
    def manual_reconcile():
        return jsonify(storage.reconcile())

    return app


def recovery_loop(store: ObjectStore):
    while True:
        time.sleep(RECONCILE_INTERVAL)
        try:
            result = store.reconcile()
            if result["repaired"] or result["corrupt_or_missing"] or result["under_replicated"]:
                print(f"[controller] reconciliation: {result}", flush=True)
        except Exception as exc:
            print(f"[controller] reconciliation error: {exc}", flush=True)


if __name__ == "__main__":
    app = create_app()
    maintenance = threading.Thread(target=recovery_loop,
                                   args=(app.config["STORE"],), daemon=True)
    maintenance.start()
    app.run(host="0.0.0.0", port=5000, threaded=True, use_reloader=False)
