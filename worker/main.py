"""HTTP storage node: persist chunks and report liveness to the controller."""
from __future__ import annotations

import os
import threading
import time

import requests
from flask import Flask, jsonify, request, send_file
from werkzeug.exceptions import HTTPException

from common.config import CHUNK_SIZE, HEARTBEAT_INTERVAL, REQUEST_TIMEOUT
from worker.storage import ChunkStore


def create_app(store=None, worker_id=None):
    app = Flask(__name__)
    node_id = worker_id or os.getenv("WORKER_ID", "worker1")
    storage = store or ChunkStore(os.getenv("STORAGE_DIR", "/data/chunks"))
    app.config["WORKER_ID"] = node_id
    app.config["STORE"] = storage

    @app.errorhandler(HTTPException)
    def http_error(exc):
        return jsonify(error=exc.description), exc.code

    @app.errorhandler(Exception)
    def internal_error(exc):
        app.logger.exception("Worker error: %s", exc)
        return jsonify(error="worker storage error"), 500

    @app.get("/health")
    def health():
        return jsonify({"worker_id": node_id, "status": "ok"})

    @app.put("/internal/chunks/<chunk_id>")
    def put_chunk(chunk_id):
        try:
            if request.content_length is not None and request.content_length > CHUNK_SIZE:
                return jsonify(error="chunk exceeds 1 MiB"), 400
            data = request.get_data(cache=False)
            result = storage.put(chunk_id, data, request.headers.get("X-Chunk-SHA256", ""))
            return jsonify(result), 201
        except ValueError as exc:
            return jsonify(error=str(exc)), 400

    @app.get("/internal/chunks/<chunk_id>")
    def get_chunk(chunk_id):
        try:
            path = storage.get(chunk_id)
        except ValueError as exc:
            return jsonify(error=str(exc)), 400
        if path is None:
            return jsonify(error="chunk not found"), 404
        return send_file(path, mimetype="application/octet-stream")

    @app.delete("/internal/chunks/<chunk_id>")
    def delete_chunk(chunk_id):
        try:
            storage.delete(chunk_id)
        except ValueError as exc:
            return jsonify(error=str(exc)), 400
        return "", 204

    @app.get("/internal/workers/<requested_id>/chunks")
    def inventory(requested_id):
        if requested_id != node_id:
            return jsonify(error="unknown worker"), 404
        return jsonify({"worker_id": node_id, "chunks": storage.inventory()})

    return app


def heartbeat_loop(node_id: str, controller_url: str):
    # A worker keeps retrying even if the controller starts later or restarts.
    while True:
        try:
            payload = {"worker_id": node_id}
            requests.post(
                f"{controller_url}/internal/workers/register",
                json=payload, timeout=REQUEST_TIMEOUT,
            ).raise_for_status()
            # A registration also acts as a heartbeat.
        except requests.RequestException as exc:
            print(f"[{node_id}] controller registration retry: {exc}", flush=True)
        time.sleep(HEARTBEAT_INTERVAL)
        try:
            requests.post(
                f"{controller_url}/internal/workers/heartbeat",
                json={"worker_id": node_id}, timeout=REQUEST_TIMEOUT,
            ).raise_for_status()
        except requests.RequestException:
            continue


if __name__ == "__main__":
    app = create_app()
    name = app.config["WORKER_ID"]
    controller_url = os.getenv("CONTROLLER_URL", "http://controller:5000").rstrip("/")
    thread = threading.Thread(target=heartbeat_loop, args=(name, controller_url), daemon=True)
    thread.start()
    app.run(host="0.0.0.0", port=5000, threaded=True, use_reloader=False)
