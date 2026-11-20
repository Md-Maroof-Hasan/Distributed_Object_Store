# Mini Distributed Cloud Object Store (Flask + Docker)

A **local distributed object storage service** implementing the Cloud Computing —
Infrastructure Project brief. A Flask controller stores object metadata and
splits arbitrary uploaded binary files into 1 MiB chunks. Four independent
Flask workers persist the chunks on Docker named volumes. Each chunk has two
replicas on separate workers. Failed or corrupted replicas are detected and
repaired automatically where a verified healthy copy is available.

## Architecture

```text
                            REST API (localhost:5000)
                                    |
                                 CLIENT
                                    |
                             +-------------+
                             |  CONTROLLER |
                             |  Flask API  |
                             | SQLite DB   |
                             | monitor     |
                             +-------------+
                                    |
          +-------------------------+-------------------------+
          |               |                    |              |
      +--------+      +--------+           +--------+     +--------+
      |worker1 |      |worker2 |           |worker3 |     |worker4 |
      |volume 1|      |volume 2|           |volume 3|     |volume 4|
      +--------+      +--------+           +--------+     +--------+
```

Configuration: **1 controller; 4 workers; 1 or more clients; 1 MiB chunks;
replication factor 2; persistent volumes; Docker Compose, not Kubernetes.**

## Requirements

- Docker Desktop (Windows/Mac) or Docker Engine + Compose plugin (Linux)
- Free ports: `127.0.0.1:5000`
- Python 3.10+ on your laptop for the optional demo/test scripts
- On Windows, use PowerShell, Git Bash, or a WSL terminal

## 1. Launch the entire system

From the extracted folder:

```bash
docker compose up --build -d
# Give workers a few seconds to register
docker compose ps
docker compose logs --tail=30 controller
curl http://127.0.0.1:5000/internal/workers
```

All four workers should show `"online": true` in `/internal/workers`.
Controller API is host-accessible on port 5000. Worker APIs are reachable
only inside the Compose network (or via `docker compose exec`).

If you use Linux without Docker Desktop, `docker compose` requires the
Compose v2 plugin. For Windows PowerShell, `curl` might alias to a different
command; use `curl.exe` for the examples below.

**Do not use `docker compose down -v` unless you want to erase data.**
Normal `docker compose stop`, `start`, `restart`, and `down` preserve volumes.

## 2. Run ALL mandatory demonstrations automatically

Install the small client dependency on your laptop:

```bash
python -m pip install requests
python scripts/demo.py
```

The script checks:

1. **Object storage:** uploads random binary data and downloads exactly the same bytes.
2. **Chunking:** object larger than 2 MiB yields three ordered 1 MiB-or-smaller chunks.
3. **Replication:** exactly two healthy copies on distinct workers (and shows locations).
4. **Metadata:** uploads custom key-value metadata and patches it without reuploading.
5. **Worker failure:** stops `worker2`, waits for heartbeat expiry, verifies repair and
   download, restarts it, and checks replicas again.
6. **Integrity:** deliberately overwrites one worker's chunk and verifies download
   from a healthy replica, corruption handling, and restored replication.

It also exercises listing and deletion. The script creates and deletes a
**temporary test object** and restarts worker2 in a `finally` block if needed.

## 3. Manual client API examples

All structured bodies/responses are JSON; uploads use multipart/form-data;
downloads/chunks use binary `application/octet-stream` (the object download
returns the stored content type).

```bash
# Create a small example file
printf 'hello distributed storage\n' > example.txt

# Upload binary + optional JSON custom metadata; expect HTTP 201
curl -sS -X POST http://127.0.0.1:5000/api/v1/objects \
  -F 'file=@example.txt' -F 'metadata={"owner":"student","course":"cloud"}'
```

Take the returned `object_id` and replace `OBJECT_ID` in the examples below:

```bash
# List all objects, expect 200
curl -sS http://127.0.0.1:5000/api/v1/objects

# Get object system metadata and chunk placement, expect 200
curl -sS http://127.0.0.1:5000/api/v1/objects/OBJECT_ID/metadata

# Update *only custom metadata* (no object reupload), expect 200
curl -sS -X PATCH http://127.0.0.1:5000/api/v1/objects/OBJECT_ID/metadata \
  -H 'Content-Type: application/json' \
  -d '{"reviewed":"yes"}'

# Download original raw bytes, expect 200
curl -sS -o restored.txt \
  http://127.0.0.1:5000/api/v1/objects/OBJECT_ID

# Compare hashes
sha256sum example.txt restored.txt

# Delete object, expect 204
curl -i -X DELETE http://127.0.0.1:5000/api/v1/objects/OBJECT_ID
```

### Client routes (exactly the assignment interface)

| Method | Endpoint | Success |
|---|---|---|
| `POST` | `/api/v1/objects` | `201` |
| `GET` | `/api/v1/objects/{object_id}` | `200` |
| `GET` | `/api/v1/objects/{object_id}/metadata` | `200` |
| `PATCH` | `/api/v1/objects/{object_id}/metadata` | `200` |
| `DELETE` | `/api/v1/objects/{object_id}` | `204` |
| `GET` | `/api/v1/objects` | `200` |

Error responses are JSON with an `error` key. Most common: 400 bad request,
404 not found, 503 insufficient workers or unavailable valid chunk data.

### Internal routes

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/internal/workers/register` | Worker registration |
| `POST` | `/internal/workers/heartbeat` | Report availability |
| `PUT` | `/internal/chunks/{chunk_id}` | Store binary chunk on a worker |
| `GET` | `/internal/chunks/{chunk_id}` | Read binary chunk from a worker |
| `DELETE` | `/internal/chunks/{chunk_id}` | Remove chunk from a worker |
| `GET` | `/internal/workers/{worker_id}/chunks` | Worker chunk inventory |
| `GET` | `/internal/workers` | Inspect online workers (extra) |
| `POST` | `/internal/admin/reconcile` | Trigger repair immediately (extra) |

## 4. Detailed failure experiment

```bash
# Upload an object larger than 1 MiB first and save its object_id.
docker compose stop worker2

# Wait roughly 8-12 s for missed heartbeats to mark worker2 offline.
curl http://127.0.0.1:5000/internal/workers

# Repair missing replicas now (automatic background task also runs every 4 s).
curl -X POST http://127.0.0.1:5000/internal/admin/reconcile

# Get object metadata: each chunk should have healthy_replica_count = 2.
curl http://127.0.0.1:5000/api/v1/objects/OBJECT_ID/metadata

# Retrieve the original object, even with worker2 stopped.
curl -o recovered.bin http://127.0.0.1:5000/api/v1/objects/OBJECT_ID

docker compose start worker2
# After worker2 re-registers, the controller trims extra live copies.
curl -X POST http://127.0.0.1:5000/internal/admin/reconcile
```

When worker2 is offline, its *durable volume* is unchanged. The controller
tracks replicas on offline workers but **counts only live copies** towards the
target of two. On return, excess live copies are trimmed.

## 5. Detailed corruption experiment

1. Upload an object, then GET `/api/v1/objects/OBJECT_ID/metadata`.
2. Choose a `chunk_id` and one of its `replicas[].worker_id` values.
3. Deliberately corrupt that worker's persistent file, for example:

   ```bash
   docker compose exec worker1 python -c "from pathlib import Path; Path('/data/chunks/CHUNK_ID.bin').write_bytes(b'CORRUPTED')"
   ```

   Replace `worker1` and `CHUNK_ID` with actual values from metadata.
4. GET `/api/v1/objects/OBJECT_ID` again. It must return **only verified data**;
   if no valid replica exists it responds `503` rather than returning bad bytes.
5. POST `/internal/admin/reconcile` and inspect metadata for two healthy copies.

A worker's inventory hashes **the actual on-disk bytes**, not a cached checksum.
The controller cross-checks the digest against SQLite and rewrites bad copies
from verified good ones. Downloads also independently hash each retrieved chunk
and check the whole-object digest before returning success.

## 6. Automated tests

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

Tests include large binary round-trip, three-chunk placement, object metadata,
restart recovery, corruption, no-valid-copy failure, remote deletion retries,
worker persistent storage, and all Flask client routes (using a fake HTTP network).

## 7. File layout

```text
common/config.py          configuration (1 MiB, replication=2, timeouts, worker URLs)
controller/main.py        Flask client/internal REST APIs; maintenance thread
controller/service.py     chunking, placement, repair, corruption handling, metadata
controller/database.py    SQLite schema + transaction helper
worker/main.py            worker chunk REST APIs + registration and heartbeat
worker/storage.py         atomic persistent chunk file read/write/delete + hashes
scripts/demo.py           executable all-requirements demonstration
tests/                   isolated logic tests + Flask API test
controller/Dockerfile     container image for controller
worker/Dockerfile         container image for workers
docker-compose.yml        1 controller + 4 worker services and persistent volumes
```

## Important limitations / design choices

- This is a **course/demo** design, not production storage. No authentication,
  TLS, quotas, object versioning, encryption, rate limiting, or multiple controllers.
- The Flask built-in server is suitable for local demonstration only; use a
  production WSGI server if deploying beyond localhost. Docker publishes the
  controller only on `127.0.0.1` by design.
- The controller is a **single point of failure** as allowed by the assignment.
  Its SQLite database is persistent but is not itself replicated.
- Worker's on-disk data is persistent using volumes. Controller metadata is
  persistent too. Restarts preserve data unless Docker volumes are removed.
- Downloads stage verified chunks into a spooled temporary file before sending
  HTTP 200; for huge files the temporary file spills to disk. This favors
  integrity over latency and may require enough temporary disk space.
- Metadata is committed only after the required chunk replicas are stored.
  A controller crash during upload can leave orphan chunk files; automatic
  garbage collection of orphan uploads is not included.
- Delete requests queue pending remote deletes for offline nodes. They are
  retried by the maintenance loop when the node returns.
- Reconciliation calculates on-disk hashes for all worker chunks every cycle.
  This is deliberately simple and reliable for demonstrations, but would be
  expensive at enterprise scale.
- The internal admin endpoint has no authentication: keep this deployment local.
- Heartbeats/requests use simple failure timeouts; brief failures can cause
  temporary under-replication. The controller can repair only when a valid
  source replica and a suitable destination worker are accessible.

## Stop / reset

```bash
docker compose down         # Stops containers; preserves all named volumes
docker compose up -d        # Recreates containers and reuses persisted data
# WARNING: erase all objects and data only if you intentionally want a reset:
docker compose down -v
```
