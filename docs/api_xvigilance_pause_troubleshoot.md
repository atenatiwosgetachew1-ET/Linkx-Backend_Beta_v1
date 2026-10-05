# Production Guide: xVigilance Engine Pause, Resume & Operational Runbook

## 1. Overview & Architecture

The **xVigilance** autonomous detective engine operates across a distributed 4-node cluster:
- **Node-19 (`172.27.23.95`)**: LinkX API Gateway (`linkx-api.service`). Exposes management and health endpoints.
- **Node-20 (`172.27.23.106`)**: Control Plane. Hosts PostgreSQL database and Apache Kafka broker.
- **Node-21 (`172.27.23.18`)**: Streaming Consumer Worker (`linkx-xvigilance-consumer.service`). Ingests micro-batches into Neo4j and triggers batch AML graph analytics at watermarks.
- **Node-22 (`172.27.23.85`)**: Extraction Runner (`linkx-xvigilance.service`) and Neo4j graph database.

---

## 2. Pause & Resume Mechanics

### REST API Endpoint
- **URL:** `POST /api/v1/reports/xvigilance/state`
- **Headers:** `Authorization: Bearer <token>` (Requires `users:manage` permission)
- **Payload:**
```json
{
  "is_paused": true
}
```
- **Response:**
```json
{
  "message": "State updated",
  "is_paused": true
}
```

### Distributed Coordination Protocol

When `is_paused` is updated in PostgreSQL (`xvigilance_checkpoints.is_paused`), both worker nodes coordinate cleanly without race conditions:

#### Node-21 (Consumer Daemon) — Micro-Batch Granularity (<6s response)
- Checks the `is_paused` flag before every 10,000-record micro-batch.
- Maintains a dedicated, persistent PostgreSQL connection to eliminate connection exhaustion or timeout issues.
- If `is_paused == true`, it halts ingestion immediately after the current micro-batch and enters a safe sleep state:
  ```text
  [xVigilance-Consumer] ⏸️ Engine paused by Admin in UI (micro-batch 5). Resting...
  ```
- No Kafka offsets are committed for unconsumed messages. Messages remain safely buffered in Kafka.

#### Node-22 (Runner Daemon) — Window-Atomic Granularity (Graceful Quiescence)
- In enterprise banking architectures (Central Bank of Ethiopia, SWIFT, ACH), 1-hour transaction ledger slices are **atomic audit units**.
- **Graceful Window Completion:** If Node-22 is actively streaming an in-flight hour window when Pause is clicked, it completes delivery of that single hour (e.g. 255,374 records), emits the `WINDOW_COMPLETE` Watermark, and safely updates the checkpoint.
- **Immediate Pause at Boundary:** Before opening the *next* hour window, Node-22 detects `is_paused == true` and sleeps:
  ```text
  [xvigilance] ⏸️ Daemon is paused by Admin. Sleeping...
  ```
- **Why this is critical for Banking Compliance:** Aborting mid-hour would leave half-sent transactions in Kafka. When resumed, either the entire hour would have to be re-sent (causing duplicate transactions and corrupted graph risk scores) or complex offset resumption would risk dropped transactions. Graceful quiescence guarantees **zero duplicates, zero dropped transactions, and 100% ledger reconciliation**.

---

## 3. Engine Health & Status API

- **Endpoint:** `GET /api/v1/reports/xvigilance/health`
- **Response:**
```json
{
  "checkpoint": {
    "feed_name": "hourly_transaction_detective",
    "last_window_end": "Mon, 24 Aug 2026 10:00:00 GMT",
    "status": "running",
    "is_paused": true
  },
  "recent_runs": [
    {
      "run_id": 42,
      "status": "SUCCESS",
      "records_count": 255374,
      "duration_ms": 210700.0,
      "window_start": "Mon, 24 Aug 2026 09:00:00 GMT",
      "window_end": "Mon, 24 Aug 2026 10:00:00 GMT",
      "finished_at": "Mon, 05 Oct 2026 09:33:43 GMT"
    }
  ]
}
```

---

## 4. Operational Runbook & Troubleshooting

### Checking Daemon Status
```bash
# Node-21 (Consumer)
sudo systemctl status linkx-xvigilance-consumer
sudo journalctl -u linkx-xvigilance-consumer -n 50 --no-pager

# Node-22 (Runner)
sudo systemctl status linkx-xvigilance
sudo journalctl -u linkx-xvigilance -n 50 --no-pager
```

### Dual-Layer Mutex Verification (Node-22)
The runner enforces strict single-instance execution via a dual-layer lock:
1. **OS Kernel Lock:** `fcntl.flock` on `/tmp/linkx_xvigilance_runner.lock`.
2. **PostgreSQL Session Lock:** `pg_try_advisory_lock(97130282477900)`.

To verify no duplicate processes are lingering:
```bash
# Verify process count (must be exactly 1)
pgrep -fl runner.py

# Check PostgreSQL advisory lock on Node-20
sudo -u postgres psql -d linkx_db -c "SELECT pid, locktype, objid, granted FROM pg_locks WHERE locktype = 'advisory' AND objid = 97130282477900;"
```

### Checking Kafka Consumer Lag (Node-20)
```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server 172.27.23.106:9092 \
  --group xvigilance-graph-consumer-v2 \
  --describe
```
- When actively running, `LAG` will hover around 10,000–50,000 as micro-batches process.
- When paused, `LAG` will remain stable.
- Once Node-21 resumes and processes all records up to the watermark, `LAG` drops to **0**.

### Checking Node-22 Graph Database & Memory Health
```bash
# Check physical RAM and emergency swap usage
free -h
swapon --show

# Check Neo4j Docker container memory usage
sudo docker stats --no-stream linkx-neo4j

# View recent Neo4j logs
sudo docker logs --tail 25 linkx-neo4j

# Inspect Neo4j memory configuration (/opt/linkx-neo4j/docker-compose.yml)
grep -iE "pagecache|heap" /opt/linkx-neo4j/docker-compose.yml
```

