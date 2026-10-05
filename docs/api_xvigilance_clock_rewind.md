# Production Architecture: UI-Driven xVigilance Clock Rewind (Scenario B: Fresh Reset)

## 1. Overview

The **xVigilance Clock Rewind** feature allows administrators to roll back the analysis window directly from the web interface. This is vital when AML risk thresholds are updated and historical transactions need to be re-evaluated under newly configured rules.

To ensure total integrity, the backend implements **Scenario B (1-Click Fresh Reset)**: rewinding the clock is not just a database timestamp edit—it performs a complete, atomic clean-slate reset across PostgreSQL, Kafka, and Neo4j.

---

## 2. API Specification

- **Endpoint:** `POST /api/v1/reports/xvigilance/rewind`
- **Authentication:** Bearer JWT with `users:manage` permission.
- **Request Payload:**
```json
{
  "target_date": "2026-08-24T09:00:00Z"
}
```

- **Successful Response (200 OK):**
```json
{
  "message": "Clock successfully rewound with full fresh reset.",
  "target_date": "2026-08-24T09:00:00Z",
  "neo4j_nodes_purged": 40000,
  "kafka_offset_reset": true
}
```

- **Error Response (400 / 500):**
```json
{
  "error": "Missing target_date"
}
```

---

## 3. Atomic Multi-Store Reset Pipeline

When `/xvigilance/rewind` is called, the backend executes four synchronized operations:

### Step 1: PostgreSQL Checkpoint Rollback & Audit Pruning
* Sets `xvigilance_checkpoints.last_window_end` to `target_date`.
* Deletes all future runs from `xvigilance_slice_runs WHERE window_end > target_date`.
* This ensures the Admin UI Audit Table instantly reflects the rewound starting point with no orphaned "future" runs.

### Step 2: Neo4j Ephemeral Graph Purge
* Purges any partially ingested or dirty in-flight nodes labeled `bank_transactions_xvigilance-daemon`:
  ```cypher
  MATCH (n:bank_transactions_xvigilance-daemon)
  WITH n LIMIT 50000
  DETACH DELETE n
  RETURN count(n) AS deleted_count
  ```
* Leaves all permanent investigation evidence and other system labels intact.

### Step 3: Kafka Consumer Offset Reset
* Connects to Kafka as the consumer group `xvigilance-graph-consumer-v2` for topic `dev.xvigilance.transactions.raw.v2`.
* Seeks all partition offsets to `latest` (end of stream).
* This flushes out any stale messages from future dates that were still buffered in Kafka, ensuring Node-21 begins immediately on the fresh historical stream.

---

## 4. Autonomous Worker Coordination

### Node-22 (Runner Daemon)
- Actively polls the PostgreSQL checkpoint on every loop iteration and during sleep cycles.
- If a rewind is detected while sleeping on time difference, it breaks sleep immediately:
  ```text
  [xvigilance] ⚠️ Clock rewind detected in database! Resetting internal clock to 2026-08-24 09:00:00+00:00
  [xvigilance] Phase starting for window [2026-08-24 09:00:00 UTC -> 2026-08-24 10:00:00 UTC]
  ```
- It begins pulling records from Elasticsearch starting from the new target date.

### Node-21 (Consumer Daemon)
- Continuously tracks the active window timestamp.
- If incoming Kafka messages arrive with timestamps significantly earlier than the currently held graph window (indicating an external rewind), the consumer automatically purges its local graph and resets its tracking state without requiring a daemon restart.

---

## 5. Frontend UI Integration Notes

1. **Button Placement:** Add a **"Re-Analyze History"** or **"Rewind Clock"** button on the xVigilance Monitoring Dashboard.
2. **Confirmation Modal:** Prompt the user with a date-time picker and clear warning:
   > *"Rewinding will reset the processing clock to the selected hour and wipe uncommitted in-flight graph state. All transactions from the selected date forward will be re-analyzed."*
3. **Post-Action:** On a `200 OK` response, trigger a toast notification (`"Engine rewound to [Date]. Reset [N] graph nodes."`) and refresh the Health table (`GET /api/v1/reports/xvigilance/health`).
