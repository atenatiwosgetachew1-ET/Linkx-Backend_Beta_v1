# Frontend Handoff: xVigilance Monitoring, Control & Anomaly Dashboard

## 1. Overview
The **xVigilance** autonomous detective engine monitors national-scale banking transactions for money laundering patterns (smurfing, circular loops, mule networks). 

This handoff outlines all REST APIs exposed to the Admin Frontend for monitoring progress, toggling execution state, triggering historical rewinds, and rendering detected graph anomalies.

---

## 2. API Endpoints Reference

### 2.1 Engine Health & Audit Log
- **URL:** `GET /api/v1/reports/xvigilance/health`
- **Query Parameters (Optional):** `?limit=50` (Controls number of recent slice runs to return; default: 50).
- **Authentication:** Bearer JWT.

#### Response Schema:
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
      "run_id": 1492,
      "window_start": "Mon, 24 Aug 2026 09:00:00 GMT",
      "window_end": "Mon, 24 Aug 2026 10:00:00 GMT",
      "status": "SUCCESS",
      "records_count": 255374,
      "duration_ms": 210700.0,
      "error_message": null,
      "finished_at": "Mon, 05 Oct 2026 09:33:43 GMT"
    }
  ]
}
```

#### UI Implementation Guidelines:
- **Status Indicator:**
  - If `checkpoint.is_paused == true`: Display **"PAUSED"** badge (Yellow/Orange).
  - If `checkpoint.is_paused == false`: Display **"RUNNING"** badge (Pulsing Green).
- **Recent Runs Table:** Display `window_start` -> `window_end`, `records_count` (formatted with commas), `duration_ms` (in seconds), and `status` (`SUCCESS` in green, `FAILED` in red with error tooltip).

---

### 2.2 Pause / Resume Engine State
- **URL:** `POST /api/v1/reports/xvigilance/state`
- **Authentication:** Bearer JWT with `users:manage` permission.
- **Request Payload:**
```json
{
  "is_paused": true
}
```
- **Response Schema:**
```json
{
  "message": "State updated",
  "is_paused": true
}
```

#### Operational Behavior:
- **Instant Consumer Halt:** The streaming consumer (Node-21) halts between 10k-record micro-batches (<6 seconds).
- **Window-Atomic Extraction:** The extraction runner (Node-22) gracefully finishes the active 1-hour window (e.g. `09:00 -> 10:00 UTC`), fires its watermark, updates the audit log, and stops before opening the next hour. This preserves ledger atomicity and prevents duplicate transactions.
- **Resuming:** Setting `is_paused: false` immediately wakes both consumer and runner daemons.

---

### 2.3 Historical Clock Rewind (Scenario B: Fresh Reset)
- **URL:** `POST /api/v1/reports/xvigilance/rewind`
- **Authentication:** Bearer JWT with `users:manage` permission.
- **Request Payload:**
```json
{
  "target_date": "2026-08-24T09:00:00Z"
}
```
- **Response Schema:**
```json
{
  "message": "Clock successfully rewound with full fresh reset.",
  "target_date": "2026-08-24T09:00:00Z",
  "neo4j_nodes_purged": 40000,
  "kafka_offset_reset": true
}
```

#### UI Implementation Guidelines:
- Add a **"Re-Analyze History"** button near the top right of the xVigilance dashboard.
- Clicking opens a Modal with a Date/Time picker.
- Include a descriptive confirmation prompt:
  > *"Rewinding will reset the processing clock to the selected hour and wipe uncommitted in-flight graph state. All transactions from this date forward will be re-analyzed."*
- On success (`200 OK`), display a toast notification with `neo4j_nodes_purged` and refresh the Health API table.

---

### 2.4 Anomaly Reports & Graph Payloads
- **URL:** `GET /api/v1/reports/list?report_type=xvigilance`
- **Authentication:** Bearer JWT.

#### Example Response Item:
```json
{
  "anomaly_type": "HUB_AND_SPOKE",
  "entity_id": "6387",
  "execution_meta": {
    "batch_id": 6886,
    "elastic_endpoint": "mobile_banking_transactions",
    "total_records": 255374,
    "worker_node": "Linkx_xmaintenance"
  },
  "reason": "account connects with multiple counterparties in short timeframe",
  "reported_to": "Risk Scoring Service",
  "trace_id": "35bbb246-65c6-486c-8ac0-7910d8d2b524",
  "fraud_score": 88,
  "score_band": "Critical",
  "top_5_accounts": [
    {"account": "1000284759", "volume": 54000.00},
    {"account": "1000984712", "volume": 12000.00}
  ],
  "graph": {
    "nodes": [
      {"id": "1000284759", "label": "Account", "degree": 45}
    ],
    "edges": [
      {"source": "1000284759", "target": "1000984712", "amount": 12000.00, "txn_date": "2026-08-24"}
    ]
  }
}
```

#### Score Bands:
- `Low`: 0–19 (Green)
- `Medium`: 20–49 (Yellow)
- `High`: 50–79 (Orange)
- `Critical`: 80+ (Red)

#### Graph Subgraph Rendering:
- The backend automatically applies an **Edge-Centric Influence Filter** to anomalies with massive node counts (e.g. 50,000+ nodes), capping the `graph` payload to the top **1,000 most influential edges** (~2,000 nodes).
- **Guaranteed Graph Integrity:** There are zero orphan nodes and zero dangling edges. Payloads render directly in Cytoscape.js or Vis.js without browser memory exhaustion.
