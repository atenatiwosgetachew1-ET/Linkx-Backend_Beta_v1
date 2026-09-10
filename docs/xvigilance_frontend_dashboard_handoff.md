# Handoff: xVigilance Frontend Health Dashboard

## Overview
The xVigilance pipeline exposes a real-time health and auditing endpoint designed specifically for the Admin Frontend. It allows the UI to monitor the progress of massive historical backfills (like the 1.4B transaction run) without impacting the Kafka or Neo4j ingestion speeds.

## Endpoint Details
**URL:** `GET /api/v1/reports/xvigilance/health`
**Query Parameters (Optional):** `?limit=50` (Controls how many historical runs to fetch).

## Response Schema
The endpoint queries PostgreSQL (`xvigilance_checkpoints` and `xvigilance_slice_runs`) and returns:
```json
{
  "success": true,
  "checkpoint": {
    "feed_name": "hourly_transaction_detective",
    "last_window_end": "Mon, 01 Sep 2025 00:00:00 GMT", 
    "total_records_analyzed": 14500000,
    "status": "active"
  },
  "recent_runs": [
    {
      "run_id": 1492,
      "window_start": "Sun, 31 Aug 2025 23:00:00 GMT",
      "window_end": "Mon, 01 Sep 2025 00:00:00 GMT",
      "status": "SUCCESS",
      "records_count": 150000,
      "duration_ms": 3500.5,
      "error_message": null,
      "finished_at": "Wed, 09 Sep 2026 10:45:00 GMT"
    }
  ]
}
```

## Performance & Safety
This endpoint is **100% physically decoupled** from the ingestion engine. 
- The Python Ingestion Worker writes to Neo4j and Kafka.
- This endpoint reads from indexed PostgreSQL tables.
- Frontend users can query this dashboard 1,000 times a second and it will have **zero impact** on the ingestion speed of the 1.4 Billion transaction backfill.

## Reports Endpoint (Anomalies & Scoring)
**URL:** `GET /api/v1/reports/list?report_type=xvigilance`

When the Engine detects fraud, the alerts are instantly pushed here. We have recently upgraded the payload to explicitly include pre-calculated **Fraud Scores** and a **Top 5 Accounts** list so the Frontend does not need to parse or compute anything.

### Example Response Payload
```json
{
  "anomaly_type": "HUB_AND_SPOKE",
  "entity_id": "6387",
  "execution_meta": {
    "batch_id": 6886,
    "elastic_endpoint": "mobile_banking_transactions",
    "total_records": 112015,
    "worker_node": "Linkx_xmaintenance"
  },
  "reason": "account connects with multiple counterparties on same day",
  "reported_to": "Risk Scoring Service",
  "trace_id": "35bbb246-65c6-486c-8ac0-7910d8d2b524",
  
  // NEW FIELDS FOR FRONTEND RENDERING:
  "fraud_score": 88,
  "score_band": "Critical",
  "top_5_accounts": [
    {"account": "1000284759", "volume": 54000.00},
    {"account": "1000984712", "volume": 12000.00}
  ]
}
```
**Score Bands:** Low (0–19), Medium (20–49), High (50–79), Critical (80+).

## Graph Subgraph Extraction (For UI Rendering)
When an anomaly contains tens of thousands of nodes (e.g., an 89,000 node Hub-and-Spoke ring), sending the entire graph to the frontend would crash the browser and hit PostgreSQL limits.

The backend now uses an **Edge-Centric Subgraph Extraction** algorithm before saving the report:
1. It analyzes the "degree of influence" of every node in the massive anomaly.
2. It sorts all edges by the combined influence of the two nodes they connect.
3. It takes a strict limit of the **top 1,000 most influential edges**.
4. It extracts exactly the nodes required to render those 1,000 edges.

**What this means for the Frontend:**
- You will receive a `graph: { nodes: [...], edges: [...] }` payload that is capped at 1,000 edges and roughly ~2,000 nodes maximum.
- **100% Intact Guarantee:** There will never be an "orphan node" (a node with no edges) or a "dangling edge" (an edge pointing to a node that isn't in the payload). The subgraph is mathematically intact and ready to render in libraries like Cytoscape.js or Vis.js perfectly out-of-the-box.
- The `records_count` and `fraud_score` math are still accurately calculated against the *full* 89,000 node dataset before this truncation happens.
