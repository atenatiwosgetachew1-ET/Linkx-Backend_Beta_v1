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
