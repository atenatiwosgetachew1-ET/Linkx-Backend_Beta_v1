# Handoff: Dynamic Risk Scoring Configuration UI

## Overview
The xVigilance fraud engine currently uses hardcoded logic to calculate a `fraud_score` (0-100) and a `score_band` (Low, Medium, High, Critical) for every detected anomaly. To allow non-technical risk analysts to adjust sensitivity on the fly, we are moving this configuration to the database.

## The Goal for the Frontend Team
We need an **Admin Settings UI** that allows the risk team to update a single JSON configuration object in the `risk_scoring_config` PostgreSQL table.

## The Configuration JSON Schema
The frontend form should generate and save a JSON object that looks exactly like this:

```json
{
  "base_scores": {
    "HIGH_RISK_LINK": 50,
    "CIRCULAR_FLOW": 30,
    "SMURFING": 20,
    "SHARED_IDENTIFIER": 20,
    "HUB_AND_SPOKE": 10,
    "RAPID_FAN_OUT": 10,
    "ABNORMAL_BALANCE_CHANGE": 10
  },
  "node_thresholds": [
    { "min_nodes": 10000, "add_points": 30 },
    { "min_nodes": 5000, "add_points": 20 },
    { "min_nodes": 1000, "add_points": 10 },
    { "min_nodes": 100, "add_points": 5 }
  ],
  "money_thresholds": [
    { "min_amount": 10000000, "add_points": 40 },
    { "min_amount": 5000000, "add_points": 30 },
    { "min_amount": 1000000, "add_points": 20 },
    { "min_amount": 500000, "add_points": 10 }
  ],
  "bands": {
    "critical_min": 80,
    "high_min": 50,
    "medium_min": 20
  }
}
```

## Backend Consumer Integration
Once this table exists and is populated via your UI, the Python `xvigilance_consumer` will:
1. Query the database at the start of every time window.
2. Inject the dynamic settings into the math engine.
3. Apply the updated scoring rules **instantly** to the very next batch, without requiring any restarts or downtime.
