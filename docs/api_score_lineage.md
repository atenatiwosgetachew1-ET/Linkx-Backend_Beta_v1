# Backend API: Score Lineage Configuration

This document outlines the REST API endpoints used to fetch and update the global risk scoring parameters (base scores, node thresholds, and money thresholds). This configuration is version-controlled and immutable (append-only) to maintain a strict audit trail of risk policy changes.

## 1. Fetch Current Score Lineage
- **Endpoint:** `GET /api/v1/reports/config/score-lineage` *(Note: Assuming standard API mounting prefix. Check routing if using `/config/score-lineage` at root level).*
- **Actually Mapped To:** `GET /config/score-lineage` (Root level in `main.py`)
- **Auth:** Requires Bearer Token + `config:read` permission.
- **Response (200 OK):** Returns the most recently active configuration JSON.
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
    { "min_nodes": 5000, "add_points": 20 }
  ],
  "money_thresholds": [
    { "min_amount": 10000000, "add_points": 40 },
    { "min_amount": 5000000, "add_points": 30 }
  ]
}
```

## 2. Update Score Lineage (Append New Version)
- **Endpoint:** `POST /config/score-lineage`
- **Auth:** Requires Bearer Token + `users:manage` permission.
- **Payload:** Must contain `base_scores`, `node_thresholds`, and `money_thresholds` objects exactly matching the structure of the GET request.
- **Behavior:** This does not overwrite the existing config. Instead, it inserts a new row into `global_score_lineage`, which becomes the new active configuration used by the risk engine.
- **Response (200 OK):**
```json
{
  "message": "success",
  "version_id": 2
}
```
