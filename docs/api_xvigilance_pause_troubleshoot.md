# Backend API: xVigilance Engine Pause & Troubleshoot

This document outlines the REST API endpoints used to pause the xVigilance daemon and view diagnostic health logs from the frontend UI.

## 1. Engine Health & Status
- **Endpoint:** `GET /api/v1/reports/xvigilance/health`
- **Auth:** Bearer Token.
- **Response:** Returns the current daemon state, including `is_paused` and recent execution logs.
```json
{
  "checkpoint": {
    "feed_name": "hourly_transaction_detective",
    "last_window_end": "Mon, 24 Aug 2026 00:00:00 GMT",
    "status": "running",
    "is_paused": false
  },
  "recent_runs": [
    {
      "run_id": 42,
      "status": "failed",
      "error_message": "ValueError: Neo4j syntax error...",
      "window_start": "Sun, 23 Aug 2026 23:00:00 GMT"
    }
  ]
}
```
*Frontend Note:* You can use the `error_message` from failed runs in the `recent_runs` array to build a "View Diagnostics" modal for troubleshooting.

## 2. Pause / Resume Engine
- **Endpoint:** `POST /api/v1/reports/xvigilance/state`
- **Auth:** Requires Bearer Token + `users:manage` permission.
- **Payload:**
```json
{
  "is_paused": true
}
```
- **Behavior:** Flips the engine state. If `true`, the background daemon will safely pause before the next hourly batch. If `false`, it instantly resumes.
