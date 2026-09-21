# Frontend Handoff: Configurable Rule Thresholds & Platform Stability

This document outlines the backend capabilities that have been deployed to the LinkX production environment and provides a prompt to hand off to the frontend development team to build the corresponding UI.

## Backend Achievements (Live on Production)
1. **Dynamic Risk Engine:** The xVigilance graph engine now dynamically pulls rule thresholds (like SMURFING limits) from PostgreSQL instead of using hardcoded values.
2. **REST API Endpoints:** Built and deployed `/rule-thresholds` endpoints for reading, updating, and auditing these values.
3. **EFFECTIVE_FLOW Bypass:** The graph engine now natively bypasses intermediate pass-through accounts to link perpetrators directly to central hubs.
4. **Zero-Downtime Concurrency:** Migrated the entire API from legacy Eventlet to a multi-threaded Gunicorn architecture, permanently eliminating Nginx 504 Gateway Timeouts and database deadlocks.

---

## API Specification for Frontend

The frontend team needs to build an interface (similar to the Classified Entities page) to manage these thresholds. 

### 1. Fetch Current Thresholds
- **Endpoint:** `GET /rule-thresholds`
- **Auth:** Requires Bearer Token
- **Response (200 OK):**
```json
{
  "smurfing_single_tx_threshold": 300000,
  "smurfing_min_tx_count": 3,
  "smurfing_cumulative_threshold": 900000,
  "reporting_threshold": 300000,
  "circular_flow_check_amounts": false,
  "late_night_start": 2300,
  "late_night_end": 400,
  "hub_spoke_min_counterparties": 3,
  "activity_spike_multiplier": 3,
  "activity_spike_min_daily_count": 10,
  "rapid_withdrawal_amount_tolerance": 0.1,
  "updated_at": "2026-09-21T07:00:00Z",
  "updated_by": "system"
}
```

### 2. Update Thresholds
- **Endpoint:** `POST /rule-thresholds`
- **Auth:** Requires Bearer Token + `users:manage` permission
- **Payload:** Same dictionary structure as the GET request.
- **Validation Guardrails (Backend Enforced):**
  - `late_night_start` / `late_night_end` must be valid military time integers (0-2359).
  - All numerical thresholds must be strictly positive (> 0).
  - `rapid_withdrawal_amount_tolerance` must be between 0.0 and 1.0.

### 3. Fetch Audit History (Optional / Future)
- **Endpoint:** `GET /rule-thresholds/history`
- **Auth:** Requires Bearer Token + `users:manage` permission
- **Response (200 OK):**
```json
[
  {
    "id": 2,
    "config": { ... },
    "updated_by": "admin",
    "created_at": "2026-09-21T08:15:00Z"
  },
  ...
]
```

---

## 📋 Copy/Paste Prompt for the Frontend Agent

Copy the text below and paste it into a new session with your frontend AI agent:

> **Task: Build the Global Rule Thresholds Management UI**
>
> We have implemented dynamic, configurable rule thresholds on the backend for our graph-based risk engine (e.g., NBE CTR limits, smurfing parameters). I need you to build the frontend interface for this.
> 
> **Requirements:**
> 1. Create a new Admin-only settings page (or add a tab next to Classified Entities) for "Rule Thresholds".
> 2. On mount, `GET /rule-thresholds` and populate a form.
> 3. The form should group related fields (e.g., a "Smurfing" section, a "Time Analysis" section, a "Graph Topology" section).
> 4. Ensure client-side validation (e.g., late night hours must be 0-2359, tolerances must be decimals between 0 and 1).
> 5. On submit, send the modified JSON to `POST /rule-thresholds`. This endpoint requires the `users:manage` permission.
> 6. Add a "View History" drawer or modal that hits `GET /rule-thresholds/history` to show an audit log of who changed which thresholds and when.
> 
> Please generate the React components, service API hooks, and update the router to integrate this page.
