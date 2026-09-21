# Architecture Plan: UI-Driven xVigilance Clock Rewind

This document outlines the architecture for allowing admin users to rewind the xVigilance processing clock directly from the frontend UI, completely eliminating the need for manual SSH restarts.

## Phase 1: Backend API Endpoint (Node-19)
Create a new REST endpoint: `POST /api/xvigilance/rewind`
- **Auth:** Bearer token + `users:manage` permission.
- **Payload:** `{ "target_date": "2026-08-24T00:00:00Z" }`
- **Logic:**
  1. `UPDATE xvigilance_checkpoints SET last_window_end = target_date`
  2. `DELETE FROM xvigilance_slice_runs WHERE window_end > target_date` (To clear the "future" audit log so the frontend sees the new runs at the top).
  3. `DELETE FROM anomalies WHERE detected_at > target_date` (Optional: to prevent duplicate anomalies).

## Phase 2: Producer Polling (Node-22)
Modify `Linkx_xmaintenance/src/linkx_xvigilance/runner.py`.
- **Current Behavior:** It fetches `last_window_end` once on boot and stores it in a Python variable.
- **New Behavior:** At the start of every while-loop iteration, fetch `last_window_end` from Postgres. 
- If `db_timestamp < memory_timestamp`, log `"Clock rewind detected!"` and overwrite the `memory_timestamp` with the `db_timestamp`.

## Phase 3: Consumer Idempotency (Node-21)
- The consumer is already idempotent (Neo4j `MERGE` statements handle duplicate anomalies safely). No changes needed here.

---

## 📋 Copy/Paste Prompt for the Frontend Agent

Copy the text below and paste it into a new session with your frontend AI agent:

> **Task: Build the xVigilance "Re-Analyze" (Rewind) UI**
>
> We are adding a feature that allows admins to rewind the xVigilance graph engine clock to re-process historical transactions under newly updated thresholds.
> 
> **Requirements:**
> 1. On the existing xVigilance Audit Log / Dashboard page, add a "Re-Analyze History" button (preferably near the top right, styled as a secondary or warning button).
> 2. Clicking the button opens a Modal with a Date/Time picker.
> 3. Add a warning text: *"Rewinding the clock will clear the audit log after the selected date and force the engine to re-process all transactions. This may cause high CPU usage."*
> 4. When submitted, make a `POST /api/xvigilance/rewind` request with the payload: `{ "target_date": "YYYY-MM-DDTHH:mm:ssZ" }`.
> 5. On success, show a toast notification and refresh the Audit Log table.
