# Handoff: Global Entity Classification (Whitelist/Blacklist)

## Overview
The Linkx backend has been upgraded to synchronize manual investigation session configs (Trusted, Risk, PEP, Sanction entities) with the real-time background processing daemon (`xVigilance`). This ensures that genuine intermediary banks and trusted partners are automatically ignored by xVigilance's graph analysis, eliminating "Super Node" false positives.

## How the Architecture Works
1. **The Global Source of Truth:** A new PostgreSQL table named `global_entity_classification` holds the master JSONB arrays for `trusted_entities`, `risk_entities`, `pep_entities`, and `sanction_entities`.
2. **Session Initialization (Read):** When any user (Admin or Analyst) initiates a new manual analysis session (`POST /init`), the API pulls the latest master list from the database and injects it directly into their `session_config`.
3. **Session Updates (Write/RBAC):** 
   - If an **Admin** edits these entity lists in their session (via `POST /configuration`), the API applies the changes to their session AND actively pushes the new arrays back to the `global_entity_classification` table.
   - If a **Standard User** (Analyst) attempts to edit these arrays, the backend securely intercepts the request and blocks the change (`403 Forbidden` / `users:manage` required).
4. **xVigilance Daemon (Execution):** The background stream processor now queries this Postgres table before every batch. It dynamically translates the `trusted_entities` into Cypher `NOT any(entry IN $trusted_entries)` clauses and injects them directly into the 7 core anomaly SQL statements (Smurfing, Circular Flow, Hub & Spoke, etc.) preventing false-positives at the database level.

## The Goal for the Frontend Team
The API architecture and database logic is already 100% complete and deployed to both the backend API (`linkx-api`) and the worker (`xvigilance_consumer`).

To fully utilize this feature, the frontend dashboard must:
1. Provide an interface in the manual session workspace for **Admins** to visually Add/Remove identifiers to the Trusted, Risk, PEP, and Sanction lists. 
2. When the admin clicks "Save", send these arrays within the standard `POST /configuration` API call.
3. Ensure the UI disables or hides the "Edit" buttons for these lists if the active user does not have Admin privileges.

## Example Configuration JSON Schema
When saving the session configuration, the `trusted_entities` array should be structured as a list of dictionaries precisely matching the graph node properties (e.g., `ACCOUNTNO`):

```json
{
  "session_id": "...",
  "trusted_entities": [
    {
      "ACCOUNTNO": "BANK_12345"
    },
    {
      "ACCOUNTNO": "PARTNER_98765"
    }
  ],
  "risk_entities": [
    {
      "BUSINESSMOBILENO": "+251911000000"
    }
  ],
  "pep_entities": [],
  "sanction_entities": []
}
```

By completing the frontend UI for this feature, the Risk Team can continuously curate their whitelists manually, and xVigilance will instantly and silently adapt to those rules for all automated anomaly detection.
