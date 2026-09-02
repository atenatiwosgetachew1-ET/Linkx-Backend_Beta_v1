# Reporting Pipeline Implementation Plan

This plan expands on the existing architecture (as outlined in `docs/report_integration_plan.md`) to create a highly dynamic, secure, and performant centralized reporting subsystem. It addresses your 3 specific requirements: Parent Reports, XVigilance Findings, and Sibling Service (Risk Scoring) Evidence.

## Architecture Principles
1. **Security**: All inbound reports must be authenticated (Parent SSO / Service Tokens). All human interactions must log to `security_audit_events`.
2. **Optimal Performance**: Ingestion must be asynchronous. We will use the **Transactional Outbox Pattern** to ensure we never drop reports even if the parent system goes down.
3. **Maintainability**: A single unified `linkx_reports` table with a flexible JSONB payload will handle all 3 report types natively, preventing database schema sprawl.

---

## Phase 1: Core Database & Domain Modeling
We need a unified storage layer that can handle the dynamic nature of the three different report sources.

- [ ] **1.1 Create `linkx_reports` Table Migration**
  - `id` (UUID, Primary Key)
  - `report_type` (Enum: `PARENT_RECEIVED`, `XVIGILANCE_FINDING`, `SERVICE_EVIDENCE`)
  - `status` (Enum: `NEW`, `INVESTIGATING`, `RESOLVED`, `SYNCED`)
  - `source_system` (e.g., `'parent_ctms'`, `'linkx_xvigilance'`, `'risk_scoring'`)
  - `external_reference_id` (Indexed. Used to map back to Parent IDs or Risk Scoring Trace IDs)
  - `payload` (JSONB) - Stores the dynamic raw data/context.
  - `created_at`, `updated_at` (TIMESTAMPTZ)
- [ ] **1.2 Create `linkx_report_evidence` Table Migration**
  - Links reports to actual graph snapshots and artifact IDs stored in LinkX.
- [ ] **1.3 Create `report_sync_outbox` Table Migration**
  - `id`, `target_system`, `payload`, `status` (`pending`, `failed`, `success`).
  - Used for guaranteed delivery of outgoing reports/updates.

## Phase 2: Ingestion & Service Integration (The 3 Pillars)
We will wire up the three specific sources you requested into the new core tables.

- [ ] **2.1 Parent Report Ingestion (`PARENT_RECEIVED`)**
  - Create secure API endpoint: `POST /api/v1/reports/import-parent`.
  - Validate parent service JWTs.
  - Save the parent payload into `linkx_reports` and immediately return HTTP 202 (Accepted) for optimal performance.
- [ ] **2.2 Autonomous XVigilance Hook (`XVIGILANCE_FINDING`)**
  - Refactor `linkx_xvigilance/publisher.py`.
  - Right before the daemon publishes to the Kafka topic (`dev.analysis.link.flagged.v1`), have it locally insert a record into `linkx_reports`.
  - This ensures every autonomous finding is persistently tracked in the LinkX workbench, not just fired into the void.
- [ ] **2.3 Sibling Service Evidence Interceptor (`SERVICE_EVIDENCE`)**
  - Intercept the end of the Risk Scoring pipeline (`risk_scoring_kafka_service.py` / `link_analysis_evidence` writes).
  - Automatically generate a `SERVICE_EVIDENCE` report capturing the Network Centrality Score, max path length, and flagged topologies (e.g., HUB_AND_SPOKE) for the analyst to review.

## Phase 3: Outbound Synchronization (Zero Data Loss)
National-scale systems cannot afford to drop findings if a network partition occurs.

- [ ] **3.1 Implement the Outbox Relay Worker (Server 3)**
  - Create a new background job in `batch_manager/jobs/report_outbox_relay.py`.
  - This job polls the `report_sync_outbox` table every few seconds.
  - Pushes updates asynchronously to the Parent API or sibling webhooks.
- [ ] **3.2 Sync Conflict Resolution**
  - If the parent system rejects an update due to a version mismatch, mark the outbox status as `conflict` and alert the local analyst to reconcile.

## Phase 4: Analyst Workbench & API
Analysts need to see these reports and bind them to graph workspaces.

- [ ] **4.1 Report Query API**
  - `GET /api/v1/reports` (Filterable by `report_type`, `status`, and `source_system`).
- [ ] **4.2 Workspace Binding API**
  - `POST /api/v1/reports/{id}/bind-workspace`
  - Allows an analyst to click a "Parent Report" or "XVigilance Finding" and instantly spawn/bind a LinkX visual graph workspace to investigate it.
- [ ] **4.3 Audit Trailing**
  - Wrap all the above API routes with `@audit_log` decorators to push access logs to `security_audit_events`.
- [ ] **4.4 RBAC Security Enforcement**
  - Enforce `RequirePermission("reports:read")` on all report routes.
  - Ensure `reports:read` is explicitly stripped from the `viewer` role so only `admin` and `analyst` roles can access them.
