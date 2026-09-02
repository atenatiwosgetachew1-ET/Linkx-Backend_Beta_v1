# IMPLEMENTED
This plan has been fully implemented. See below for the original design document.

# Report Integration Plan

## Objective

Introduce a secure, scalable report-integration subsystem that lets LinkX act as a specialized analysis microservice inside the parent centralized monitoring system.

The parent system owns the canonical report lifecycle. LinkX owns:

- analysis workspaces
- graph/evidence generation
- analyst findings for the LinkX domain
- bounded report collaboration features specific to link analysis

The design goal is to let analysts work naturally inside LinkX while keeping parent-report synchronization explicit, auditable, and resilient.

## Core Model

The recommended model is:

- Parent project = system of record for reports
- LinkX = specialized analysis service with local report workbench

That means:

- parent report identity stays canonical upstream
- LinkX stores only a local mirror/cache of the report data it needs
- analysts bind parent reports to saved workspaces
- LinkX saves findings and evidence locally first
- LinkX synchronizes structured updates back to the parent through explicit sync flows

## Why This Model Fits LinkX

The current backend already has important foundation pieces:

- CTMS/parent SSO token exchange via `/auth/parent-token`
- service-to-service authentication via `/auth/service-token`
- actor/session ownership via `analysis_sessions`
- durable session config storage via `session_configs`
- workspace layout persistence
- artifact registration and retention support
- RBAC vocabulary that already includes `reports:read`

Because of that, the report feature should be built as a report domain layer on top of the current auth/session/artifact base, not as a parallel identity or persistence system.

## Design Principles

- Parent report state remains canonical in the parent system.
- LinkX keeps a minimal local mirror of report data needed for analysis.
- User actions and machine sync actions should be distinct and auditable.
- Findings and evidence are persisted locally before any upstream sync attempt.
- Secret handling must stay inside the existing managed-secret model.
- Frontend should operate on LinkX-local report/workspace bindings, not orchestrate parent synchronization itself.
- Outbound report updates should be retryable and decoupled from the UI request path.

## Functional Scope

The feature should support:

- receiving report context from the parent project
- storing a local report mirror
- binding a report to a saved workspace
- attaching evidence artifacts to a report
- saving LinkX-specific findings
- allowing limited analyst-side report actions inside LinkX
- synchronizing finalized findings back to the parent

The feature should not try to replace the parent report system.

## Recommended User Flow

### Parent-to-LinkX entry

1. Analyst is authenticated in the parent system.
2. Parent system launches LinkX with parent identity context.
3. LinkX exchanges the parent token for a local LinkX JWT.
4. Parent report context is passed or fetched.
5. LinkX upserts a local mirror of that report.

### Report workspace binding

1. Analyst opens an existing workspace or creates a new one.
2. Analyst binds the current parent report to that saved workspace.
3. LinkX stores the binding locally.
4. Future reopen operations can resume the same report-linked investigation workspace.

### Analysis work

1. Analyst performs graph and link analysis in LinkX.
2. Findings are stored locally against:
   - report
   - workspace
   - analyst
3. Evidence artifacts are attached as references.
4. Intermediate saves remain local and responsive.

### Sync back to parent

1. LinkX prepares a structured update payload.
2. Payload is queued in an outbound sync outbox.
3. Sync worker or retrier pushes the update to the parent system.
4. Parent acknowledges and updates canonical report state.
5. LinkX records sync status and timestamps.

## Trust Boundaries

The system should use two distinct trust paths.

### User trust path

- parent user authenticates with CTMS
- LinkX exchanges CTMS token via `/auth/parent-token`
- LinkX creates/updates the external user locally
- all analyst actions in LinkX are attributed to that local actor

### Service trust path

- LinkX calls parent report APIs using service credentials or service tokens
- the service identity should be machine-authenticated
- the acting analyst should be carried as metadata, not by reusing a user token for backend-to-backend calls unless the parent explicitly requires delegation

This separation keeps user accountability and service trust clear.

## Recommended Data Model

### `linked_reports`

This stores the local mirrored report state.

Suggested columns:

- `id uuid primary key`
- `parent_report_id text not null unique`
- `owner_user_id bigint null`
- `assigned_user_id bigint null`
- `report_snapshot jsonb not null`
- `parent_status text null`
- `parent_version text null`
- `sync_state text not null default 'fresh'`
- `last_parent_sync_at timestamptz null`
- `created_at timestamptz not null`
- `updated_at timestamptz not null`

Purpose:

- local mirror/cache
- minimal metadata needed for LinkX analysis
- sync/version tracking

### `report_workspace_bindings`

This links a parent report to a saved workspace and optionally the active runtime session.

Suggested columns:

- `id uuid primary key`
- `parent_report_id text not null`
- `workspace_id uuid not null`
- `active_runtime_session_id text null`
- `bound_by_user_id bigint not null`
- `status text not null default 'active'`
- `created_at timestamptz not null`
- `updated_at timestamptz not null`

Purpose:

- durable report-to-workspace relationship
- ability to resume report-linked investigations

### `report_findings`

This stores LinkX-generated findings as structured records.

Suggested columns:

- `id uuid primary key`
- `parent_report_id text not null`
- `workspace_id uuid not null`
- `finding_type text not null`
- `severity text null`
- `summary text not null`
- `details jsonb not null`
- `source_window_id text null`
- `created_by_user_id bigint not null`
- `created_at timestamptz not null`
- `updated_at timestamptz not null`

Purpose:

- structured LinkX output
- queryable and syncable findings

### `report_evidence`

This stores evidence references tied to a report and workspace.

Suggested columns:

- `id uuid primary key`
- `parent_report_id text not null`
- `workspace_id uuid not null`
- `artifact_id uuid not null`
- `evidence_type text not null`
- `caption text null`
- `metadata jsonb not null default '{}'::jsonb`
- `created_by_user_id bigint not null`
- `created_at timestamptz not null`

Purpose:

- attach images, graph snapshots, exports, and other evidence through artifact refs

### `report_sync_outbox`

This stores outbound updates to the parent system.

Suggested columns:

- `id uuid primary key`
- `parent_report_id text not null`
- `event_type text not null`
- `payload jsonb not null`
- `status text not null default 'pending'`
- `attempts integer not null default 0`
- `last_error text null`
- `scheduled_at timestamptz not null`
- `created_at timestamptz not null`
- `updated_at timestamptz not null`

Purpose:

- decouple UI actions from upstream availability
- enable retries and auditability

### Optional `report_action_audit`

Suggested columns:

- `id bigserial primary key`
- `parent_report_id text not null`
- `workspace_id uuid null`
- `actor_user_id bigint not null`
- `action text not null`
- `metadata jsonb not null default '{}'::jsonb`
- `created_at timestamptz not null`

Purpose:

- traceability for analyst and service actions

## What Should Be Mirrored From Parent

Only mirror the minimum fields needed by LinkX.

Good candidates:

- `parent_report_id`
- parent report title or label
- report type
- high-level report status
- subject/entity references
- case metadata needed by analysis rules
- assigned analyst/team
- version or etag
- timestamps relevant to freshness

Avoid mirroring:

- unrelated parent-only workflow data
- giant nested payloads not used by LinkX
- large attachments
- raw upstream auth material

## What LinkX Should Own Locally

LinkX should own:

- report-to-workspace binding
- analyst findings
- evidence references
- local progress states for the LinkX portion of the report
- final LinkX summary/recommendation before synchronization

This preserves the parent system as canonical while letting LinkX remain a first-class analysis environment.

## Limited Analyst Report Actions

The analyst-side actions that fit well in LinkX are:

- bind report to workspace
- update local LinkX findings
- attach or remove evidence references
- mark LinkX analysis as draft / ready / finalized
- generate report-specific graph/image evidence
- request sync back to parent

Potentially allowed later, if parent supports it:

- update parent-visible analysis notes
- update LinkX section status inside the report
- reopen the LinkX section if parent policy allows

Avoid broad parent-report editing inside LinkX unless there is a very clear reason.

## API Design Inside LinkX

Recommended internal endpoints:

### Report mirror upsert / fetch

- `POST /reports/import`
- `GET /reports`
- `GET /reports/<parent_report_id>`

`POST /reports/import` should be used by the parent system or a trusted service integration flow to seed/update the local mirror.

### Report-workspace binding

- `POST /reports/<parent_report_id>/bind-workspace`
- `GET /reports/<parent_report_id>/workspace`

### Findings

- `POST /reports/<parent_report_id>/findings`
- `GET /reports/<parent_report_id>/findings`
- `PATCH /reports/<parent_report_id>/findings/<finding_id>`

### Evidence

- `POST /reports/<parent_report_id>/evidence`
- `GET /reports/<parent_report_id>/evidence`
- `DELETE /reports/<parent_report_id>/evidence/<evidence_id>`

### Finalization and sync

- `POST /reports/<parent_report_id>/finalize-linkx`
- `POST /reports/<parent_report_id>/sync`
- `GET /reports/<parent_report_id>/sync-status`

## Outbound Sync Strategy

Outbound sync should use an outbox model.

Recommended flow:

1. user performs action in LinkX
2. LinkX commits local state
3. LinkX writes outbox event
4. async worker sends update to parent
5. response updates outbox state

Benefits:

- UI stays responsive
- parent downtime does not destroy analyst work
- retries are straightforward
- audit trail remains complete

Suggested outbound event types:

- `report_workspace_bound`
- `report_finding_added`
- `report_evidence_attached`
- `report_linkx_section_finalized`
- `report_linkx_section_reopened`

## Parent Communication Strategy

LinkX should not call the parent on every page or action.

Use this pattern:

- mirror once
- work locally
- sync explicitly

Possible synchronization sources:

- parent pushes report context into LinkX
- LinkX fetches report context on demand
- LinkX refreshes mirror when stale or when a sync conflict is detected

Recommended freshness controls:

- `parent_version`
- `last_parent_sync_at`
- `sync_state`

## Conflict Strategy

Conflicts will happen if parent report state changes while analysis is in progress.

Suggested approach:

- treat parent as source of truth
- store `parent_version` on mirror rows
- include `parent_version` in outbound updates
- if mismatch occurs:
  - mark sync conflict
  - keep local findings intact
  - require explicit refresh/reconcile

Do not silently discard local LinkX analyst work.

## Security Strategy

### Authentication

- parent user entry via CTMS token exchange
- service-to-service operations via service account credentials
- no raw parent secrets in frontend

### Authorization

- local user must be allowed to access the linked report
- report operations should check:
  - actor identity
  - report access
  - workspace ownership or workspace access policy

### Secret handling

- never store parent access tokens in session config
- never store raw service credentials in report rows
- use managed secrets for any persistent upstream credentials if needed

### Auditability

Every meaningful action should be attributable:

- who bound the report
- who added evidence
- who finalized LinkX section
- what payload was sent to parent
- when sync succeeded or failed

### Payload control

- validate imported parent report payloads
- size-limit local mirror snapshots
- sanitize free-text notes and evidence captions

## Performance Strategy

To keep this scalable:

- store only minimum report snapshot fields
- reference artifacts instead of storing blobs
- query local mirror rather than parent on every screen
- use asynchronous outbox sync
- avoid embedding report data in session configs
- index report ids, workspace ids, sync states, and assigned actor ids

Recommended indexes:

- `linked_reports(parent_report_id)`
- `linked_reports(assigned_user_id, updated_at desc)`
- `report_workspace_bindings(parent_report_id)`
- `report_workspace_bindings(workspace_id)`
- `report_findings(parent_report_id, created_at desc)`
- `report_evidence(parent_report_id, created_at desc)`
- `report_sync_outbox(status, scheduled_at)`

## Relationship To Saved Workspaces

This feature should integrate directly with the saved-workspace model, not with old runtime sessions.

Best pattern:

- report binds to `workspace_id`
- workspace restore creates fresh runtime sessions
- active runtime session may be tracked in binding state
- old runtime session ids remain temporary execution identifiers only

This keeps report linkage stable even when runtime sessions rotate or are recreated.

## Relationship To Session Configs

Report linkage should not live only inside session configs.

Session configs are appropriate for:

- current runtime behavior
- active tool/rule settings
- temporary active state

Dedicated report tables are better for:

- durable report identity
- findings
- evidence refs
- sync state
- workspace linkage

This separation keeps the domain clean and queryable.

## Suggested First Implementation Scope

The best first cut is:

1. local mirrored report table
2. report-to-saved-workspace binding table
3. findings table
4. evidence table using artifact refs
5. outbound sync outbox
6. basic endpoints for bind, save finding, attach evidence, finalize LinkX section

This is already enough to support meaningful parent-service collaboration without overbuilding.

## Suggested Rollout Phases

### Phase 1

- define report tables
- implement local report mirror ingest
- implement report-workspace binding
- implement finding and evidence persistence

### Phase 2

- implement outbound sync outbox
- implement parent update API client
- add sync status and retry behavior

### Phase 3

- add conflict handling and refresh/reconcile flows
- add richer evidence generation from graph outputs
- add limited parent-visible summary editing

### Phase 4

- optional cross-service orchestration enhancements
- optional shared report timelines or combined evidence feeds if parent supports them

## Main Risks To Avoid

- turning LinkX into a second full report system
- using live parent APIs for every UI request
- storing report state in session config blobs
- synchronously coupling analyst actions to parent availability
- leaking parent or service credentials
- overloading runtime session ids with durable report identity

## Recommendation

Build LinkX report handling as a dedicated local report-workbench subsystem with:

- local mirrored report state
- report-to-workspace binding
- local findings and evidence
- asynchronous upstream synchronization

That gives the cleanest fit for:

- parent SSO
- sibling-service architecture
- analyst accountability
- performance under load
- resilience when the parent system is slow or temporarily unavailable
