# Workspace Plan

## Objective

Introduce a durable saved-workspace layer that lets a user save and later restore a full LinkX workstation state without reusing old runtime session ids directly.

The core model should be:

- Saved workspace = durable, user-owned snapshot
- Runtime session = temporary execution instance restored from that snapshot

This keeps runtime cleanup, worker activity, job history, and transient graph state separate from the user's saved investigation state.

## Design Principles

- Never treat historical runtime session ids as the durable saved object.
- Save references and normalized configuration, not transient worker/runtime state.
- Preserve secret refs only; never store raw passwords or tokens.
- Restore into fresh parent and child runtime sessions.
- Keep restores fast by rebuilding structure first and hydrating heavy data lazily.
- Make degraded restore possible when some artifacts or secret refs are no longer valid.

## Existing Backend Ground

The current backend already provides fertile ground for this design:

- `analysis_sessions` already models parent and child sessions via `parent_session_id`.
- `session_configs` already stores parent and window configs.
- `session_configs.source_config_id` already supports config lineage/copy ancestry.
- `managed_secrets` already separates secret storage from session config payloads.
- Artifacts, jobs, and logs already carry `session_id` and `job_id` references.
- `/init` and `/init_source` already create runtime parent and child sessions.

Because of that, this feature should be built as an additional durable layer above runtime sessions rather than as a rewrite of the session model.

## What Should Be Saved

### Parent configuration

Save durable parent-level state such as:

- active tool selection
- trusted entities
- risk entities
- rule preferences
- session-scoped backend configuration that affects behavior
- user-specific investigation defaults that belong to the workspace

### Window definitions

Save each logical window separately:

- logical window key like `1`, `2`, `3`
- source mode/type
- selected rule/filter/search state
- graph filter state
- per-window tool context
- dataframe identity or artifact reference
- backend-relevant child configuration

### Frontend workstation state

Save only what is needed to rebuild the workstation layout and active UI state:

- window layout
- active window
- opened panes/tabs
- selected relationship/rule/filter state
- graph camera or render preferences if useful

### Artifact references

Save references to heavyweight resources rather than embedding them:

- dataframe artifact ids
- uploaded rule artifact ids
- optional graph snapshot refs if ever supported
- optional source upload refs

### Credential references

Save only masked secret-bearing structures plus managed secret refs, for example:

```json
{
  "password": "***",
  "password_ref": "<managed_secret_id>"
}
```

## What Must Not Be Saved

- live worker state
- running/cancelling job status
- graph chunk payloads
- temporary cleanup markers
- runtime locks
- log stream state
- socket state
- raw passwords/tokens
- full graph payloads
- dataframe row content

## Proposed Data Model

### `saved_workspaces`

Suggested columns:

- `id uuid primary key`
- `owner_user_id bigint not null`
- `name text not null`
- `description text null`
- `status text not null default 'active'`
- `base_parent_config jsonb not null`
- `frontend_workspace_state jsonb not null default '{}'::jsonb`
- `source_parent_session_id text null`
- `created_at timestamptz not null`
- `updated_at timestamptz not null`
- `last_opened_at timestamptz null`

### `saved_workspace_windows`

Suggested columns:

- `id uuid primary key`
- `workspace_id uuid not null`
- `window_key text not null`
- `window_config jsonb not null`
- `frontend_window_state jsonb not null default '{}'::jsonb`
- `artifact_refs jsonb not null default '{}'::jsonb`
- `ordering integer not null default 0`
- `created_at timestamptz not null`
- `updated_at timestamptz not null`

### Optional `saved_workspace_audit`

Suggested columns:

- `id bigserial primary key`
- `workspace_id uuid not null`
- `actor_user_id bigint not null`
- `action text not null`
- `source_runtime_session_id text null`
- `restored_runtime_session_id text null`
- `metadata jsonb not null default '{}'::jsonb`
- `created_at timestamptz not null`

## Restore Flow

When a user restores a saved workspace:

1. Authenticate actor.
2. Authorize actor ownership/access to the saved workspace.
3. Create a fresh runtime parent session id.
4. Bind the new parent session to the actor in `analysis_sessions`.
5. Materialize a fresh parent `session_config` from `saved_workspaces.base_parent_config`.
6. Recreate child runtime sessions for each saved window:
   - `1_<parent>`
   - `2_<parent>`
   - etc.
7. Copy each saved window config into the new child session config.
8. Validate referenced artifacts and secret refs.
9. Return a restore manifest to the frontend.

Restore should be best-effort:

- if secret refs are missing, mark tool reconnect required
- if artifacts are missing, mark the window degraded
- if layout state is partially invalid, restore what remains valid

## Save Flow

When a user saves a workspace:

1. Read the active parent config.
2. Read all active child/window configs for that parent.
3. Accept normalized frontend workstation state from the request.
4. Strip transient runtime fields.
5. Preserve managed secret refs while keeping secrets masked.
6. Upsert the workspace row.
7. Upsert window rows.
8. Write an audit entry.

This should be an explicit save action first. Autosave can be added later.

## API Shape

### Save workspace

`POST /workspaces/save`

Suggested request:

```json
{
  "workspace_id": "optional-existing-id",
  "parent_session_id": "452162",
  "name": "June fraud investigation",
  "description": "Bank transactions workspace",
  "frontend_workspace_state": {},
  "frontend_windows": []
}
```

Suggested response:

```json
{
  "message": "success",
  "results": {
    "workspace_id": "<uuid>",
    "saved": true,
    "updated_at": "<timestamp>"
  }
}
```

### List workspaces

`GET /workspaces`

Return lightweight metadata only:

- id
- name
- description
- updated_at
- last_opened_at
- summary counts

### Get one workspace

`GET /workspaces/<id>`

Return metadata and normalized preview state, but not heavy graph payloads.

### Restore workspace

`POST /workspaces/<id>/restore`

Suggested response:

```json
{
  "message": "success",
  "results": {
    "workspace_id": "<uuid>",
    "runtime_session_id": "827441",
    "windows": [
      {
        "window_id": "1",
        "session_id": "1_827441",
        "config": {},
        "frontend_state": {}
      }
    ],
    "warnings": []
  }
}
```

### Archive or delete

- `POST /workspaces/<id>/archive`
- `DELETE /workspaces/<id>`

Prefer soft-delete/archive semantics first.

## Normalization Requirements

A dedicated normalizer should run both on save and restore so behavior stays symmetrical.

It should:

- allow only approved config structures
- normalize legacy names to canonical names
- preserve `password_ref` and other managed secret refs
- preserve masked secret placeholders
- remove job ids, log file names, running flags, chunk cursors, stop events, and other transient runtime fields

This normalizer should be shared by both API save/restore code paths.

## Artifact Strategy

Use references, not copies.

Rules:

- save artifact ids or durable paths only
- do not duplicate heavy artifacts into workspace rows
- validate referenced artifacts on restore
- return warnings when missing/expired
- keep workspace restore usable even if some artifacts are unavailable

This keeps rows smaller and restore latency lower.

## Secret Strategy

Secrets must remain in the existing managed-secret system.

Rules:

- never store raw password/token material in saved workspace rows
- store masked secret fields and refs only
- on restore, rebuild masked config with refs intact
- if secret decryption fails or ref is gone, do not silently fall back
- mark the restored tool/session as requiring reconnect

## Performance Strategy

To keep the system efficient:

- store metadata and refs, not giant payloads
- separate workspace and window rows
- keep list endpoints lightweight
- restore layout/config first, hydrate heavy data later
- index workspace ownership and updated timestamps
- avoid embedding full graph arrays or dataframe content in workspace records

Recommended indexes:

- `saved_workspaces(owner_user_id, updated_at desc)`
- `saved_workspace_windows(workspace_id, ordering)`

## Security Strategy

- Enforce ownership checks on every workspace action.
- Validate all incoming frontend workspace payloads with size limits.
- Keep auth token handling separate from workspace restore.
- Never trust frontend-provided runtime session ids without actor/session access checks.
- Preserve only managed secret refs for credentials.
- Audit save/restore/archive actions.
- Use soft-delete/archive before hard deletion.

## Rollout Plan

### Phase 1

- Add durable workspace tables
- Add save/list/get/restore/archive endpoints
- Save parent config and child configs only
- Restore into fresh runtime parent and child sessions

### Phase 2

- Add frontend workstation state persistence
- Add artifact refs
- Add degraded-restore warnings

### Phase 3

- Add drafts/autosave if needed
- Add version history
- Add rollback/compare tools if valuable

### Phase 4

- Consider export/import or sharing if ever required

## Recommended First Implementation Scope

The safest and most useful first implementation is:

- save parent config
- save child window configs
- save trusted entities
- save risk entities
- save tool refs and secret refs
- save frontend layout/workstation state
- restore into fresh parent and child runtime sessions

Explicitly postpone:

- stream replay
- in-flight job replay
- log replay
- graph payload persistence
- cancellation/cleanup state restore

## Frontend Contract Guidance

Frontend should use `workspace_id` as the durable identifier, not old runtime session ids.

Recommended flow:

1. user logs in
2. frontend calls `/init`
3. user works in runtime sessions
4. user clicks save
5. backend stores workspace snapshot
6. later user opens saved workspace
7. backend returns a fresh runtime restore manifest
8. frontend rebuilds the workstation from that manifest

The frontend should never assume a historical runtime session id is permanently reusable.

## Main Risks To Avoid

- saving too much transient runtime state
- coupling saved workspaces directly to old runtime session ids
- storing large graph/dataframe payloads in workspace rows
- leaking raw secrets through serialized config
- restoring stale cancellation or cleanup markers

## Recommendation

Implement a dedicated saved-workspace subsystem, not a permanent runtime-session persistence model.

That approach best matches the current backend architecture and gives the cleanest path for:

- secure secret handling
- efficient save/restore
- clean worker behavior
- safer session lifecycle management
- predictable frontend restoration
