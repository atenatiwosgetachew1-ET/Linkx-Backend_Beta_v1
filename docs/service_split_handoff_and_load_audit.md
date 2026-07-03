# LinkX Service Split Handoff And Load Audit

Last updated: 2026-07-03

This document is a handoff summary for a new chat/session. It captures the current four-server LinkX backend split, how the services communicate, how deployments are updated, and what work is still running on each server. The goal of the split is to keep the current working system stable while gradually moving heavy analysis and ingestion work out of the API server and into worker/maintenance services.

## Latest Status Snapshot

As of 2026-07-03, the backend hardening and Parent project SSO implementation are functionally complete on the LinkX side. Runtime secrets were moved out of raw config JSON into encrypted `managed_secrets`, old hardcoded/default credentials were rotated, `.env` files were locked down, legacy parent shared-secret SSO was disabled by default, request body limits and security audit events were added, and service hardening/UFW restrictions were applied across the four servers.

Parent project SSO now supports two backend entry points:
- preferred browser flow: OAuth authorization code + PKCE through `POST /auth/exchange` (and `POST /api/auth/exchange`)
- rollback/direct-token flow: `POST /auth/parent-token` with a verified ES256 access JWT

LinkX verifies Parent project access JWTs through JWKS, with the configured PEM public key retained only as a fallback verifier key. The remaining end-to-end dependency is still a real short-lived Parent project ES256 access JWT plus a live OAuth code exchange from the Parent project environment so the final browser flow can be proven against the actual issuer, audience, redirect URI, and userinfo payload.

P2 recovery evidence is now mostly proven: PostgreSQL backup/restore passed, artifact backup/restore passed, Neo4j offline dump/load passed against the current empty graph, Redis was verified disposable, and the `LINKX_SECRET_ENCRYPTION_KEY` recovery copy is kept separately by the developer. The remaining P2 follow-ups are to install/enable the scheduled backup timers, configure an encrypted/off-host backup target, and repeat the Neo4j restore drill after a representative ingestion creates nonzero nodes/relationships.

Current repo-side follow-up: backup timer units and retention/off-host helper scripts are present in the repo but still need deployment to the relevant servers. Treat the uninstalled timer work as ready for ops rollout, not yet live production automation.

## 2026-07-03 HTTPS SSO Rollout Note

- Server 1 API environment was aligned to the HTTPS frontend callback flow at `https://172.27.23.21/auth/callback`.
- Live frontend origin in use is `https://172.27.23.21`, with LinkX embedded under `https://172.27.23.21/linkxDS2026/`.
- Server 1 now uses HTTPS Parent project auth endpoints/origins in runtime env, and the API drift check passed after restart.
- Verified live checks on Server 1 after rollout:
  - `GET http://127.0.0.1:8000/db/health -> 200`
  - `GET http://127.0.0.1:8000/auth/me -> 401`
  - nginx local gateway checks also passed
- Remaining proof is no longer backend boot/config validation; it is the real browser OAuth round trip plus Parent project-side callback/origin registration and a trusted certificate chain for the LinkX HTTPS host.

## 2026-06-30 Session Config / Neo4j / Entity Catalog Status

This is the current backend state after the latest API and worker fixes in `service_factory/services/**`.

### Neo4j connection and streaming/session handling

- `POST /connect_to_tool` now validates Neo4j credentials before attempting the connection and persists the canonical window-session credential record only after a successful connection.
- Masked passwords remain masked in config JSON as `***`; the live secret is still resolved through `password_ref` and `managed_secrets`.
- `POST /live_batch_files` with `id=stream` now rejects invalid or missing Neo4j session credentials with a clean `400` body:
  - `message: "neo4j_not_connected_for_session"`
  - `detail: "<reason>"`
- This replaced the earlier crashy behavior where stale or non-dict `tool_credentials` could bubble into a misleading frontend message such as "Unable to reach the streaming service."

### Config-save during active stream

- Saving configuration during an active streaming/realtime session previously risked wiping runtime connection fields and forcing a reconnect.
- The API `configuration` save path now preserves runtime connection fields through `_preserve_runtime_connection_fields(...)` before normalization/merge.
- `connection_utils.disconnect(...)` was also adjusted so `tool_credentials` is cleared as `None` instead of an empty string, which avoids later type confusion.

### Trusted and risk entity catalogs

- Backend naming is now standardized on:
  - `trusted_entities`
  - `risk_entities`
- The old name `trusted_catalog` is retained only as a compatibility fallback while older saved configs or frontend payloads are still being phased out.
- Runtime rule-loading logs were updated to:
  - `Trusted entities loaded: <n>`
  - `Risk entities loaded: <n>`
- Rule helpers now live under:
  - `service_factory/services/linkx-api/src/batch_manager/utils/Classified_entities.py`
  - `service_factory/services/linkx-worker/src/batch_manager/utils/Classified_entities.py`
- The former `trusted_catalog.py` helper is no longer the intended active file. `Classified_entities.py` is the canonical catalog helper.

### Entity payload shape expected by backend

Each entry must be a dynamic object of scalar key/value pairs. Example valid payloads:

```json
{
  "trusted_entities": [
    { "ACCOUNTNO": "ACC10001" },
    { "customer_id": 12345, "status": "government" }
  ],
  "risk_entities": [
    { "ACCOUNTNO": "ACC10035" }
  ]
}
```

This older shape is not correct for matching logic and should not be used:

```json
{
  "risk_entities": [
    { "key": "ACCOUNTNO", "value": "ACC10035" }
  ]
}
```

### Session placement rule for entity catalogs

- Classified entity lists are intended to belong to the parent/base session, not to each individual window session.
- Example:
  - parent session: `452162`
  - window session: `1_452162`
- The frontend should save `trusted_entities` and `risk_entities` against the parent session so every window inherits the same user-specific classifications.

### Current known edge still worth watching

- If a window session stores its own empty `risk_entities` or `trusted_entities`, that window-level empty value can override a non-empty parent value depending on the load path.
- Operationally, the frontend should currently avoid saving empty classified-entity arrays into child/window sessions when the intent is to inherit from the parent session.

### Graph fetch delivery model

- `POST /get_graph` now queues `graph_fetch` work onto the worker.
- `GET /jobs/<job_id>` returns:
  - lightweight event metadata for graph jobs
  - canonical `result`
  - optional progressive `chunks` when `include_chunks=1`
- Current graph chunk behavior is controlled by environment variables on both services:
  - Server 3 worker emits chunks with `LINKX_GRAPH_FIRST_CHUNK_SIZE` and `LINKX_GRAPH_CHUNK_SIZE`
  - Server 1 API limits chunk events per poll with `LINKX_GRAPH_CHUNK_POLL_LIMIT`
- Graph relationship fetching now uses two layers:
  - Neo4j fetch pages controlled by `LINKX_GRAPH_FETCH_PAGE_SIZE`, default `5000` relationships per page
  - frontend delivery chunks controlled by `LINKX_GRAPH_FIRST_CHUNK_SIZE`, default `100`, then `LINKX_GRAPH_CHUNK_SIZE`, default `250`
- The graph worker pages Neo4j by relationship cursor (`id(r)`) instead of running one large relationship query. This keeps the frontend chunk contract intact while reducing long-query pressure and improving cancellation responsiveness.
- Graph job results/chunk metadata can include `fetch_page_size`, `pages_fetched`, `last_relationship_cursor`, `complete`, and `truncated_by`.
- `LINKX_GRAPH_FETCH_LIMIT` remains the hard total relationship cap for an interactive graph request; when reached, the result is marked partial with `truncated_by=graph_limit`.

### Frontend alignment notes

- Do not send masked secrets as fresh values. When a Neo4j password is unchanged, preserve the existing connection state instead of resubmitting `***` as a new password.
- Use `trusted_entities` rather than `trusted_catalog` in new payloads.
- Save `trusted_entities` and `risk_entities` on the parent session.
- Send classified entities as dynamic objects, not `{key, value}` wrappers.

### Parent session reuse and timed rotation

- `POST /init` no longer blindly creates a brand-new random parent session for the same user on every fresh login/bootstrap.
- The API now first tries to reuse the actor's latest active parent session when it is still within the configured rotation age.
- If the current parent session is older than the configured interval, the API rotates to a new parent session id and seeds the new parent session config from the previous parent session config before returning it.
- This preserves session-scoped values such as classified entity lists across controlled parent-session rotation instead of dropping back to defaults.

Current API behavior:
- request provides `existing_session` or `session_id`: API tries that first
- otherwise API checks the actor's latest active parent session
- if that parent session is still fresh: API reuses it
- if that parent session is too old: API creates a new parent session and copies config forward from the old parent session

Current response additions from `/init`:
- `session_rotated: true|false`
- `rotated_from_session: <old_parent_session_id>` when rotation occurred

Current environment knob on Server 1 API:

```env
LINKX_SESSION_ROTATION_SECONDS=43200
```

Notes:
- `43200` means 12 hours
- `0` disables timed rotation
- the implementation also avoids relying on a single random-id attempt; parent-session creation now retries allocation before failing

Scope note:
- this improves continuity for session-scoped config, but it does not change the longer-term design split between user-scope config and parent-session-scope config
- `trusted_entities` and `risk_entities` are still best treated as user-owned/shared-across-windows data even though parent-session continuity is now improved

## 2026-06-28 Ingestion/Graph Debug Status

Current reported regressions after security hardening:
- Realtime ingestion connects to broker/topic/tool, but Link Analysis, Source/Target mapping, and Store Data all fail with Neo4j `AuthenticationRateLimit`, meaning the runtime is repeatedly presenting invalid Neo4j credentials.
- Batch Source/Target mapping loads far enough to start, then writes `Neo4j driver not found` and the frontend shows `Streaming could not start` / `analysis failed or was cancelled`.

Finding: the most likely backend cause is masked-secret echo/overwrite. `tool_credentials.password` is intentionally stored and returned as `***` with a separate `password_ref`, but a later frontend/config save can send back only the masked `password: "***"`. Before the latest fix, `save_session_config(..., merge=True)` used a shallow merge, so `tool_credentials` could be replaced and lose `password_ref`. Once that happened, workers could reload `***` instead of decrypting the real password from `managed_secrets`, causing Neo4j auth failures and then `Neo4j driver not found`.

Backend mitigation now present in both API and worker service copies:
- [session_config_store.py](../service_factory/services/linkx-api/src/session_config_store.py) and [session_config_store.py](../service_factory/services/linkx-worker/src/session_config_store.py) use recursive `_merge_config()` for session config saves, preserving nested `password_ref` fields when masked values are echoed back.
- [analyzer.py](../service_factory/services/linkx-api/src/batch_manager/analyzing/analyzer.py) and [analyzer.py](../service_factory/services/linkx-worker/src/batch_manager/analyzing/analyzer.py) now validate `payload["tool_credentials"]` directly before falling back to `tools("neo4j", "check", {"session_id": ...})`. This prevents a valid decrypted job payload from being discarded only because the separate config lookup is stale or window-mismatched.

Security/privacy posture remains intact: raw passwords still stay in `managed_secrets`, frontend/API responses can remain masked, and logs should continue redacting credentials. This fix preserves secret refs; it does not reintroduce plaintext config storage.

Frontend alignment still recommended: when saving config, do not send masked secret values as real updates. If a password/token field is unchanged, omit that field or preserve the backend-provided `*_ref`; only send a real secret value when the user intentionally changes it.

Next verification after deploying the fix:
1. Deploy API `session_config_store.py` and analyzer copy to Server 1, worker `session_config_store.py` and analyzer copy to Server 3.
2. Restart `linkx-api`, `linkx-analysis-worker`, and relevant worker services.
3. Reconnect Neo4j once from the frontend so a clean `tool_credentials.password_ref` is saved.
4. Query `session_configs` and confirm `tool_credentials.password='***'` and `tool_credentials.password_ref` exists for the active source/window.
5. Retry realtime Store Data, realtime Link Analysis, realtime Source/Target, and batch Source/Target.
6. Watch logs for absence of `AuthenticationRateLimit` and `Neo4j driver not found`.

## Current Server Map

| Server | Hostname Seen | IP | Main Role | Runtime | Main Path | Service Names |
|---|---:|---:|---|---|---|---|
| Server 1 | node-19 | 172.27.23.95 | Flask API, RBAC, Socket.IO, frontend-facing routes, auth service tokens | Python venv + systemd | `/opt/linkx-backend-api` | `linkx-api` |
| Server 2 | node-20 | 172.27.23.106 | PostgreSQL + Redis control data | Docker Compose + systemd wrapper | `/opt/linkx-control-data` | `linkx-control-data`, containers `linkx-postgres`, `linkx-redis` |
| Server 3 | node-21 | 172.27.23.18 | Analysis/ingestion/dataframe workers | Python venv + systemd | `/opt/linkx-worker` | `linkx-worker` |
| Server 4 | node-22 | 172.27.23.85 | Neo4j Enterprise + cleanup services | Neo4j Docker Compose, cleanup Python venv + systemd | `/opt/linkx-neo4j`, `/opt/linkx-graph-maintenance` | `linkx-cleanup-worker`, `linkx-cleanup-scheduler`, Neo4j container `linkx-neo4j` |

Frontend is a separate React/Vite project and talks to Server 1 only.

## Intended Communication Shape

| From | To | Ports | Reason |
|---|---|---:|---|
| Frontend / browser | Server 1 | 8000 | Main LinkX API + Socket.IO |
| Server 1 | Server 2 | 5432, 6379 | Auth/RBAC/config/session metadata, job queue/control data |
| Server 3 | Server 2 | 5432, 6379 | Claim jobs, read config/control data, write job status/events |
| Server 3 | Server 4 | 7687 | Worker analysis writes/reads Neo4j graph data |
| Server 1 | Server 4 | 7687 | Current API graph reads, tool checks, some legacy/interactive paths |
| Server 4 cleanup | Server 2 | 5432, 6379 | Read cleanup runs/artifacts/sessions, write cleanup audit |
| Admin workstation | Servers | 22 | SSH |
| Admin workstation | Server 4 | 7474 | Neo4j Browser admin access only |
| AI partner service | Server 1 | 8000 | Controlled `/auth/service-token` and `/ai/...` API access |

The AI partner service should not access Postgres, Redis, or Neo4j directly. It uses a LinkX service account:

```env
LINKX_API_BASE_URL=http://172.27.23.95:8000
LINKX_AI_CLIENT_ID=ai
LINKX_AI_CLIENT_SECRET=ai@alex
```

Token request:

```bash
curl -X POST http://172.27.23.95:8000/auth/service-token \
  -H "Content-Type: application/json" \
  -d '{"client_id":"ai","client_secret":"ai@alex"}'
```

Then call `/ai/...` with `Authorization: Bearer <token>`.

## Current Firewall Posture

Server 1 currently allows API port 8000 from frontend/admin/AI service. Server 2 allows Postgres/Redis only from API, worker, and graph-maintenance servers. Server 4 allows Neo4j Bolt only from API and worker, Neo4j Browser only from the admin workstation.

Known AI direct access cleanup already done:

- PostgreSQL direct role `linkx_ai` removed.
- Server 2 UFW rule allowing `172.27.23.195 -> 5432` removed.
- Neo4j user `ai` dropped.
- Server 4 UFW rules allowing `172.27.23.195 -> 7687/7474` removed.
- Server 1 UFW allows `172.27.23.195 -> 8000`.

## Main Repository Structure

The old monolith still exists at repo root, but the active refactor/deployment factory is under `service_factory/services`.

| Path | Purpose |
|---|---|
| `service_factory/services/linkx-api` | Server 1 deployable API/RBAC/Socket service |
| `service_factory/services/linkx-worker` | Server 3 deployable DB-backed worker service |
| `service_factory/services/linkx-control-data` | Server 2 Postgres/Redis Docker Compose, migrations, backups |
| `service_factory/services/linkx-graph-maintenance` | Server 4 cleanup services and Neo4j compose assets |
| `service_factory/services/linkx-api/src/main.py` | Main Flask routes and Socket.IO bootstrap |
| `service_factory/services/linkx-api/src/auth` | Users, service accounts, JWT, RBAC routes |
| `service_factory/services/linkx-api/src/api/ai_service.py` | Controlled AI partner API endpoints |
| `service_factory/services/linkx-api/src/api/STR_link_analysis.py` | External/source-target-report API; currently still heavy in API |
| `service_factory/services/linkx-worker/src/linkx_worker` | Worker runner, job claiming, cancellation handling |
| `service_factory/services/linkx-graph-maintenance/src/linkx_cleanup` | Cleanup worker/scheduler/tasks |
| `service_factory/services/linkx-control-data/postgres/migrations` | Control DB schema migrations |

## Current Update Workflow

Development happens in the local repo under `/var/www/linkx-backend`. Changes are pushed/pulled into `/opt/linkx-backend-update` on the servers, then copied into the clean deployed service directories.

Typical Server 1 API update:

```bash
cd /opt/linkx-backend-update
sudo git pull
sudo cp /opt/linkx-backend-update/service_factory/services/linkx-api/src/main.py /opt/linkx-backend-api/src/main.py
sudo cp /opt/linkx-backend-update/service_factory/services/linkx-api/src/api/ai_service.py /opt/linkx-backend-api/src/api/ai_service.py
cd /opt/linkx-backend-api/src
sudo /opt/linkx-backend-api/.venv/bin/python -m py_compile main.py api/ai_service.py
sudo systemctl restart linkx-api
sudo systemctl status linkx-api --no-pager
```

Typical Server 3 worker update:

```bash
cd /opt/linkx-backend-update
sudo git pull
sudo cp -r /opt/linkx-backend-update/service_factory/services/linkx-worker/src/<changed-file-or-folder> /opt/linkx-worker/src/<target>
cd /opt/linkx-worker/src
sudo /opt/linkx-worker/.venv/bin/python -m py_compile <changed-files>
sudo systemctl restart linkx-worker
sudo systemctl status linkx-worker --no-pager
```

Typical Server 4 cleanup update:

```bash
cd /opt/linkx-backend-update
sudo git pull
sudo cp /opt/linkx-backend-update/service_factory/services/linkx-graph-maintenance/src/linkx_cleanup/tasks.py /opt/linkx-graph-maintenance/src/linkx_cleanup/tasks.py
sudo cp /opt/linkx-backend-update/service_factory/services/linkx-graph-maintenance/src/batch_manager/utils/neo4j_cleanup.py /opt/linkx-graph-maintenance/src/batch_manager/utils/neo4j_cleanup.py
cd /opt/linkx-graph-maintenance/src
sudo /opt/linkx-graph-maintenance/.venv/bin/python -m py_compile linkx_cleanup/tasks.py batch_manager/utils/neo4j_cleanup.py
sudo systemctl restart linkx-cleanup-worker
sudo systemctl restart linkx-cleanup-scheduler
```

## Current Service Contracts Worth Remembering

### Auth And AI

| Endpoint | Purpose |
|---|---|
| `POST /auth/login` | User login |
| `POST /auth/service-token` | Service-account token, used by AI service |
| `POST /auth/lock` | Backend-enforced session lock |
| `POST /auth/unlock` | Unlock validation, no cleanup |
| `POST /auth/idle-timeout` | Final max-idle expiry and cleanup |
| `GET /auth/session-policy` | Frontend idle-lock policy |
| `GET /ai/health` | AI service health check |
| `GET /ai/sessions` | AI session listing |
| `GET /ai/sessions/<session_id>` | AI session detail |
| `GET /ai/sessions/<session_id>/artifacts` | AI artifact list |
| `GET /ai/cleanup-runs?session_id=<session_id>` | AI cleanup/audit read |
| `GET /ai/sessions/<session_id>/graph/metadata` | AI graph summary from Neo4j |

### Frontend/Core Analysis

| Endpoint | Current Purpose | Load Concern |
|---|---|---|
| `POST /connect_to_source` | Checks Kafka/API/HDFS and often fetches latest data sample | API does network I/O and may create sample DataFrame |
| `POST /connect_to_tool` | Checks Neo4j credentials/connectivity | API opens Neo4j driver/check |
| `POST /live_batch_files` with `id=search` | HDFS/Elastic/Hive search flow | API calls `batch_data_manager` directly |
| `POST /live_batch_files` with `id=create_DF` | Creates/merges dataframe artifacts | API calls dataframe workflow directly |
| `POST /live_batch_files` with `id=stream` | Creates and starts analysis session | API still calls `batch_data_manager` and may start API-memory thread |
| `POST /get_graph` | Fetch graph relationship payload | API calls Neo4j graph fetch directly, can be long-running |
| `POST /api/STR_link_analysis` | External STR report analysis | API performs ES search, dataframe creation, analyzer, Neo4j summary |

## Load Audit By Server

| Server | Current Activities | Load Type | Risk / Notes | Should Move? |
|---|---|---|---|---|
| Server 1 API | Auth/RBAC, JWT, service tokens, user/session config, CORS, request validation | Light CPU, DB I/O | Correct place for this work | No |
| Server 1 API | Socket.IO log streaming and graph status polling | Persistent connections, periodic DB/Neo4j metadata reads | Acceptable if throttled; can grow with many analysts | Maybe later to notification service |
| Server 1 API | `connect_to_source` Kafka/API/HDFS checks and latest sample load | Network I/O, Kafka consumer, WebHDFS/API calls, sample DataFrame | This pressures API during source connection and can stall requests | Yes, move source checks/sample preview to worker job |
| Server 1 API | `live_batch_files` search path via `batch_data_manager` | Elastic/Hive/HDFS network I/O and dataframe preparation | Should be job-backed for large searches or Hive fallback | Yes |
| Server 1 API | `live_batch_files` `create_DF` | File I/O, pandas/Spark/HDFS/Elastic/Hive reads | Heavy and belongs on worker | Yes, high priority |
| Server 1 API | `live_batch_files` `stream/start_session` | Creates API-memory streaming/session thread via `batch_data_manager`/session manager | Major source of zombie ingestion; restart stops it, proving it lives in API memory | Yes, highest priority |
| Server 1 API | `get_graph` relationship graph fetch | Neo4j read, graph payload generation, possible 15-min fetch | User-triggered, but expensive and blocks API worker | Prefer worker/graph-read job with polling |
| Server 1 API | `api/STR_link_analysis` | ES search, dataframe creation, analyzer, Neo4j writes, Neo4j summaries | Very heavy; directly calls analyzer in API | Yes, high priority |
| Server 1 API | AI service endpoints | Postgres reads and Neo4j read-only queries | Controlled, okay for now; relationship queries capped | Keep for now; monitor |
| Server 2 Control Data | PostgreSQL auth/config/session/jobs/artifacts/cleanup audit | DB I/O | Correct role; tune indexes and backups as usage grows | No |
| Server 2 Control Data | Redis | Control/cache/queue support | Current worker claims jobs from Postgres; Redis role is light/transitional | No |
| Server 3 Worker | DB-backed jobs for ingestion/dataframe/analysis/graph queues | Heavy CPU/I/O/Neo4j writes | Correct destination for analysis jobs; currently underused if API bypasses queue | Should receive migrated API work |
| Server 3 Worker | Cancellation-aware job execution | Long-running job control | Good design; relies on jobs table and cancellation state | Keep |
| Server 4 Graph/Cleanup | Neo4j Enterprise graph storage/query | Graph DB CPU/RAM/disk | Correct place for graph persistence | No |
| Server 4 Graph/Cleanup | Cleanup worker/scheduler: Neo4j session cleanup, artifacts, configs, old metadata, abandoned sessions | DB I/O, filesystem I/O, Neo4j deletes | Correct place; ensure all session/window events enqueue cleanup | No |

## Biggest Remaining Architecture Mismatch

Server 3 worker is ready to run `batch_data_manager` and `analyzer` jobs, but Server 1 still calls those same functions directly in several frontend-facing routes. That means the system is physically split but not fully behaviorally split yet.

The most important refactor is to make Server 1 become an orchestrator only:

1. Validate request and auth.
2. Save/update session config in Postgres.
3. Create a row in `jobs` with queue `ingestion`, `dataframe`, `analysis`, or `graph`.
4. Return `202 Accepted` with `job_id`/`session_id`.
5. Let Server 3 claim and execute the job.
6. Send progress through job events / Socket.IO notifications.
7. Cleanup remains Server 4 responsibility.

## Migration Priority To Reduce API Pressure

| Priority | Move From API | Move To Worker Queue | Why |
|---:|---|---|---|
| 1 | `/live_batch_files` `id=stream` start_session/realtime ingestion | `analysis` or `ingestion` queue | Fixes API-memory zombie ingestions and removes long-running loops from API |
| 2 | `/live_batch_files` `id=create_DF` | `dataframe` queue | Dataframe/HDFS/Hive/Elastic work is heavy and file-oriented |
| 3 | `/api/STR_link_analysis` analyzer path | `analysis` queue | Currently does ES + dataframe + Neo4j analysis synchronously in API |
| 4 | `/live_batch_files` `id=search` Hive/Elastic fallback | `ingestion` or `dataframe` queue for large queries | Keeps simple strict search fast, moves broad fuzzy/Hive fallback off API |
| 5 | `/get_graph` graph fetch over large relationships | `graph` queue | Prevents long Neo4j fetch from blocking API; API can return partial/progress |
| 6 | Source preview in `/connect_to_source` | `ingestion` preview job | Lets connect endpoint only verify credentials and queue sample fetch |

## Suggested Near-Term Refactor Contract

For heavy actions, Server 1 should return:

```json
{
  "message": "accepted",
  "results": {
    "job_id": "...",
    "session_id": "1_811696",
    "queue": "analysis",
    "status": "queued"
  }
}
```

Server 3 updates `jobs` and `job_events`. Frontend/Socket.IO can poll or subscribe for job status.

## Operational Checks

Server 1:

```bash
sudo systemctl status linkx-api --no-pager
sudo journalctl -u linkx-api -n 100 --no-pager
curl -i http://127.0.0.1:8000/db/health
```

Server 2:

```bash
sudo docker compose -f /opt/linkx-control-data/docker-compose.yml ps
sudo docker exec -it linkx-postgres pg_isready -U linkx -d linkx
sudo docker exec -it linkx-postgres psql -U linkx -d linkx -c "select id, job_type, queue_name, status, created_at from jobs order by created_at desc limit 10;"
```

Server 3:

```bash
sudo systemctl status linkx-worker --no-pager
sudo journalctl -u linkx-worker -n 100 --no-pager
```

Server 4:

```bash
cd /opt/linkx-neo4j
sudo docker compose ps
sudo systemctl status linkx-cleanup-worker --no-pager
sudo systemctl status linkx-cleanup-scheduler --no-pager
sudo docker exec linkx-neo4j cypher-shell -u neo4j -p '<password>' "SHOW DATABASES;"
```

## Implemented First Migration Slice: Streaming To Worker

After this audit, the first load-distribution change was implemented locally:

- Server 1 API now has `enqueue_worker_job(...)` in `service_orchestration.py`.
- `POST /live_batch_files` with `id=stream` now queues a `start_session` job on the `analysis` queue when `LINKX_ASYNC_WORKER_JOBS` is enabled, which is the default.
- The API still returns a predictable log filename in `results`, plus `job_id`, `job`, `status: queued`, and `queued: true`.
- Server 3 worker `session_manager.start_session(...)` supports `run_inline: true`, so the analyzer runs inside the worker job process instead of spawning a detached API-memory thread.
- Worker job failure detection now treats returned `{status: "failed"}` / `{status: "error"}` as failed jobs.
- New API route `GET /jobs/<job_id>` returns authenticated job status and recent job events.

Deployment touches Server 1 API and Server 3 worker. No database migration is required because the existing `jobs` table is used.

## Implemented Second Migration Slice: Dataframe Creation To Worker

The second load-distribution change was implemented locally after streaming moved to the worker:

- `batch_manager/services/dataframe_workflow.py` now exposes `create_dataframe_result(...)`, which returns a plain JSON-able `(body, status)` tuple.
- `create_dataframe_response(...)` remains as the Flask wrapper for legacy synchronous API mode.
- Server 3 worker `linkx_worker.handlers` now supports job types `create_DF`, `create_dataframe`, and `dataframe`.
- `POST /live_batch_files` with `id=create_DF` now queues a `create_DF` job on the `dataframe` queue when `LINKX_ASYNC_WORKER_JOBS` is enabled, which is the default.
- The API returns `202 Accepted` with `message: accepted`, `results.job_id`, `results.status: queued`, and `results.queue: dataframe`.
- Frontend should poll `GET /jobs/<job_id>` or subscribe to job progress before assuming the dataframe is ready.

Deployment touches Server 1 API and Server 3 worker. No database migration is required.

## Implemented Third Migration Slice: STR Link Analysis To Worker

The third load-distribution change was implemented locally:

- `POST /api/STR_link_analysis` now validates/authenticates on Server 1, then queues `str_link_analysis` on the `analysis` queue when `LINKX_ASYNC_WORKER_JOBS` is enabled, which is the default.
- A new worker module `batch_manager/services/str_link_analysis_workflow.py` performs the old heavy sequence on Server 3: strict ES search, dataframe creation, Link Analysis ingestion, Source/Target relationship ingestion, and Neo4j summary.
- Server 3 worker `linkx_worker.handlers` now supports `str_link_analysis` / `STR_link_analysis` job types.
- API returns `202 Accepted` with `message: accepted`, `session_id`, `wait_for_prepare: true`, and `results.job_id`.
- Frontend/consumer should poll `GET /jobs/<job_id>` to collect the worker result instead of expecting the full STR analysis result synchronously.

Deployment touches Server 1 API and Server 3 worker. No database migration is required.

## Notes For A New Chat

- The active deployment is the service-factory split, not the root monolith.
- Be careful not to break current frontend contracts while migrating heavy routes.
- API port is `8000` on Server 1.
- Shared artifacts live under `/mnt/linkx-artifacts` with subdirs `uploads`, `dfparts`, `logs`, `rules`, `graphs`, `reports`, `configs`.
- Config is increasingly Postgres-backed via `session_configs` and `user_configs`; old `public/temp_config` should be considered legacy fallback only.
- Cleanup is event-driven plus scheduled. Explicit window close, source disconnect, tool disconnect, idle timeout, abandoned sessions, and cleanup scheduler should remove Neo4j/session/artifact footprints.
- AI access is now controlled through `/ai/...`, not through database credentials.

## 2026-06-22 Runtime Config Cleanup Note

Server 1 auth configuration was previously split across `.env`, direct unit `Environment=` lines, and a drop-in with legacy parent-token variables. That duplication caused confusion while validating the newer Parent project SSO flow.

Current intended configuration:
- The deployed service unit reads [EnvironmentFile=/opt/linkx-backend-api/.env](../service_factory/services/linkx-api/deploy/systemd/linkx-api.service).
- Parent project settings should live in one place, preferably `/opt/linkx-backend-api/.env`.
- Runtime code still accepts older compatibility aliases for direct-token verification, but the long-term contract should be the generic `LINKX_PARENT_*` OAuth/JWKS variables.
- `LINKX_ENABLE_LEGACY_PARENT_TOKEN=false` is the expected production value unless a rollback path is explicitly approved.
- `LINKX_PARENT_SHARED_SECRET` was removed from runtime and should stay absent unless a separate legacy integration is formally re-enabled.

Current behavior:
- Preferred SSO path is `POST /auth/exchange` or `POST /api/auth/exchange`, where LinkX exchanges an authorization code server-side, calls Parent project `userinfo`, upserts a local LinkX user, stores Parent project tokens encrypted, and returns a normal LinkX JWT.
- Direct-token fallback remains available at `POST /auth/parent-token` and expects a real ES256 access JWT verified through JWKS or the configured fallback public key. Placeholder strings, the PEM public key, or the JWKS public key itself correctly return `Invalid parent token` when submitted as `access_token`.
- Legacy shared-secret parent-token mode only runs when `LINKX_ENABLE_LEGACY_PARENT_TOKEN=true`, which should remain disabled in production.

Verification recommendation:
- Verify the final runtime env from the live process, not only `systemctl show`, because `EnvironmentFile=` values may not appear in `systemctl show linkx-api -p Environment`.
- Final browser SSO proof requires a real Parent project authorization code flow plus working token, userinfo, revoke, issuer, audience, and redirect-uri alignment from the Parent project side.
- Final direct-token proof requires a real Parent project ES256 access JWT with `alg=ES256`, `token_type=access`, future `exp`, and a valid `sub`.

## P0 Secret Hygiene Status

Code-side hardening now redacts sensitive job-event payloads, high-volume dataframe/search logs, and `/configuration` API responses. Worker/API Elastic logs no longer print raw search payloads, keywords, response bodies, or row data; dataframe routing logs avoid full local paths and DataFrame contents. Runtime defaults no longer fall back to the old hardcoded Neo4j password. Sensitive configuration writes such as `active_tool_password`, `tool_credentials.password`, tokens, secrets, and authorization headers now require `users:manage` and are audited by key path only, not by value.

Encrypted config-secret storage is now implemented for newly saved user/session configuration secrets. Sensitive values are stored in `managed_secrets.ciphertext` and config JSON keeps masked values plus `*_ref` IDs. API and worker services must share the same `LINKX_SECRET_ENCRYPTION_KEY` so API can store secrets and workers can decrypt them in memory.

Deployment requirements:

```bash
# Generate once, store securely, and set the same value on API + worker services
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
LINKX_SECRET_ENCRYPTION_KEY=<generated-fernet-key>
```

Existing raw secrets in `session_configs.config` were migrated after deploying `LINKX_SECRET_ENCRYPTION_KEY`. Verification showed `65` masked password rows, `65` password ref rows, and `0` possible raw password rows in `session_configs`; `managed_secrets` contained encrypted rows for `active_tool_password` and `tool_credentials.password`.

Live P0 rotation/completion status as of 2026-06-23:

1. PostgreSQL `linkx` password rotated and API `/db/health` verified.
2. Neo4j password rotated, API/worker/cleanup env files updated, encrypted config refs rotated, and `neo4j_residue_scan` verified as `succeeded`.
3. `LINKX_FLASK_SECRET_KEY` rotated; old LinkX-issued sessions/tokens were intentionally invalidated.
4. Built-in/admin password rotated; new login returned `200 OK` and old default password returned `401 invalid_credentials`.
5. Stale `LINKX_PARENT_SHARED_SECRET` removed from runtime; `LINKX_ENABLE_LEGACY_PARENT_TOKEN=false` and generic Parent project verification remains the only enabled parent-token path.
6. Live `.env` files on API, worker, and cleanup servers were set to `root:root` and `chmod 600`.
7. Parent project SSO now includes OAuth code exchange plus ES256/JWKS direct-token verification; final end-to-end proof still needs a real Parent project authorization code flow and ES256 access JWT from the Parent project team.

Recommended post-rotation verification:

```bash
# Server 1 / Server 3 / Server 4 as applicable
sudo find /opt -maxdepth 3 -name '.env' -exec ls -l {} \;
sudo journalctl -u linkx-api -u linkx-search-worker -u linkx-dataframe-worker -u linkx-analysis-worker -u linkx-graph-worker -n 300 --no-pager | grep -Ei 'password|secret|token|authorization|postgresql://|neo4j://.*@'
```

The grep should not show raw credential values. It may show safe field names or redacted `***` values.

## P1 Security Hardening Status

Request/body limits:
- API upload bodies are capped with `LINKX_MAX_UPLOAD_BYTES` through Flask `MAX_CONTENT_LENGTH`.
- JSON API requests have a separate smaller cap, `LINKX_MAX_JSON_BYTES`, defaulting to `2097152` bytes. Oversized JSON was verified to return `413 payload_too_large` before schema parsing.
- Set `LINKX_MAX_JSON_BYTES=0` only for controlled debugging.

Structured audit logging:
- `security_audit_events` was added for auth/admin/config/cleanup security events.
- Verified rows include successful and failed `auth.login` events without passwords, tokens, or request bodies.
- Admin endpoint: `GET /auth/admin/audit/security` with `users:manage`.
- Covered events include `auth.login`, `auth.service_token`, `auth.parent_token`, `admin.user.*`, `admin.service_account.*`, `admin.cleanup.request`, `config.user.save`, `config.session.save`, and `config.session.sensitive_update`.

Systemd hardening:
- Server 1 `linkx-api` uses `NoNewPrivileges=true`, `PrivateTmp=true`, `ProtectHome=true`, and `ProtectSystem=full`; `/db/health` verified after restart.
- Server 3 workers use `NoNewPrivileges=true`, `PrivateTmp=true`, and `ProtectHome=true`; `ProtectSystem` is intentionally not enabled yet because Spark/artifact writes need more compatibility testing.
- Server 4 cleanup worker/scheduler use `NoNewPrivileges=true`, `PrivateTmp=true`, and `ProtectHome=true`; cleanup dry-run scan verified after restart.

Firewall/exposure hardening:
- Server 2 UFW restricts Postgres `5432` and Redis `6379` to Server 1/API, Server 3/workers, and Server 4/cleanup.
- Server 4 UFW restricts Neo4j Bolt `7687` to API/worker/cleanup nodes and Neo4j Browser `7474` to the admin workstation.
- Server 1 UFW restricts API `8000` to frontend/Parent project/dev/admin/AI sources and allows NFS basics to worker/cleanup nodes.
- Post-firewall verification: cleanup `neo4j_residue_scan` succeeded, and Server 3 graph worker service-env Neo4j test returned `1`.

Current security caveats:
- CORS still includes the active frontend development origin; production target should remove dev-only origins and keep only the approved LinkX frontend and Parent project origins.
- SSH is still allowed broadly to avoid lockout during active hardening. Restrict SSH to VPN/admin IPs once operational access is confirmed.
- Server 1 NFS/RPC still shows random RPC listener ports. Pin NFS/rpcbind auxiliary ports before tightening Server 1 firewall further.
- Parent project SSO implementation is ready on the LinkX side, but final proof still waits on real Parent project OAuth exchange and ES256 access-token evidence.

## 2026-06-22 Parent Project Deployment Note

The current Server 1 Parent project rollout initially failed on startup because the deployed venv did not yet have PyJWT installed. The error surfaced as `ModuleNotFoundError: No module named 'jwt'` while importing [auth/tokens.py](../service_factory/services/linkx-api/src/auth/tokens.py).

Use `python -m pip`, not the missing `pip` entrypoint, when installing into the deployed venv:

```bash
cd /opt/linkx-backend-update
sudo git pull

sudo cp /opt/linkx-backend-update/service_factory/services/linkx-api/src/requirements.txt \
  /opt/linkx-backend-api/src/requirements.txt
sudo cp /opt/linkx-backend-update/service_factory/services/linkx-api/src/auth/jwks_client.py \
  /opt/linkx-backend-api/src/auth/jwks_client.py
sudo cp /opt/linkx-backend-update/service_factory/services/linkx-api/src/auth/tokens.py \
  /opt/linkx-backend-api/src/auth/tokens.py
sudo cp /opt/linkx-backend-update/service_factory/services/linkx-api/src/auth/routes.py \
  /opt/linkx-backend-api/src/auth/routes.py
sudo cp /opt/linkx-backend-update/service_factory/services/linkx-api/src/main.py \
  /opt/linkx-backend-api/src/main.py
sudo cp /opt/linkx-backend-update/service_factory/services/linkx-api/src/security/redaction.py \
  /opt/linkx-backend-api/src/security/redaction.py

sudo /opt/linkx-backend-api/.venv/bin/python -m pip install 'PyJWT>=2.8.0'

cd /opt/linkx-backend-api/src
sudo /opt/linkx-backend-api/.venv/bin/python -m py_compile \
  auth/jwks_client.py auth/tokens.py auth/routes.py main.py

sudo systemctl restart linkx-api
sudo systemctl status linkx-api --no-pager
sudo journalctl -u linkx-api -n 50 --no-pager
```

If the service is stuck in a restart loop, stop it before copying files and clear the failed state first:

```bash
sudo systemctl stop linkx-api
sudo systemctl reset-failed linkx-api
```

Use generic Parent project OAuth/JWKS configuration on Server 1. Prefer JWKS because it supports key rotation:

```bash
LINKX_PARENT_SSO_TOKEN_URL=https://<parent-host>/api/sso/token
LINKX_PARENT_SSO_USERINFO_URL=https://<parent-host>/api/sso/userinfo
LINKX_PARENT_SSO_REVOKE_URL=https://<parent-host>/api/sso/revoke
LINKX_PARENT_OAUTH_CLIENT_ID=<linkx-client-id>
LINKX_PARENT_OAUTH_CLIENT_SECRET=<server-side-client-secret>
LINKX_PARENT_OAUTH_REDIRECT_URI=https://<linkx-frontend-host>/auth/callback
LINKX_PARENT_OAUTH_ALLOWED_REDIRECT_URIS=https://<linkx-frontend-host>/auth/callback
LINKX_PARENT_JWKS_URL=https://<parent-host>/api/.well-known/jwks.json
LINKX_PARENT_JWT_JWKS_URL=https://<parent-host>/api/.well-known/jwks.json
LINKX_PARENT_JWT_ISSUER=<parent-issuer>
LINKX_PARENT_JWT_AUDIENCE=<parent-audience>
LINKX_PARENT_AUTH_ALLOWED_HOSTS=<parent-host>
LINKX_PARENT_AUTH_ALLOW_HTTP=false
LINKX_PARENT_JWKS_CACHE_SECONDS=300
LINKX_PARENT_FRAME_ORIGIN=https://<parent-host>
LINKX_AUTH_TOKEN_SECONDS=1800
LINKX_ENABLE_LEGACY_PARENT_TOKEN=false
LINKX_FRAME_OPTIONS=
LINKX_CONTENT_SECURITY_POLICY=default-src 'self'; frame-ancestors 'self' https://<parent-host>
```

If the Parent project also provides a matching ES256 public key, treat it only as a fallback verifier key. It is not a shared secret and not a token:

```text
-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEjM98cnQ+950yGc1dtiXRdFj0tsrt
+zfFs0gRGyJZfWagqcb/aW9oFmxgjikMJdA8AYsFARX8b+OZCqgNdkvjDQ==
-----END PUBLIC KEY-----
```

Fallback file path if needed:

```bash
LINKX_PARENT_JWT_PUBLIC_KEY_FILE=/etc/linkx/parent-es256-public.pem
```

The remaining Parent project dependency is live auth data for final proof: a real authorization code flow for `/auth/exchange` and a real ES256 access JWT for `/auth/parent-token`. Do not test with the public key itself; public keys correctly return `Invalid parent token` when submitted as `access_token`.


## 2026-06-19 Operations Update

This section captures the latest state after the shared-storage, Hadoop/HDFS, dataframe, cleanup, and Neo4j hardening work.

### Shared Artifact Storage Is Now Mandatory

`/mnt/linkx-artifacts` is no longer just a same-name local folder. It must be the same mounted filesystem across the services that create, consume, or clean artifacts.

| Server | Mount State | Why It Matters |
|---|---|---|
| Server 1 API `172.27.23.95` | NFS export host for `/mnt/linkx-artifacts` | API writes uploads, logs, rules, configs, and request-side artifacts |
| Server 3 Worker `172.27.23.18` | NFS client mounted at `/mnt/linkx-artifacts` | Worker reads uploads and writes dataframe parts |
| Server 4 Graph/Cleanup `172.27.23.85` | NFS client mounted at `/mnt/linkx-artifacts` | Cleanup removes the same files created by API/worker |

Verification commands:

```bash
findmnt /mnt/linkx-artifacts
df -h /mnt/linkx-artifacts
sudo find /mnt/linkx-artifacts/uploads -maxdepth 3 -type f | tail -20
```

Expected source on server 3 and server 4:

```text
172.27.23.95:/mnt/linkx-artifacts
```

The old worker-local backup may exist at:

```text
/mnt/linkx-artifacts.local-backup
```

Keep it briefly after the NFS migration, then remove it once upload/dataframe/cleanup behavior is verified.

### Hadoop And Search Configuration

The active Hadoop/data-processing host is:

```text
172.27.23.43
```

Important endpoint split:

| Purpose | Endpoint / Port | Notes |
|---|---|---|
| WebHDFS metadata/listing | `http://172.27.23.43:9870` | Used for raw HDFS file search/listing without Spark |
| HDFS RPC for Spark reads | `hdfs://172.27.23.43:9000` | Required by worker dataframe creation from selected HDFS files |
| Elastic/data-processing API | `http://172.27.23.43:5000` | Used for strict/fuzzy/hybrid search |
| Hive Metastore | `thrift://172.27.23.43:9083` | Used only if large-search backend is Hive |

Required `.env` values on Server 1 and Server 3:

```env
LINKX_ACTIVE_STORAGE_ADDRESS=172.27.23.43
LINKX_ACTIVE_STORAGE_HOST=172.27.23.43
LINKX_STORAGE_WEBHDFS_PORT=9870
LINKX_STORAGE_WEBHDFS_URL=http://172.27.23.43:9870
LINKX_HDFS_RPC_PORT=9000
LINKX_STORAGE_HDFS_URI=hdfs://172.27.23.43:9000
LINKX_HIVE_SERVER_HOST=172.27.23.43
LINKX_THRIFT_PORT=9083
LINKX_HIVE_METASTORE_URI=thrift://172.27.23.43:9083
LINKX_ELASTIC_API_BASE_URL=http://172.27.23.43:5000
```

Why `9000` matters: server 3 confirmed `172.27.23.43:8020` is refused, while `172.27.23.43:9000` accepts TCP. Existing `session_configs` created before this fix may still contain `hdfs://172.27.23.43:8020`; update those rows or create a fresh session.

### Search And Dataframe Status

The following source/dataframe paths are now working:

| Flow | Status | Notes |
|---|---|---|
| Realtime Kafka broker ingestion | Working | Runs from worker-side flow |
| Batch Kafka dataframe creation | Working | Worker creates parquet under shared `dfparts` |
| Batch API dataframe creation | Working | Uses configured API source path |
| Raw HDFS file search | Working | API lists through WebHDFS, no Spark on API |
| Dataframe from selected raw HDFS files | Working | Worker reads through `hdfs://172.27.23.43:9000` |
| Strict Elastic search | Working | Strict search sends clean payload without pagination keys |
| Fuzzy Elastic search | Working subject to data-processing API behavior | Large results can route to Elastic scroll or Hive based on config |
| Dataframe from strict/fuzzy/fused search results | Working after batch-size and endpoint fixes | Large Elastic batches capped at `10000` per request |
| Local file upload dataframe creation | Working after NFS mount | API writes uploads, worker reads same shared files |

Raw HDFS file search request shape:

```json
{
  "id": "search",
  "session_id": "1_815493",
  "value": {
    "keyword": "part-00001",
    "date": null,
    "hybrid": false,
    "offset": 0,
    "limit": 50
  }
}
```

Strict Elastic search request shape:

```json
{
  "id": "search",
  "session_id": "1_815493",
  "value": {
    "keyword": "1000558269034",
    "date": null,
    "hybrid": true,
    "offset": 0,
    "limit": 50,
    "strict_mood": true,
    "search_column": "accountno"
  }
}
```

Large fuzzy result behavior is configurable per user/session:

```json
{
  "large_search_backend": "elastic_scroll",
  "elastic_scroll_enabled": true,
  "elastic_scroll_limit": 1000000,
  "elastic_scroll_batch_size": 10000
}
```

`10000` is intentional because the downstream Elasticsearch/data-processing API rejects scroll/result-window batch sizes above the index limit.

### Cleanup Service Status

Server 4 cleanup services are active:

```text
linkx-cleanup-worker
linkx-cleanup-scheduler
```

The scheduler queues these cleanup types periodically:

| Cleanup Type | Mode | Purpose |
|---|---|---|
| `artifacts_expired` | destructive | Deletes expired filesystem artifacts registered in Postgres |
| `metadata_prune` | destructive metadata prune | Removes old deleted artifact metadata and old finished jobs |
| `abandoned_sessions` | destructive session cleanup | Cleans sessions inactive beyond `LINKX_ABANDONED_SESSION_MINUTES` |
| `neo4j_residue_scan` | dry-run/report-only | Reports unmanaged Neo4j data and inactive-session residue |

Cleanup verification query on server 2:

```bash
sudo docker exec -it linkx-postgres psql -U linkx -d linkx -c "
select cleanup_type, status, dry_run, summary, error_message, created_at, finished_at
from cleanup_runs
order by created_at desc
limit 10;
"
```

Current residue scanner result was clean:

```json
{
  "status": "clean",
  "unmanaged": {"nodes": 0, "relationships": 0},
  "inactive_session_residue": {"nodes": 0, "relationships": 0}
}
```

### Neo4j Ownership Hardening

Graph writes now stamp ownership metadata so cleanup can reliably identify LinkX-created data.

Required properties on managed graph records:

```text
created_by = linkx
linkx_managed = true
session_id
parent_session_id when session is window-scoped, e.g. 1_815493 -> 815493
run_id when available
batch_id when available
created_at or ownership_stamped_at
```

The analyzer now performs a post-write ownership stamping pass for both batch and realtime Neo4j writes. Logs to look for:

```text
Ownership metadata stamped
Realtime ownership metadata stamped
```

Cleanup is reliable when ingestion follows the LinkX flow and carries at least `session_id`, `parent_session_id`, `run_id`, or `batch_id`. The only true zombie risk is graph data inserted without ownership metadata. The report-only residue scanner exists to detect that condition before destructive cleanup is considered.

### NFS Export/Mount Commands Used

Server 1 exports the artifact root:

```bash
sudo mkdir -p /mnt/linkx-artifacts/{uploads,dfparts,logs,rules,graphs,reports,configs}
sudo chmod -R 775 /mnt/linkx-artifacts
sudo mkdir -p /etc/exports.d
echo "/mnt/linkx-artifacts 172.27.23.18(rw,sync,no_subtree_check,no_root_squash) 172.27.23.85(rw,sync,no_subtree_check,no_root_squash)" | sudo tee /etc/exports.d/linkx-artifacts.exports
sudo exportfs -ra
sudo exportfs -v
sudo systemctl enable --now nfs-kernel-server
```

Server 3 and Server 4 mount it:

```bash
sudo apt install -y nfs-common
sudo mkdir -p /mnt/linkx-artifacts
sudo mount -t nfs 172.27.23.95:/mnt/linkx-artifacts /mnt/linkx-artifacts
grep -q '172.27.23.95:/mnt/linkx-artifacts' /etc/fstab || \
echo "172.27.23.95:/mnt/linkx-artifacts /mnt/linkx-artifacts nfs defaults,_netdev,nofail 0 0" | sudo tee -a /etc/fstab
sudo systemctl daemon-reload
```


### Cleanup Neo4j Startup Retry

After reboot testing, cleanup sometimes reached Neo4j before Bolt/routing was fully ready even though the Docker container had started. Cleanup now warms up the Neo4j connection before running Neo4j cleanup or residue scanning.

Defaults:

```env
LINKX_NEO4J_RETRY_ATTEMPTS=6
LINKX_NEO4J_RETRY_DELAY_SECONDS=5
```

These values can remain unset because the code defaults are the same. This prevents transient post-reboot `Unable to retrieve routing information` failures from becoming cleanup audit noise unless Neo4j stays unavailable for roughly 30 seconds.

### Artifact And Cleanup Lifecycle Policy

Current rule: ingestion termination is not final session cleanup. Terminate/cancel must clean Neo4j residue for the current run, but must preserve reusable source-window state so the analyst can re-ingest without recreating the dataframe/source setup.

| Event | Cleanup type | Delete dfparts/uploads/session config? | Clean Neo4j? | Notes |
|---|---|---:|---:|---|
| Terminate ingestion from the UI | `neo4j_session` | No | Yes | Cleans all Neo4j graph residue for the source session, preserves dataframe artifacts for re-ingestion. |
| Cancelled ingestion job | `neo4j_session` | No | Yes | Worker cancellation cleanup must not enqueue `run` or destructive `session`. |
| Browser refresh / socket abandoned after grace period | non-final stop, `run`/`neo4j_session` behavior | No | Yes | A reconnect within grace preserves the work. |
| Re-ingest same source window | no destructive artifact cleanup | No | Previous graph/run residue may be cleaned first | Requires existing dfpart or a fresh `create_DF`. |
| Close source window | `window` | Yes | Yes | This is a final source-window lifecycle event. |
| Logout / max idle expiry | `session_tree` | Yes | Yes | Final user/session lifecycle cleanup. |
| Admin manual cleanup | `window`, `session`, or `session_tree` depending on scope | Yes | Yes | Explicit admin action. |
| Retention expiry | `artifacts_expired` | Yes, only expired active artifacts | No, unless paired with residue scan/cleanup | Prevents artifact pile-up and reduces security exposure. |

Implementation notes:

- `request_session_cancellation(..., cancel_session=False)` is the non-final ingestion stop path. It should only cancel ingestion/analysis jobs and schedule `neo4j_session` cleanup.
- Worker job cancellation cleanup now preserves dfparts, uploads, rules, and session config; it must enqueue `neo4j_session`, not `run` or destructive `session`, for a cancelled ingestion job.
- `window`, `session`, and `session_tree` are intentionally destructive and should be reserved for close-window, logout, max-idle expiry, or admin cleanup.
- `create_DF` registers dfparts with expiry metadata. Scheduled `artifacts_expired` cleanup is the safety net for old artifacts that were not removed by a final lifecycle event.
- Recommended retention policy: dfparts/uploads expire after 6-24 hours depending on analyst workflow; logs can remain 7-30 days; deleted metadata is pruned after `LINKX_METADATA_RETENTION_DAYS`.

Operational verification query for accidental destructive cleanup:

```sql
select cleanup_type, status, session_id, summary, created_at, finished_at
from cleanup_runs
where session_id = '<source_session_id>'
order by created_at desc
limit 20;
```

If ordinary terminate creates `cleanup_type = session` or deletes `/mnt/linkx-artifacts/dfparts/merged_dfpart_<session_id>`, that is a regression.

### Latest Mandatory Remaining Work

| Priority | Item | Reason | Status |
|---|---|---|---|
| 1 | End-to-end cleanup test after NFS | Prove uploads, dfparts, logs, and Neo4j data disappear after session/window cleanup | Done, `1_815493` cleanup removed files and Neo4j count was `0` |
| 2 | Reboot persistence test for server 3 and server 4 NFS mounts | Ensure worker/cleanup still see shared artifacts after restart/reboot | Done, both NFS mounts and services survived reboot |
| 3 | Cleanup Neo4j startup retry | Avoid transient post-reboot routing failures before Neo4j is fully ready | Done in cleanup task retry helper |
| 4 | Remove old local artifact backups | Avoid confusion and stale duplicate files after NFS migration | Done on server 3 and server 4 |
| 5 | Continue moving heavy API routes to worker jobs | API should remain orchestration/auth/socket layer only | Ongoing, get_graph relationship fetch now queues graph_fetch on worker; worker runner has generic stale/running job timeout recovery |
| 6 | Keep `neo4j_residue_scan` report-only until reviewed over time | Avoid deleting non-LinkX data accidentally | Active, report-only, currently clean |
| 7 | Eventually add an admin cleanup/audit UI endpoint for manual session cleanup and residue review | Makes operations safer for admins | API endpoints exist for cleanup/security audit; frontend UI still pending |
| 8 | Backup/restore evidence | Production recovery must be proven, not assumed | Postgres, artifacts, and Neo4j offline dump/load drills verified; encrypted off-host target and future non-empty graph drill still pending |

### Worker Timeout Recovery

Server 3 worker runner now protects all queues from stuck running jobs, not only graph fetches. Active child processes are terminated and marked failed when they exceed their configured timeout, and stale `running` rows left by dead workers are recovered on the next worker scan. Defaults are conservative and can be tuned per service:

```bash
WORKER_JOB_TIMEOUT_SECONDS_SEARCH=300
WORKER_JOB_TIMEOUT_SECONDS_GRAPH=300
WORKER_JOB_TIMEOUT_SECONDS_DATAFRAME=3600
WORKER_JOB_TIMEOUT_SECONDS_ANALYSIS=7200
WORKER_JOB_TIMEOUT_SECONDS_INGESTION=7200
WORKER_GRAPH_STALE_SECONDS=300
```

Use `0` only for controlled debugging; production workers should keep timeout recovery enabled so queued work cannot be blocked forever by a lost child process.


### P2 Backup And Restore Evidence

This is the current backup/restore baseline. Treat it as an operational control, not just documentation: every backup family needs a recent restore proof before it can be called complete.

| Data | Source | Backup Method | Restore Proof | Current Status |
|---|---|---|---|---|
| PostgreSQL control DB | Server 2 `linkx-postgres` | Custom-format `pg_dump` into `/opt/linkx-backups/postgres` | Restore into `linkx_restore_test`, verify key table counts, then drop test DB | Verified 2026-06-23 with checksum OK and restore drill passed |
| Shared artifacts | Server 1 `/mnt/linkx-artifacts` | Tar snapshot script into `/opt/linkx-backups/artifacts`; encrypted off-host target still preferred | Restore to an isolated empty directory and verify directory/file counts | Verified 2026-06-24 with checksum OK and restore drill passed; off-host target still pending |
| Neo4j graph DB | Server 4 `linkx-neo4j` | Maintenance-window offline `neo4j-admin database dump` to `/opt/linkx-backups/neo4j`; online backup not enabled yet | Restore to an isolated Neo4j container and run count/query smoke tests | Verified 2026-06-24 with checksum and isolated load; repeat after non-empty ingestion data |
| Redis queue/cache | Server 2 `linkx-redis` | Treat as disposable coordination/cache state while workers claim durable jobs from Postgres | Restart Redis empty; optionally capture RDB if future Redis queues become durable | Verified disposable on 2026-06-24: `DBSIZE=0`, AOF enabled but empty |
| Secret material | `.env` files and `LINKX_SECRET_ENCRYPTION_KEY` | Store in a protected secret manager/offline escrow, never in Git | Prove API can decrypt one managed secret after restore | Verified; escrow copy kept with developer, no secret values documented |

Suggested RPO/RTO target until the business sets formal values:

| Area | Draft RPO | Draft RTO | Notes |
|---|---:|---:|---|
| PostgreSQL | 24h, plus before deployments | 1h | Use daily scheduled dump and manual dump before schema/security changes. |
| Artifacts | 24h | 2-4h | Use incremental file backup where possible; artifact volume can grow quickly. |
| Neo4j | 24h or before graph schema changes | 2-4h | Restore time depends on graph size and whether offline dump is required. |
| Secrets | Immediate after rotation | 1h | Losing `LINKX_SECRET_ENCRYPTION_KEY` makes managed secret refs unrecoverable. |
| Redis | Disposable unless durable queues are moved into Redis | Minutes | Current worker queue is Postgres-backed; Redis can restart empty unless future code stores durable work there. |

Server 2 PostgreSQL backup:

```bash
cd /opt/linkx-backend-update/service_factory/services/linkx-control-data
sudo BACKUP_DIR=/opt/linkx-backups/postgres ./scripts/backup-postgres.sh
sudo ls -lh /opt/linkx-backups/postgres | tail
```

Server 2 PostgreSQL restore drill into an isolated test DB:

```bash
cd /opt/linkx-backend-update/service_factory/services/linkx-control-data
DUMP_FILE=/opt/linkx-backups/postgres/<backup-file>.dump
sudo POSTGRES_DB=linkx_restore_test ./scripts/restore-postgres.sh "$DUMP_FILE"
sudo docker exec -it linkx-postgres psql -U linkx -d linkx_restore_test -P pager=off -c "select count(*) from users;"
sudo docker exec -i linkx-postgres dropdb -U linkx --if-exists linkx_restore_test
```

Server 1 artifact snapshot and safe restore drill:

```bash
cd /opt/linkx-backend-update/service_factory
sudo BACKUP_DIR=/opt/linkx-backups/artifacts ./scripts/backup-artifacts.sh
sudo ls -lh /opt/linkx-backups/artifacts | tail

ARTIFACT_ARCHIVE=/opt/linkx-backups/artifacts/<backup-file>.tar.gz
sudo ./scripts/restore-artifacts-to-dir.sh "$ARTIFACT_ARCHIVE" /opt/linkx-restore-tests/artifacts_$(date -u +%Y%m%dT%H%M%SZ)
```

The restore script refuses `/`, `/mnt`, and `/mnt/linkx-artifacts` as targets and requires an empty restore directory. For large production artifact volume, prefer an encrypted off-host `rsync` target over repeated full tar archives. Keep backups off the same disk/host where possible.

Server 4 Neo4j backup method is confirmed as an offline maintenance-window dump. The live container must be stopped for consistency, cleanup services should be stopped while dumping, and the backup directory must be writable by the Neo4j container user (`7474:7474`) or the dump should be written through the tested helper script. Inspect mounts again before changing the method:

```bash
sudo docker inspect linkx-neo4j --format '{{json .Mounts}}' | python3 -m json.tool
```

Evidence to record after each restore drill:

| Field | Value |
|---|---|
| Backup file/path | `<path>` |
| SHA256 | `<sha256>` |
| Backup timestamp UTC | `<timestamp>` |
| Restore target | `<test database/path/container>` |
| Verification queries/checks | `<row counts, sample artifacts, Neo4j counts>` |
| Operator | `<name>` |
| Result | `passed` / `failed` |


Recorded Postgres restore evidence:

| Field | Value |
|---|---|
| Backup file/path | `/opt/linkx-backups/postgres/linkx_20260623T150431Z.dump` |
| SHA256 | `sha256sum -c` returned `OK` |
| Backup timestamp UTC | `2026-06-23T15:04:31Z` |
| Restore target | `linkx_restore_test` on Server 2 `linkx-postgres` |
| Verification queries/checks | `users=1`, `jobs=505`, `session_configs=70`, `managed_secrets=69` |
| Result | `passed`; test DB dropped after verification |


Recorded artifact restore evidence:

| Field | Value |
|---|---|
| Backup file/path | `/opt/linkx-backups/artifacts/linkx-artifacts_20260624T075512Z.tar.gz` |
| SHA256 | `sha256sum -c` returned `OK` |
| Backup timestamp UTC | `2026-06-24T07:55:12Z` |
| Restore target | `/opt/linkx-restore-tests/artifacts_20260624T075512Z` on Server 1 |
| Verification checks | restored `linkx-artifacts` tree with `configs`, `dfparts`, `graphs`, `logs`, `reports`, `rules`, and `uploads`; file count `3`, directory count `10`, size `12M` |
| Result | `passed`; restore was isolated from live `/mnt/linkx-artifacts` |


Recorded Neo4j restore evidence:

| Field | Value |
|---|---|
| Backup file/path | `/opt/linkx-backups/neo4j/20260624T092050Z/neo4j.dump` |
| SHA256 | `0b3cffa0627dba287ab6c9fe1cddae6a41be27fead99c2113a801ebaf896a3f6` |
| Backup timestamp UTC | `2026-06-24T09:20:50Z` |
| Backup method | Offline maintenance-window `neo4j-admin database dump neo4j`; live Neo4j and cleanup services stopped during dump, then restarted |
| Restore target | isolated Docker volume/container `linkx-neo4j-restore-test-20260624T092050Z` on ports `17474/17687` |
| Verification checks | `neo4j-admin database load` processed `309/309` files; restored container started; count queries returned `nodes=0`, `relationships=0`, matching pre-backup live counts |
| Result | `passed`; isolated restore container and volume removed after verification |
| Caveat | Current live graph was empty. Repeat this restore drill after a representative ingestion creates nonzero nodes/relationships. |


### P2 Secret Escrow And Decrypt Proof

`LINKX_SECRET_ENCRYPTION_KEY` protects the encrypted rows in `managed_secrets`. A successful Postgres restore is not enough without this key: the database can come back, but stored Neo4j/API/config secrets cannot be decrypted.

Rules:

- Do not commit `LINKX_SECRET_ENCRYPTION_KEY` to Git.
- Do not paste it into chat, tickets, screenshots, or runbooks.
- Store it in a real secret manager or offline escrow with access logging and at least two trusted recovery owners.
- Store it immediately after every rotation. The RPO for this key is effectively zero.

Server 1 fingerprint check without printing the key:

```bash
sudo sh -c "awk -F= '/^LINKX_SECRET_ENCRYPTION_KEY=/{print \$2}' /opt/linkx-backend-api/.env" | sha256sum
```

Server 3 worker fingerprint check should match Server 1:

```bash
sudo sh -c "awk -F= '/^LINKX_SECRET_ENCRYPTION_KEY=/{print \$2}' /opt/linkx-worker/.env" | sha256sum
```

Decrypt smoke test on Server 1 without printing any secret value:

```bash
cd /opt/linkx-backend-api/src
PID=$(systemctl show -p MainPID --value linkx-api)

sudo env $(sudo sh -c "tr '\0' '\n' < /proc/$PID/environ" | grep -E '^(DATABASE_URL|LINKX_POSTGRES_DSN|LINKX_SECRET_ENCRYPTION_KEY)=') \
  /opt/linkx-backend-api/.venv/bin/python -c '
import os
import psycopg
from security.secret_store import decrypt_secret

dsn = os.getenv("DATABASE_URL") or os.getenv("LINKX_POSTGRES_DSN")
if not dsn:
    raise SystemExit("missing database DSN")
with psycopg.connect(dsn) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            select id::text, secret_type, ciphertext
            from managed_secrets
            where deleted_at is null
            order by created_at desc
            limit 1
        """)
        row = cur.fetchone()
if not row:
    raise SystemExit("no managed secrets available to test")
secret_id, secret_type, ciphertext = row
plaintext = decrypt_secret(ciphertext)
if not plaintext:
    raise SystemExit("decrypt returned empty value")
print({"decrypt_ok": True, "secret_id": secret_id, "secret_type": secret_type, "plaintext_length": len(plaintext)})
'
```

Record evidence after the smoke test:

| Field | Value |
|---|---|
| Escrow location | kept separately with developer; no secret values documented here |
| Escrow owners | developer-held recovery copy |
| Server 1 key fingerprint | `2090083b86a828bb5934aef0b71092df686e64b915583e1da1049b5406035503` |
| Server 3 key fingerprint | `2090083b86a828bb5934aef0b71092df686e64b915583e1da1049b5406035503` |
| Decrypt smoke test | `decrypt_ok=True`, `secret_type=active_tool_password`, `plaintext_length=44`; secret id redacted |
| Result | decrypt proof passed, Server 1/Server 3 key fingerprints match, and recovery copy is kept separately with developer |


### P2 Redis Backup Policy

Current policy: Redis is disposable coordination/cache state. Durable jobs, job events, sessions, artifacts, cleanup records, users, and managed secrets are stored in PostgreSQL and artifact/graph storage, not Redis.

Operational check on Server 2:

```bash
sudo docker exec -it linkx-redis redis-cli INFO persistence
sudo docker exec -it linkx-redis redis-cli DBSIZE
sudo docker exec -it linkx-redis redis-cli --scan | head -50
```

If `DBSIZE` is near zero or keys are short-lived coordination/progress keys, no Redis backup is required. If future changes move durable queues or irreplaceable state into Redis, enable an RDB/AOF backup drill and add restore evidence here.

Optional one-off Redis RDB capture if needed later:

```bash
sudo mkdir -p /opt/linkx-backups/redis
sudo docker exec linkx-redis redis-cli BGSAVE
sudo docker cp linkx-redis:/data/dump.rdb /opt/linkx-backups/redis/dump_$(date -u +%Y%m%dT%H%M%SZ).rdb
sudo sha256sum /opt/linkx-backups/redis/*.rdb | tail
```


Recorded Redis policy evidence:

| Field | Value |
|---|---|
| Server | Server 2 `linkx-redis` |
| Persistence | `aof_enabled=1`, `aof_current_size=0`, `rdb_last_bgsave_status=ok` |
| Key count | `DBSIZE=0` |
| Scan sample | no keys returned |
| Result | no Redis backup required today; Redis can restart empty because durable state is Postgres/artifact/Neo4j-backed |


### P2 Scheduled Backups And Retention

Manual restore drills are proven. The next operational layer is scheduled local backups plus an off-host copy target. Timer templates and helper scripts are now in the repo, but they are not yet installed/enabled on the live servers.

| Server | Timer | Backup | Default Schedule | Retention |
|---|---|---|---|---|
| Server 2 | `linkx-postgres-backup.timer` | PostgreSQL custom dump | Daily `01:15 UTC` plus randomized delay | `14` days |
| Server 1 | `linkx-artifacts-backup.timer` | `/mnt/linkx-artifacts` tar snapshot | Daily `01:45 UTC` plus randomized delay | `14` days |
| Server 4 | `linkx-neo4j-backup.timer` | Offline Neo4j dump, stops Neo4j/cleanup briefly | Daily `02:30 UTC` plus randomized delay | `14` days |

Install on Server 2:

```bash
cd /opt/linkx-backend-update
sudo git pull
sudo cp service_factory/services/linkx-control-data/deploy/systemd/linkx-postgres-backup.service /etc/systemd/system/
sudo cp service_factory/services/linkx-control-data/deploy/systemd/linkx-postgres-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now linkx-postgres-backup.timer
systemctl list-timers 'linkx-postgres-backup*'
```

Install on Server 1:

```bash
cd /opt/linkx-backend-update
sudo git pull
sudo cp service_factory/deploy/systemd/linkx-artifacts-backup.service /etc/systemd/system/
sudo cp service_factory/deploy/systemd/linkx-artifacts-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now linkx-artifacts-backup.timer
systemctl list-timers 'linkx-artifacts-backup*'
```

Install on Server 4:

```bash
cd /opt/linkx-backend-update
sudo git pull
sudo cp service_factory/services/linkx-graph-maintenance/deploy/systemd/linkx-neo4j-backup.service /etc/systemd/system/
sudo cp service_factory/services/linkx-graph-maintenance/deploy/systemd/linkx-neo4j-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now linkx-neo4j-backup.timer
systemctl list-timers 'linkx-neo4j-backup*'
```

Optional off-host sync target:

```bash
sudo systemctl edit linkx-artifacts-backup.service
sudo systemctl edit linkx-neo4j-backup.service
```

Add an override when a real encrypted/off-host destination exists:

```ini
[Service]
Environment="LINKX_BACKUP_OFFHOST_TARGET=backup-user@backup-host:/srv/linkx-backups/<server-name>"
Environment="LINKX_BACKUP_RSYNC_OPTS=-aH --numeric-ids --delete --partial"
```

Do not enable off-host sync to an unencrypted or untrusted target. Until `LINKX_BACKUP_OFFHOST_TARGET` is set, the sync script exits cleanly and local backups continue.

Verification after timer install:

```bash
systemctl list-timers 'linkx-*backup*'
sudo journalctl -u linkx-postgres-backup -u linkx-artifacts-backup -u linkx-neo4j-backup --since today --no-pager
sudo find /opt/linkx-backups -maxdepth 3 -type f | sort | tail -50
```

Current status: timer templates and retention scripts are ready; live timer installation and off-host target are still pending.

### Quick Health Checklist

Server 1:

```bash
curl -i http://127.0.0.1:8000/db/health
sudo systemctl status linkx-api --no-pager
```

Server 2:

```bash
sudo docker compose -f /opt/linkx-control-data/docker-compose.yml ps
sudo docker exec -it linkx-postgres pg_isready -U linkx -d linkx
```

Server 3:

```bash
findmnt /mnt/linkx-artifacts
sudo systemctl status linkx-worker --no-pager
```

Server 4:

```bash
findmnt /mnt/linkx-artifacts
sudo systemctl status linkx-cleanup-worker --no-pager
sudo systemctl status linkx-cleanup-scheduler --no-pager
cd /opt/linkx-neo4j && sudo docker compose ps
```

