# LinkX Service Split Handoff And Load Audit

Last updated: 2026-06-19

This document is a handoff summary for a new chat/session. It captures the current four-server LinkX backend split, how the services communicate, how deployments are updated, and what work is still running on each server. The goal of the split is to keep the current working system stable while gradually moving heavy analysis and ingestion work out of the API server and into worker/maintenance services.

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

### Latest Mandatory Remaining Work

| Priority | Item | Reason |
|---|---|---|
| 1 | End-to-end cleanup test after NFS | Prove uploads, dfparts, logs, and Neo4j data disappear after session/window cleanup |
| 2 | Reboot persistence test for server 3 and server 4 NFS mounts | Ensure worker/cleanup still see shared artifacts after restart/reboot |
| 3 | Continue moving heavy API routes to worker jobs | API should remain orchestration/auth/socket layer only |
| 4 | Keep `neo4j_residue_scan` report-only until reviewed over time | Avoid deleting non-LinkX data accidentally |
| 5 | Eventually add an admin cleanup/audit UI endpoint for manual session cleanup and residue review | Makes operations safer for admins |

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

