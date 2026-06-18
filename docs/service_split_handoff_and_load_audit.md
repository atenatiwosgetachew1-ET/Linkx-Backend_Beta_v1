# LinkX Service Split Handoff And Load Audit

Last updated: 2026-06-18

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

## Notes For A New Chat

- The active deployment is the service-factory split, not the root monolith.
- Be careful not to break current frontend contracts while migrating heavy routes.
- API port is `8000` on Server 1.
- Shared artifacts live under `/mnt/linkx-artifacts` with subdirs `uploads`, `dfparts`, `logs`, `rules`, `graphs`, `reports`, `configs`.
- Config is increasingly Postgres-backed via `session_configs` and `user_configs`; old `public/temp_config` should be considered legacy fallback only.
- Cleanup is event-driven plus scheduled. Explicit window close, source disconnect, tool disconnect, idle timeout, abandoned sessions, and cleanup scheduler should remove Neo4j/session/artifact footprints.
- AI access is now controlled through `/ai/...`, not through database credentials.
