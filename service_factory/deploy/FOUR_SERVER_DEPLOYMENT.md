# Four-Server Backend Deployment Target

This factory is prepared around four backend roles, excluding the separate React frontend project.

## Server 1: linkx-api

Runs the API control plane.

Processes:
- Flask API
- Socket.IO gateway
- auth and dynamic RBAC
- validation
- session and job APIs

Important rule:
- API should enqueue work and query state. It should not run heavy analysis or cleanup jobs.

Factory local default:
- PORT=8100

## Server 2: linkx-control-data

Runs shared coordination data.

Processes:
- PostgreSQL
- Redis

Owns:
- users, roles, permissions
- sessions
- jobs
- job events
- artifact metadata
- queues and locks

## Server 3: linkx-worker

Runs heavy non-API work.

Processes:
- ingestion workers
- dataframe workers
- rule-analysis workers
- final-analysis workers
- graph-write workers

Important rule:
- Workers read job payloads from Redis/PostgreSQL and read/write artifacts through LINKX_ARTIFACT_ROOT.

## Server 4: Linkx_xmaintenance

Runs graph and lifecycle maintenance.

Processes:
- Neo4j
- cleanup workers
- retention scheduler

Owns:
- graph data
- Neo4j session/run cleanup
- expired artifact cleanup
- old log and temp cleanup

## Shared Artifact Storage

All runtime file bytes should move toward a shared artifact root.

Recommended startup path:
- LINKX_ARTIFACT_ROOT=/mnt/linkx-artifacts

Recommended layout:
- uploads/
- dfparts/
- logs/
- rules/
- configs/
- graphs/
- reports/

PostgreSQL should track artifact metadata. The shared storage should hold the actual file bytes.

## Migration Principle

Make service contracts first, then move servers.

Order:
1. Add job/session tables.
2. Add artifact registry and shared artifact root.
3. Add Redis queues.
4. Extract cleanup worker.
5. Extract analysis workers.
6. Run API stateless behind the frontend/reverse proxy.
