# LinkX Backend Service Factory

This folder is a working duplicate of the current backend source. Use it to
develop the service split while keeping the live backend layout unchanged.

## Target Backend Roles

1. `linkx-api`
   - Flask routes
   - Socket.IO gateway
   - auth and dynamic RBAC
   - request validation
   - session and job APIs

2. `linkx-control-data`
   - PostgreSQL for users, RBAC, sessions, jobs, artifacts, audit metadata
   - Redis for queues, locks, short-lived progress, and worker coordination

3. `linkx-worker`
   - ingestion workers
   - dataframe workers
   - rule-analysis workers
   - final-analysis workers
   - graph-write workers

4. `Linkx_xmaintenance`
   - Neo4j
   - cleanup workers
   - retention scheduler
   - Neo4j session/run cleanup

## First Refactor Goals

- Move job/session state away from process memory.
- Add a PostgreSQL-backed job model.
- Add an artifact registry for uploads, dataframe parts, rules, logs, reports,
  configs, and graph outputs.
- Introduce Redis queue boundaries before moving code onto separate servers.
- Extract cleanup into a worker process before extracting heavy analysis.

## Safety Rule

Do not change the production/live source path for service-split experiments.
Make architectural edits here first, then promote proven changes back deliberately.
## Future Recommendations To Preserve

- Keep service boundaries by role: API, control data, workers, graph maintenance.
- Make APIs stateless before scaling API nodes.
- Keep PostgreSQL as metadata/catalog, not file-byte storage.
- Keep artifact bytes in shared storage and track them in PostgreSQL.
- Use cleanup as a separate process/service, even when it runs on the same server as Neo4j.
- Keep cleanup dry-run capable and policy-driven.
- Move direct analyzer/session/cleanup calls to queued jobs before multi-node scale-out.
- For Neo4j, scale vertically first, then consider clustering/read replicas when the workload proves it.
