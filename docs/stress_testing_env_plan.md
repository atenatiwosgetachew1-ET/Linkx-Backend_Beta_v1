# LinkX Stress Testing Environment Plan

Last updated: 2026-07-06

## Purpose

This plan builds a repeatable stress-testing environment for the current four-server LinkX backend split and adds web-based visualization for test runs, server health, queue depth, and bottlenecks.

It is based on the current handoff state in:

- `docs/service_split_handoff_and_load_audit.md`
- `docs/security_hardening_handoff.md`
- `docs/api_gateway_config.md`

## Current Architecture Constraints

The active backend split is:

- Server 1 `node-19` `172.27.23.95`: `linkx-api`
- Server 2 `node-20` `172.27.23.106`: PostgreSQL + Redis
- Server 3 `node-21` `172.27.23.18`: `linkx-worker`
- Server 4 `node-22` `172.27.23.85`: Neo4j + cleanup services

Important constraints from the handoff:

- Browser traffic should continue to hit Server 1 only for LinkX product traffic.
- Heavy execution belongs on Server 3, not Server 1.
- Redis/Postgres/Neo4j are private east-west dependencies and should stay private.
- Security drift checks and backup verification already exist and should be extended, not bypassed.
- The frontend code is not in this repository, so dashboard visualization should be treated as an ops/admin web surface unless the separate frontend repo is later extended.

## Recommended Target Design

Use a three-layer stress-testing stack:

1. Load generation:
   - `k6` for API, auth, Socket.IO polling, and job lifecycle traffic
   - optional `Locust` only if browser-like multi-step analyst workflows need Python-level customization

2. Metrics collection:
   - `Prometheus` as the central metrics scraper
   - `node_exporter` on all four servers
   - service-specific exporters for PostgreSQL, Redis, and Neo4j
   - custom LinkX application metrics exposed by API, worker, and cleanup services

3. Visualization:
   - `Grafana` as the web UI for dashboards
   - admin-only access through private IP or strict nginx allow-list

## Deployment Topology

### Preferred topology

Add one small dedicated monitoring host outside the four production roles:

- Monitoring host:
  - Prometheus
  - Grafana
  - Alertmanager later
  - optional long-term metric retention
- Load generator host:
  - `k6`
  - optional `Locust`

Why this is preferred:

- avoids biasing performance numbers by running tests on production nodes
- keeps Grafana and Prometheus off the frontend-facing API host
- reduces security and capacity risk on Server 1

### Fallback topology

If no fifth host is available:

- run Prometheus + Grafana on Server 1 behind admin-only access
- run `k6` on Server 3 or an admin workstation, but not during production user activity

This works, but the measurements will be less clean and Server 1 becomes more operationally sensitive.

## Server-By-Server Install Plan

### Server 1: API

Install:

- `node_exporter`
- optional nginx exporter if nginx terminates traffic here
- LinkX API `/metrics` endpoint

Measure:

- request count, latency, status codes by route
- auth/login/service-token rates
- queued job creation rates
- Socket.IO connection count
- active request concurrency
- Python process CPU, RSS, file descriptors

Do not use Server 1 as the main load executor.

### Server 2: Control data

Install:

- `node_exporter`
- `postgres_exporter`
- `redis_exporter`

Measure:

- PostgreSQL connections, locks, slow queries, cache hit ratio
- `jobs` table queue depth by `queue_name` and `status`
- Redis memory, clients, command rates, auth failures
- disk IOPS and free space for database volumes

### Server 3: Worker

Install:

- `node_exporter`
- LinkX worker `/metrics` endpoint or Prometheus client sidecar inside the worker service

Measure:

- queued, running, succeeded, failed jobs by type
- job duration by queue and handler
- cancellation counts
- dataframe creation duration
- STR analysis duration
- worker CPU, memory, open files, local disk pressure

This server is the primary place to validate that heavy work has truly moved out of the API.

### Server 4: Graph and cleanup

Install:

- `node_exporter`
- Neo4j Prometheus metrics plugin or supported exporter
- cleanup service `/metrics` endpoint

Measure:

- Neo4j query latency
- transaction counts
- page cache hit ratio
- heap and store usage
- cleanup run duration and deletion counts
- Bolt connection counts

## Code Work Required In This Repository

The most important coding task is to add first-class application metrics instead of relying only on host exporters.

### 1. Add Prometheus metrics to Server 1 API

Suggested location:

- `service_factory/services/linkx-api/src/main.py`
- small helper module under `service_factory/services/linkx-api/src/observability/metrics.py`

Expose:

- `/metrics` on an internal-only route
- counters for request totals
- histograms for request latency
- counters for auth failures and rate-limit hits
- counters for job enqueue events by queue and route

### 2. Add Prometheus metrics to Server 3 worker

Suggested location:

- `service_factory/services/linkx-worker/src/linkx_worker`
- helper module under `service_factory/services/linkx-worker/src/observability/metrics.py`

Expose:

- jobs claimed, started, succeeded, failed, cancelled
- duration histograms per job type
- queue wait time histograms
- current in-progress jobs gauge

### 3. Add Prometheus metrics to Server 4 cleanup service

Suggested location:

- `service_factory/services/Linkx_xmaintenance/src/linkx_cleanup`

Expose:

- cleanup runs started/completed/failed
- cleanup duration histogram
- files removed, sessions cleaned, Neo4j entities removed

### 4. Add queue-depth SQL-backed metrics

Because LinkX already uses PostgreSQL-backed jobs, export:

- queued jobs by `queue_name`
- oldest queued job age
- failed jobs in last 15m / 1h

This can be done either:

- inside the API metrics endpoint via lightweight cached queries, or
- through a small dedicated metrics collector process on Server 2 or Server 3

Preferred: collector process or worker-side poller, so Server 1 stays light.

### 5. Add stress-test scripts to the repo

Create a test directory such as:

- `tests/stress/k6/`

Initial scripts:

- `auth_login.js`
- `init_session.js`
- `enqueue_stream.js`
- `enqueue_create_df.js`
- `enqueue_str_analysis.js`
- `poll_job_status.js`
- `graph_fetch.js`

Each script should:

- authenticate once and reuse tokens where appropriate
- tag results by route and queue
- support environment variables for host, users, concurrency, and duration
- avoid logging secrets

## Web Visualization Plan

Use Grafana as the main graphical web UI.

### Dashboard set 1: Executive health

Show:

- requests per second
- p95/p99 API latency
- queued jobs
- failed jobs
- worker throughput
- Neo4j latency
- PostgreSQL saturation

### Dashboard set 2: Server role dashboards

Create one dashboard per role:

- API dashboard
- control-data dashboard
- worker dashboard
- graph-maintenance dashboard

### Dashboard set 3: Stress run dashboard

Show:

- current test name
- VUs / concurrency
- success rate
- error rate
- p50/p95/p99 latency by route
- queue wait time
- end-to-end job completion time

### Dashboard set 4: Migration proof dashboard

This dashboard should prove the architecture is behaving correctly:

- Server 1 CPU stays stable while heavy jobs scale up
- Server 3 job execution increases with load
- Server 2 queue depth rises then drains predictably
- Server 4 Neo4j load correlates to analysis jobs, not random API spikes

## Security And Access Controls

The current handoff makes security constraints explicit, so the stress stack should follow them.

Rules:

- Keep Grafana and Prometheus off the public internet.
- Expose `/metrics` only on private interfaces, localhost, or through nginx allow-listing.
- Do not place database or Neo4j credentials inside dashboard JSON.
- Use read-only monitoring credentials for PostgreSQL, Redis, and Neo4j exporters.
- Extend `verify-linkx-server.py` later to confirm metrics endpoints/exporters are installed and locked down.
- Extend backup/drift docs after deployment so the monitoring stack becomes part of the documented secure state.

## Rollout Phases

### Phase 1: Foundation

1. Provision monitoring host and load-generator host, or approve the fallback topology.
2. Install Prometheus and Grafana.
3. Install `node_exporter` on Server 1-4.
4. Install PostgreSQL, Redis, and Neo4j exporters.
5. Confirm metrics are reachable only from the monitoring tier.

### Phase 2: Application instrumentation

1. Add Prometheus client library to API, worker, and cleanup service requirements.
2. Implement `/metrics` or internal metrics servers.
3. Add request, queue, and job metrics.
4. Add unit tests for metrics registration and endpoint availability.

### Phase 3: Stress scripts

1. Add `k6` scenarios covering login, init, enqueue, poll, and graph paths.
2. Create baseline profiles:
   - smoke
   - steady-state
   - spike
   - soak
3. Store results in Prometheus remote write, Influx, or Grafana-supported backend as needed.

### Phase 4: Dashboards

1. Build Grafana dashboards for API, worker, DB, Redis, Neo4j, and end-to-end load runs.
2. Add annotations for deployments and config changes.
3. Add threshold panels for queue age, p95 latency, worker failures, and Neo4j stress.

### Phase 5: Operationalization

1. Add a handoff/runbook for starting and stopping stress campaigns.
2. Add safe test data and non-production credentials.
3. Add drift checks for exporters and metrics exposure.
4. Add alerting after baseline thresholds are known.

## Suggested First Test Matrix

Run these in non-production first:

1. API smoke:
   - `GET /db/health`
   - `POST /auth/login`
   - `POST /init`

2. Queue orchestration:
   - `POST /live_batch_files` with `id=stream`
   - `POST /live_batch_files` with `id=create_DF`
   - `POST /api/STR_link_analysis`
   - `GET /jobs/<job_id>`

3. Graph load:
   - `POST /get_graph`

4. Soak:
   - 2-4 hour steady job enqueue and polling load

5. Spike:
   - sudden 5x concurrency against enqueue + job polling

Success criteria:

- Server 1 remains responsive and mostly orchestration-only
- Server 3 absorbs heavy compute growth
- queue backlog drains after spikes
- Neo4j and PostgreSQL stay within safe CPU, RAM, and disk thresholds
- no auth, secret, or drift-check regressions appear

## Recommended Order Of Implementation

1. Build metrics first.
2. Install exporters second.
3. Stand up Grafana and Prometheus third.
4. Add `k6` scenarios fourth.
5. Run baseline tests before any more architecture moves.
6. Compare results before and after future queue migrations.

## Concrete Deliverables

The first implementation pass should produce:

- repo metrics helpers for API, worker, and cleanup services
- `tests/stress/k6/` scripts
- Prometheus scrape config
- Grafana dashboard JSON exports
- server install runbook
- updated drift-check or monitoring validation script

## Key Recommendation

For this environment, the cleanest plan is:

- dedicated monitoring host for Prometheus + Grafana
- separate load-generator host running `k6`
- exporters on all four LinkX servers
- custom Prometheus metrics added to API, worker, and cleanup services
- Grafana as the web visualization layer

That approach matches the current split architecture, preserves the security posture described in the handoff, and gives a clear way to prove whether Server 1 is becoming a true orchestrator while Server 3 carries the heavy work.
