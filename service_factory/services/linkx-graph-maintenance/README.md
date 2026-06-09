# LinkX Graph Maintenance Service

This is the Server 4 bundle for Neo4j plus cleanup/retention services.

## Owns

- Neo4j graph database deployment scaffold
- Neo4j session/run cleanup
- artifact cleanup
- expired artifact retention cleanup
- old metadata pruning
- cleanup scheduling

## Does Not Own

- Flask API routing
- auth/RBAC
- PostgreSQL/Redis hosting
- analysis worker execution

## Transitional Neo4j Setup

For now, Neo4j may live on a different server. Set these values in .env to point cleanup at the current remote graph server:

    LINKX_NEO4J_URL=neo4j://current-graph-host:7687
    LINKX_NEO4J_USERNAME=neo4j
    LINKX_NEO4J_PASSWORD=change-me

For final deployment, Neo4j is expected to run on this same server, usually at:

    LINKX_NEO4J_URL=neo4j://127.0.0.1:7687

## Local Setup

    cp .env.example .env
    python3 -m venv .venv
    .venv/bin/pip install -r src/requirements.txt

Run one cleanup job if any cleanup_runs are queued:

    scripts/run-cleanup-once.sh

Enqueue a session cleanup dry run:

    scripts/enqueue-cleanup.sh session --dry-run --payload '{"session_id":"123"}'

Schedule retention cleanup once:

    scripts/schedule-cleanup-once.sh --dry-run

## Docker Compose

Run Neo4j plus cleanup services:

    docker compose up -d

Run only Neo4j:

    docker compose up -d neo4j

## Cleanup Types

Supported cleanup_type values:

- session
- artifacts_expired
- artifacts_session
- neo4j_session
- metadata_prune

## Safety Rules

- Filesystem artifact deletion is constrained to LINKX_ARTIFACT_ROOT.
- Dry-run mode is supported through cleanup_runs.dry_run.
- Unsupported artifact storage backends are skipped instead of guessed.
- Neo4j cleanup requires explicit credentials. Missing credentials skip graph cleanup instead of failing file cleanup.

## Next Integration Step

Update Server 1 / Server 3 end-session flows so they enqueue cleanup_runs instead of spawning cleanup subprocesses.
