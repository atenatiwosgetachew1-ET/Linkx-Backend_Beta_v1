# LinkX Worker Service

This is the Server 3 bundle for ingestion, dataframe, analysis, and graph-write workers.

## Owns

- ingestion jobs
- dataframe jobs
- rule-analysis jobs
- final-analysis jobs
- graph-write jobs

## Does Not Own

- Flask routing
- auth/RBAC decision making
- PostgreSQL/Redis hosting
- Neo4j database hosting
- retention cleanup scheduling

## Current Runtime

The first worker runner polls the PostgreSQL jobs table with SELECT FOR UPDATE SKIP LOCKED. This gives us a deployable worker boundary immediately. Redis remains part of the service contract and can become the queue transport in the next step.

Supported job routing:

- queue ingestion: load_sourceData, search, merge, batch_data_manager jobs
- queue dataframe: batch_data_manager jobs
- queue analysis: analyzer, analysis, run_analysis jobs
- queue graph: analyzer jobs that write to Neo4j

## Local Setup

    cp .env.example .env
    python3 -m venv .venv
    .venv/bin/pip install -r src/requirements.txt

Run once:

    scripts/run-once.sh

Run continuously:

    cd src
    set -a; . ../.env; set +a
    ../.venv/bin/python -m linkx_worker.runner

Enqueue a test job:

    scripts/enqueue-job.sh --queue analysis --type analyzer --payload '{"id":"batch_data","type":"new"}'

## Server Deployment Shape

Recommended install path on Server 3:

    /opt/linkx-worker

Expected structure:

    /opt/linkx-worker/src
    /opt/linkx-worker/.env
    /opt/linkx-worker/.venv

## Next Refactor Step

Update Server 1 API routes so live_batch_files and STR routes insert jobs into PostgreSQL/Redis instead of calling analyzer or batch_data_manager directly.
