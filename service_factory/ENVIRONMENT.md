# Service Factory Environment

This factory is isolated from the live backend.

## Local Defaults

- Live backend: PORT=8000
- Factory backend: PORT=8100
- Factory PostgreSQL compose port: 55432
- Factory Redis compose port: 16379

## Local Python Environment

The factory virtual environment lives at service_factory/.venv.

Install dependencies from inside service_factory:

    .venv/bin/pip install -r requirements.txt

Run the factory API:

    cp .env.example .env
    .venv/bin/python -u main.py

## Docker Compose Factory Stack

Use the factory compose file so ports do not collide with the live backend:

    cp .env.example .env
    docker compose -f docker-compose.factory.yml up api postgres redis

## Planned Deployment Roles

1. linkx-api
   - Flask API
   - Socket.IO
   - auth/RBAC
   - session/job APIs

2. linkx-control-data
   - PostgreSQL
   - Redis

3. linkx-worker
   - ingestion workers
   - dataframe workers
   - analysis workers
   - graph-write workers

4. Linkx_xmaintenance
   - Neo4j
   - cleanup workers
   - retention scheduler

## Artifact Root

The future shared artifact root is configured with LINKX_ARTIFACT_ROOT=/mnt/linkx-artifacts.

This should eventually replace direct use of local public/temp_* paths.
