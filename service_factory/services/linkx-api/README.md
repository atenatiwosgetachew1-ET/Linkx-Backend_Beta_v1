# LinkX API Service

This is the Server 1 service bundle for API / RBAC / Socket.IO. It is separated from the live backend and can be copied to the API server for deployment.

## Owns

- Flask routes
- Socket.IO gateway
- auth and dynamic RBAC
- request validation
- session/job APIs
- health checks

## Does Not Eventually Own

- ingestion execution
- rule-analysis execution
- final-analysis execution
- cleanup execution
- Neo4j maintenance

The current source bundle still includes batch_manager for compatibility because main.py, io_sockets.py, and the STR API import analysis helpers at module load time. The next refactor step is replacing direct analyzer/session execution with queue submissions to Server 3 workers.

## Local Smoke Run

    cp .env.example .env
    docker compose up --build

The service listens on port 8100 by default.

Health check:

    curl http://127.0.0.1:8100/db/health

## Systemd Deployment Shape

Recommended install path on Server 1:

    /opt/linkx-api

Expected structure:

    /opt/linkx-api/src
    /opt/linkx-api/.env
    /opt/linkx-api/.venv

Install dependencies:

    cd /opt/linkx-api/src
    python3 -m venv /opt/linkx-api/.venv
    /opt/linkx-api/.venv/bin/pip install -r requirements.txt

Run manually:

    cd /opt/linkx-api/src
    set -a; . /opt/linkx-api/.env; set +a
    /opt/linkx-api/.venv/bin/python -u main.py

## First Hardening Tasks

1. Move _session_store state to PostgreSQL/Redis.
2. Replace analyzer/session thread calls with Redis job enqueue calls.
3. Move file writes behind the artifact store.
4. Use Redis adapter or a notification service for multi-node Socket.IO.
