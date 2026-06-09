#!/usr/bin/env bash
set -euo pipefail
POSTGRES_USER=linkx
POSTGRES_DB=linkx
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
pg_isready -h "" -p "" -U "" -d ""
redis-cli -h "" -p "" ping
