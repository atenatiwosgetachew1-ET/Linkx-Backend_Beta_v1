#!/usr/bin/env bash
set -euo pipefail
BACKUP_DIR=./backups
POSTGRES_DB=linkx
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_USER=linkx
mkdir -p ""
out="/linkx_20260609T082438Z.dump"
pg_dump -h "" -p "" -U "" -Fc "" -f ""
echo ""
