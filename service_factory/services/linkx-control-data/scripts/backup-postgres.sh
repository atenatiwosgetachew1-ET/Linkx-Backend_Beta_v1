#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-/opt/linkx-backups/postgres}"
CONTAINER_NAME="${POSTGRES_CONTAINER:-linkx-postgres}"
POSTGRES_DB="${POSTGRES_DB:-linkx}"
POSTGRES_USER="${POSTGRES_USER:-linkx}"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
out="${BACKUP_DIR}/linkx_${timestamp}.dump"

mkdir -p "${BACKUP_DIR}"

docker exec "${CONTAINER_NAME}" pg_dump -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -Fc > "${out}"
chmod 600 "${out}"

sha256sum "${out}" > "${out}.sha256"
echo "${out}"
