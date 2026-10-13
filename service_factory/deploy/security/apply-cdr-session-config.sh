#!/usr/bin/env bash
# ==============================================================================
# Apply fresh CDR default session configuration to LinkX PostgreSQL database
# Usage: sudo bash apply-cdr-session-config.sh
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/seed-cdr-session-config.sql"

if [ ! -f "${SQL_FILE}" ]; then
  echo "ERROR: SQL file not found: ${SQL_FILE}"
  exit 1
fi

echo "Applying fresh CDR session configuration to PostgreSQL..."

if sudo docker ps --format '{{.Names}}' | grep -q "^linkx-postgres$"; then
  echo "Found Docker container: linkx-postgres"
  sudo docker exec -i linkx-postgres psql -U linkx -d linkx < "${SQL_FILE}"
elif command -v psql >/dev/null 2>&1; then
  echo "Running psql directly..."
  psql -U linkx -d linkx < "${SQL_FILE}"
else
  echo "ERROR: Neither linkx-postgres docker container nor psql CLI was found."
  exit 1
fi

echo "SUCCESS: Fresh CDR session configuration applied."
