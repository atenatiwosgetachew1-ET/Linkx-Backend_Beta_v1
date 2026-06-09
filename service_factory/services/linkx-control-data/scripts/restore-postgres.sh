#!/usr/bin/env bash
set -euo pipefail
if [ 0 -ne 1 ]; then
  echo "usage: /bin/bash backup.dump" >&2
  exit 2
fi
POSTGRES_DB=linkx
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_USER=linkx
pg_restore -h "" -p "" -U "" -d "" --clean --if-exists ""
