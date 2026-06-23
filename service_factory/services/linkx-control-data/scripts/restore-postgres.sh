#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 /path/to/linkx.dump" >&2
  exit 2
fi

dump_file="$1"
CONTAINER_NAME="${POSTGRES_CONTAINER:-linkx-postgres}"
POSTGRES_DB="${POSTGRES_DB:-linkx_restore_test}"
POSTGRES_USER="${POSTGRES_USER:-linkx}"

if [ ! -r "${dump_file}" ]; then
  echo "backup dump is not readable: ${dump_file}" >&2
  exit 2
fi

docker exec "${CONTAINER_NAME}" dropdb -U "${POSTGRES_USER}" --if-exists "${POSTGRES_DB}"
docker exec "${CONTAINER_NAME}" createdb -U "${POSTGRES_USER}" "${POSTGRES_DB}"
docker exec -i "${CONTAINER_NAME}" pg_restore -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" --clean --if-exists < "${dump_file}"

echo "restored ${dump_file} into ${POSTGRES_DB}"
