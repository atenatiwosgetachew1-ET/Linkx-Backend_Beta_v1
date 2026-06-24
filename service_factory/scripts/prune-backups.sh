#!/usr/bin/env bash
set -euo pipefail

BACKUP_ROOT="${BACKUP_ROOT:-/opt/linkx-backups}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"

if [ ! -d "${BACKUP_ROOT}" ]; then
  echo "backup root does not exist: ${BACKUP_ROOT}" >&2
  exit 0
fi

case "${BACKUP_ROOT}" in
  /|/opt|/opt/|/tmp|/tmp/)
    echo "refusing unsafe backup root: ${BACKUP_ROOT}" >&2
    exit 2
    ;;
esac

find "${BACKUP_ROOT}" -type f   \( -name '*.dump' -o -name '*.tar.gz' -o -name '*.rdb' -o -name '*.sha256' -o -name 'SHA256SUMS' \)   -mtime "+${RETENTION_DAYS}" -print -delete
find "${BACKUP_ROOT}" -type d -empty -mtime "+${RETENTION_DAYS}" -print -delete
