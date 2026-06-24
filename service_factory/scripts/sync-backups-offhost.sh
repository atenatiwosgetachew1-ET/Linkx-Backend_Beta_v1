#!/usr/bin/env bash
set -euo pipefail

BACKUP_ROOT="${BACKUP_ROOT:-/opt/linkx-backups}"
OFFHOST_TARGET="${LINKX_BACKUP_OFFHOST_TARGET:-}"
RSYNC_OPTS="${LINKX_BACKUP_RSYNC_OPTS:- -aH --numeric-ids --delete --partial}"

if [ -z "${OFFHOST_TARGET}" ]; then
  echo "LINKX_BACKUP_OFFHOST_TARGET is not set; skipping off-host backup sync"
  exit 0
fi

if [ ! -d "${BACKUP_ROOT}" ]; then
  echo "backup root does not exist: ${BACKUP_ROOT}" >&2
  exit 2
fi

rsync ${RSYNC_OPTS} "${BACKUP_ROOT}/" "${OFFHOST_TARGET%/}/"
