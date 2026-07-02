#!/usr/bin/env bash
set -euo pipefail

BACKUP_ROOT="${BACKUP_ROOT:-/opt/linkx-backups}"
OFFHOST_TARGET="${LINKX_BACKUP_OFFHOST_TARGET:-}"
RSYNC_OPTS="${LINKX_BACKUP_RSYNC_OPTS:- -aH --numeric-ids --delete --partial}"
SSH_KEY="${LINKX_BACKUP_SSH_KEY:-}"
SSH_OPTS="${LINKX_BACKUP_SSH_OPTS:- -o BatchMode=yes -o StrictHostKeyChecking=accept-new}"

if [ -z "${OFFHOST_TARGET}" ]; then
  echo "LINKX_BACKUP_OFFHOST_TARGET is not set; skipping off-host backup sync"
  exit 0
fi

if [ ! -d "${BACKUP_ROOT}" ]; then
  echo "backup root does not exist: ${BACKUP_ROOT}" >&2
  exit 2
fi

read -r -a rsync_opts <<< "${RSYNC_OPTS}"
rsync_cmd=(rsync "${rsync_opts[@]}")

if [ -n "${SSH_KEY}" ]; then
  if [ ! -r "${SSH_KEY}" ]; then
    echo "backup SSH key is not readable: ${SSH_KEY}" >&2
    exit 2
  fi
  rsync_cmd+=(-e "ssh -i ${SSH_KEY} ${SSH_OPTS}")
fi

"${rsync_cmd[@]}" "${BACKUP_ROOT}/" "${OFFHOST_TARGET%/}/"
