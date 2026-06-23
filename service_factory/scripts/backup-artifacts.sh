#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_ROOT="${LINKX_ARTIFACT_ROOT:-/mnt/linkx-artifacts}"
BACKUP_DIR="${BACKUP_DIR:-/opt/linkx-backups/artifacts}"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
out="${BACKUP_DIR}/linkx-artifacts_${timestamp}.tar.gz"

if [ ! -d "${ARTIFACT_ROOT}" ]; then
  echo "artifact root does not exist: ${ARTIFACT_ROOT}" >&2
  exit 2
fi

mkdir -p "${BACKUP_DIR}"

tar --xattrs --acls --one-file-system -czf "${out}" -C "$(dirname "${ARTIFACT_ROOT}")" "$(basename "${ARTIFACT_ROOT}")"
chmod 600 "${out}"
sha256sum "${out}" > "${out}.sha256"

echo "${out}"
