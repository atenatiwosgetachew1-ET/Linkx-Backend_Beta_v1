#!/usr/bin/env bash
set -euo pipefail

BACKUP_ROOT="${BACKUP_ROOT:-/opt/linkx-backups/neo4j}"
DATA_VOLUME="${NEO4J_DATA_VOLUME:-linkx-neo4j_neo4j_data}"
CONTAINER_NAME="${NEO4J_CONTAINER_NAME:-linkx-neo4j}"
IMAGE="${NEO4J_IMAGE:-neo4j:enterprise}"
DATABASE="${NEO4J_DATABASE:-neo4j}"
CLEANUP_SERVICES="${LINKX_CLEANUP_SERVICES:-linkx-xcleanup-worker linkx-xcleanup-scheduler}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"

BACKUP_TS="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_DIR="${BACKUP_ROOT}/${BACKUP_TS}"

mkdir -p "${BACKUP_DIR}"
# The neo4j image runs as uid/gid 7474, so the mounted host backup dir must be writable by that uid.
chown 7474:7474 "${BACKUP_DIR}"
chmod 775 "${BACKUP_DIR}"

start_services() {
  docker start "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  for svc in ${CLEANUP_SERVICES}; do
    systemctl start "${svc}" >/dev/null 2>&1 || true
  done
}
trap start_services EXIT

for svc in ${CLEANUP_SERVICES}; do
  systemctl stop "${svc}" >/dev/null 2>&1 || true
done
docker stop "${CONTAINER_NAME}"

docker run --rm \
  -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
  -v "${DATA_VOLUME}":/data \
  -v "${BACKUP_DIR}":/backups \
  "${IMAGE}" \
  neo4j-admin database dump "${DATABASE}" --to-path=/backups --overwrite-destination=true

start_services
trap - EXIT

chown -R root:root "${BACKUP_DIR}"
chmod 600 "${BACKUP_DIR}"/*
sha256sum "${BACKUP_DIR}"/*.dump > "${BACKUP_DIR}/SHA256SUMS"

if [ -n "${RETENTION_DAYS}" ] && [ "${RETENTION_DAYS}" != "0" ]; then
  find "${BACKUP_ROOT}" -mindepth 1 -maxdepth 1 -type d -mtime "+${RETENTION_DAYS}" -print -exec rm -rf {} +
fi

echo "${BACKUP_DIR}"
