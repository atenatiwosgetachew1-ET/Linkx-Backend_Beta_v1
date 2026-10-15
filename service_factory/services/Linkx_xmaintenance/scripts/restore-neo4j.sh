#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 /path/to/neo4j.dump" >&2
  exit 2
fi

dump_path="$1"
CONTAINER_NAME="${NEO4J_CONTAINER_NAME:-linkx-neo4j}"
DATA_VOLUME="${NEO4J_DATA_VOLUME:-linkx-neo4j_neo4j_data}"
IMAGE="${NEO4J_IMAGE:-neo4j:enterprise}"
DATABASE="${NEO4J_DATABASE:-neo4j}"
CLEANUP_SERVICES="${LINKX_CLEANUP_SERVICES:-linkx-xcleanup-worker linkx-xcleanup-scheduler}"

if [ ! -r "${dump_path}" ]; then
  echo "Error: Backup dump file is not readable: ${dump_path}" >&2
  exit 2
fi

dump_dir="$(cd "$(dirname "${dump_path}")" && pwd)"
dump_file="$(basename "${dump_path}")"

# Verify checksum if SHA256SUMS exists in the dump directory
if [ -f "${dump_dir}/SHA256SUMS" ]; then
  echo "Verifying SHA256 checksum..."
  (cd "${dump_dir}" && sha256sum --check --ignore-missing SHA256SUMS) || {
    echo "Error: Checksum verification failed for ${dump_path}" >&2
    exit 1
  }
fi

start_services() {
  echo "Restarting services..."
  docker start "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  for svc in ${CLEANUP_SERVICES}; do
    systemctl start "${svc}" >/dev/null 2>&1 || true
  done
}
trap start_services EXIT

echo "Stopping maintenance worker & scheduler services..."
for svc in ${CLEANUP_SERVICES}; do
  systemctl stop "${svc}" >/dev/null 2>&1 || true
done

echo "Stopping Neo4j container: ${CONTAINER_NAME}..."
docker stop "${CONTAINER_NAME}" >/dev/null 2>&1 || true

echo "Restoring database '${DATABASE}' from dump ${dump_file}..."
docker run --rm \
  -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
  -v "${DATA_VOLUME}":/data \
  -v "${dump_dir}":/backups \
  "${IMAGE}" \
  neo4j-admin database load "${DATABASE}" --from-path=/backups --overwrite-destination=true

start_services
trap - EXIT

echo "Neo4j database '${DATABASE}' successfully restored from ${dump_path}"
