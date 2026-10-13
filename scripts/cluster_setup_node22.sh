#!/usr/bin/env bash
set -euo pipefail

echo "=== [Node 22] Configuring LinkX Graph & Maintenance Service ==="

# 1. Ensure directories exist
mkdir -p /opt/Linkx_xmaintenance/neo4j/import /opt/Linkx_xmaintenance/neo4j/plugins

# 2. Write exact production .env without formatting errors
cat << 'ENVFILE' > /opt/Linkx_xmaintenance/.env
PYTHONUNBUFFERED=1
DATABASE_URL=postgresql://linkx:tqvyTlaM_Dhl0mB6vrme9g@172.21.13.68:5432/linkx
LINKX_POSTGRES_DSN=postgresql://linkx:tqvyTlaM_Dhl0mB6vrme9g@172.21.13.68:5432/linkx
LINKX_REDIS_URL=redis://:Yp8R5KULTolW7wkllybkag@172.21.13.68:6379/0
LINKX_ARTIFACT_ROOT=/mnt/linkx-artifacts

LINKX_NEO4J_URL=bolt://127.0.0.1:7687
LINKX_NEO4J_USERNAME=neo4j
LINKX_NEO4J_PASSWORD=kMea_0CMcoWRIJEWKh-eyQ
LINKX_CLEANUP_NEO4J_URL=bolt://127.0.0.1:7687
LINKX_CLEANUP_NEO4J_USERNAME=neo4j
LINKX_CLEANUP_NEO4J_PASSWORD=kMea_0CMcoWRIJEWKh-eyQ
LINKX_SECRET_ENCRYPTION_KEY=VrJVoT3RQ3Qvf1wgg0_Y_PX1sMGOufoMLeS8ZLS0Ebg=

NEO4J_AUTH=neo4j/kMea_0CMcoWRIJEWKh-eyQ
NEO4J_server_memory_heap_initial__size=4G
NEO4J_server_memory_heap_max__size=4G
NEO4J_server_memory_pagecache_size=3G

CLEANUP_WORKER_NAME=linkx-xcleanup-1
CLEANUP_POLL_INTERVAL=5
CLEANUP_SCHEDULE_INTERVAL_SECONDS=3600
CLEANUP_DRY_RUN=false
CLEANUP_ARTIFACT_LIMIT=500
LINKX_METADATA_RETENTION_DAYS=30
LINKX_UPLOAD_RETENTION_DAYS=7
LINKX_DFPART_RETENTION_DAYS=3
LINKX_LOG_RETENTION_DAYS=30
LINKX_CONFIG_RETENTION_DAYS=7
LINKX_GRAPH_RETENTION_DAYS=14
ENVFILE

chmod 600 /opt/Linkx_xmaintenance/.env
echo "[+] Wrote /opt/Linkx_xmaintenance/.env successfully."

# 3. Restart Neo4j with updated credentials
cd /opt/Linkx_xmaintenance
docker compose up -d neo4j

# 4. Restart maintenance services
echo "[+] Restarting maintenance services..."
systemctl restart linkx-xcleanup-worker linkx-xcleanup-scheduler linkx-xvigilance

sleep 3
echo "[+] Checking cleanup-worker status:"
systemctl is-active linkx-xcleanup-worker
echo "[+] Node 22 setup completed successfully!"
