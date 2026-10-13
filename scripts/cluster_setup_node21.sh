#!/usr/bin/env bash
set -euo pipefail

echo "=== [Node 21] Configuring LinkX Worker Service ==="

# 1. Mount NFS if not already mounted
mkdir -p /mnt/linkx-artifacts
if ! grep -qs '/mnt/linkx-artifacts' /proc/mounts; then
    mount -t nfs 172.21.13.70:/mnt/linkx-artifacts /mnt/linkx-artifacts || true
fi
grep -q '172.21.13.70:/mnt/linkx-artifacts' /etc/fstab || echo "172.21.13.70:/mnt/linkx-artifacts /mnt/linkx-artifacts nfs defaults,_netdev,nofail 0 0" >> /etc/fstab

# 2. Copy service package
mkdir -p /opt/linkx-worker
cp -r /opt/linkx-backend-update/service_factory/services/linkx-worker/. /opt/linkx-worker/

# 3. Write production .env
cat << 'ENVFILE' > /opt/linkx-worker/.env
PYTHONUNBUFFERED=1
LINKX_SECRET_ENCRYPTION_KEY=VrJVoT3RQ3Qvf1wgg0_Y_PX1sMGOufoMLeS8ZLS0Ebg=
LINKX_POSTGRES_DSN=postgresql://linkx:tqvyTlaM_Dhl0mB6vrme9g@172.21.13.68:5432/linkx
LINKX_REDIS_URL=redis://:Yp8R5KULTolW7wkllybkag@172.21.13.68:6379/0
LINKX_ARTIFACT_ROOT=/mnt/linkx-artifacts

# Neo4j Graph Target on Server 4
LINKX_NEO4J_URL=bolt://172.21.13.67:7687
LINKX_ACTIVE_TOOL_PROTOCOL=bolt://172.21.13.67:7687
LINKX_NEO4J_USERNAME=neo4j
LINKX_NEO4J_PASSWORD=kMea_0CMcoWRIJEWKh-eyQ

# Worker Timeouts & Limits
WORKER_JOB_TIMEOUT_SECONDS_SEARCH=300
WORKER_JOB_TIMEOUT_SECONDS_GRAPH=300
WORKER_JOB_TIMEOUT_SECONDS_DATAFRAME=3600
WORKER_JOB_TIMEOUT_SECONDS_ANALYSIS=7200
ENVFILE

chmod 600 /opt/linkx-worker/.env
echo "[+] Wrote /opt/linkx-worker/.env successfully."

# 4. Setup Python venv
echo "[+] Creating Python virtual environment..."
python3 -m venv /opt/linkx-worker/.venv
/opt/linkx-worker/.venv/bin/pip install --upgrade pip
/opt/linkx-worker/.venv/bin/pip install -r /opt/linkx-worker/src/requirements.txt

# 5. Install and start systemd service
cp /opt/linkx-worker/deploy/systemd/linkx-worker.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now linkx-worker

sleep 3
echo "[+] Checking linkx-worker status:"
systemctl is-active linkx-worker
echo "[+] Node 21 setup completed successfully!"
