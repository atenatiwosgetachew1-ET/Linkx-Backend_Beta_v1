#!/usr/bin/env bash
set -euo pipefail

echo "=== [Node 19] Configuring LinkX API Gateway Service ==="

# 1. Copy service package
mkdir -p /opt/linkx-backend-api
cp -r /opt/linkx-backend-update/service_factory/services/linkx-api/. /opt/linkx-backend-api/
ln -sfn /opt/linkx-backend-api /opt/linkx-api

# 2. Write production .env
cat << 'ENVFILE' > /opt/linkx-backend-api/.env
PORT=8000
FLASK_ENV=production
PYTHONUNBUFFERED=1
LINKX_SECRET_ENCRYPTION_KEY=VrJVoT3RQ3Qvf1wgg0_Y_PX1sMGOufoMLeS8ZLS0Ebg=
LINKX_FLASK_SECRET_KEY=a6b6e536f073f984e1f1889312bb7de3f8faf5aab0bf18033c1a2424a0721160

# Data Plane Connections
DATABASE_URL=postgresql://linkx:tqvyTlaM_Dhl0mB6vrme9g@172.21.13.68:5432/linkx
LINKX_POSTGRES_DSN=postgresql://linkx:tqvyTlaM_Dhl0mB6vrme9g@172.21.13.68:5432/linkx
LINKX_REDIS_URL=redis://:Yp8R5KULTolW7wkllybkag@172.21.13.68:6379/0
LINKX_ARTIFACT_ROOT=/mnt/linkx-artifacts

# Neo4j Target on Server 4
LINKX_NEO4J_URL=bolt://172.21.13.67:7687
LINKX_ACTIVE_TOOL_PROTOCOL=bolt://172.21.13.67:7687
LINKX_NEO4J_USERNAME=neo4j
LINKX_NEO4J_PASSWORD=kMea_0CMcoWRIJEWKh-eyQ

# Auto-login admin mode and request limits
LINKX_AUTO_LOGIN_ADMIN=true
LINKX_CORS_ORIGINS=*
LINKX_MAX_JSON_BYTES=2097152
LINKX_ASYNC_WORKER_JOBS=true
ENVFILE

chmod 600 /opt/linkx-backend-api/.env
echo "[+] Wrote /opt/linkx-backend-api/.env successfully."

# 3. Setup Python venv (if not already created)
if [ ! -f "/opt/linkx-backend-api/.venv/bin/python" ]; then
    echo "[+] Creating Python virtual environment..."
    python3 -m venv /opt/linkx-backend-api/.venv
    /opt/linkx-backend-api/.venv/bin/pip install --upgrade pip
    /opt/linkx-backend-api/.venv/bin/pip install -r /opt/linkx-backend-api/src/requirements.txt
fi

# 4. Install and start systemd service
cp /opt/linkx-backend-api/deploy/systemd/linkx-api.service /etc/systemd/system/
systemctl daemon-reload
systemctl restart linkx-api
systemctl enable linkx-api

# 5. Configure Nginx reverse proxy
if [ -d "/etc/nginx/sites-available" ]; then
    rm -f /etc/nginx/sites-enabled/default
    rm -f /etc/nginx/sites-enabled/linkx-api-gateway.conf
    cp /opt/linkx-backend-update/service_factory/deploy/nginx/linkx-api-gateway.conf /etc/nginx/sites-available/linkx-api-gateway.conf
    # Allow matching any host header or IP
    sed -i 's/server_name linkx-api.local;/server_name _ linkx-api.local;/' /etc/nginx/sites-available/linkx-api-gateway.conf
    ln -sf /etc/nginx/sites-available/linkx-api-gateway.conf /etc/nginx/sites-enabled/linkx-api-gateway.conf
    nginx -t && systemctl reload nginx
fi

sleep 4
echo "[+] Checking linkx-api systemd service status:"
systemctl is-active linkx-api

echo "[+] Checking API health endpoint (http://127.0.0.1:8000/db/health):"
curl -s http://127.0.0.1:8000/db/health || echo "Healthcheck failed"
echo ""

echo "[+] Node 19 setup completed successfully!"
