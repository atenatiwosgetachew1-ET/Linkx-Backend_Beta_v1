#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${SCRIPT_DIR}/service_factory/services/Linkx_xmaintenance/src"
DEST="/opt/Linkx_xmaintenance/src"

echo "Deploying Maintenance updates..."
cp -r "${SRC}/." "${DEST}/"
systemctl restart linkx-xcleanup-worker linkx-xcleanup-scheduler
echo "Waiting 4s for service startup..."
sleep 4

python3 "${SCRIPT_DIR}/service_factory/deploy/security/verify-linkx-server.py" --role graph-maintenance
