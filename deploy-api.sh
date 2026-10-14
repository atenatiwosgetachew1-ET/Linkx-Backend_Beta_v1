#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${SCRIPT_DIR}/service_factory/services/linkx-api/src"
DEST="/opt/linkx-backend-api/src"

echo "Deploying API updates..."
cp -r "${SRC}/." "${DEST}/"
systemctl restart linkx-api
echo "Waiting 4s for service startup..."
sleep 4

python3 "${SCRIPT_DIR}/service_factory/deploy/security/verify-linkx-server.py" --role api
