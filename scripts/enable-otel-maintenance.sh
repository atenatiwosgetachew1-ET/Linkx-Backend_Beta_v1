#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "${SCRIPT_DIR}")"
VENV_PYTHON="/opt/Linkx_xmaintenance/.venv/bin/python"

echo "Configuring OpenTelemetry metrics on Server 4 (maintenance)..."
if [[ -f "${VENV_PYTHON}" ]]; then
    "${VENV_PYTHON}" -m pip install -q 'prometheus-client>=0.22.1' 2>/dev/null || true
fi

echo "Restarting cleanup worker..."
systemctl restart linkx-xcleanup-worker
echo "Waiting 3s for service startup..."
sleep 3

echo "Running verification..."
python3 "${REPO_DIR}/service_factory/deploy/security/verify-linkx-server.py" --role graph-maintenance
