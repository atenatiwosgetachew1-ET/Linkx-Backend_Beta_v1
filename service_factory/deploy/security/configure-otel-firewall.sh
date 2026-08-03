#!/usr/bin/env bash
set -euo pipefail

# OpenTelemetry Firewall Configuration Helper
# Configures UFW to allow inbound metrics traffic on port 8889 strictly from the monitoring server.

MONITORING_IP="${1:-172.27.23.36}"
OTEL_PORT="${2:-8889}"

echo "=========================================================="
echo "  LinkX OpenTelemetry Firewall Configuration (Phase 1)    "
echo "=========================================================="
echo "Target Collector / Monitoring IP: ${MONITORING_IP}"
echo "OpenTelemetry Metrics Port:       ${OTEL_PORT}/tcp"
echo "----------------------------------------------------------"

if command -v ufw >/dev/null 2>&1; then
    echo "[+] Applying UFW rule for ${MONITORING_IP}:${OTEL_PORT}..."
    sudo ufw allow from "${MONITORING_IP}" to any port "${OTEL_PORT}" proto tcp comment "OpenTelemetry metrics to collector ${MONITORING_IP}"
    
    echo "[+] Reloading UFW firewall..."
    sudo ufw reload
    
    echo "[+] Verifying current UFW rules for port ${OTEL_PORT}:"
    sudo ufw status verbose | grep "${OTEL_PORT}" || true
    echo "----------------------------------------------------------"
    echo "[SUCCESS] Firewall rule successfully configured."
else
    echo "[WARN] 'ufw' command not found. Ensure port ${OTEL_PORT} is opened for ${MONITORING_IP} in your firewall settings."
fi
