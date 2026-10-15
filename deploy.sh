#!/usr/bin/env bash
# ==============================================================================
# LinkX Unified Deployment Script
#
# Usage:
#   sudo bash deploy.sh --role api
#   sudo bash deploy.sh --role worker
#   sudo bash deploy.sh --role graph-maintenance
#   sudo bash deploy.sh --role control-data
#   sudo bash deploy.sh --role api --rollback
#   sudo bash deploy.sh --role worker --yes          # skip confirmation
#
# This script replaces deploy-api.sh, deploy-worker.sh, deploy-maintenance.sh
# with a single role-aware entry point.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
GIT_COMMIT="$(git -C "${SCRIPT_DIR}" rev-parse --short HEAD 2>/dev/null || echo 'unknown')"
GIT_BRANCH="$(git -C "${SCRIPT_DIR}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo 'unknown')"
HOSTNAME_VAL="$(hostname)"
VERIFY_SCRIPT="${SCRIPT_DIR}/service_factory/deploy/security/verify-linkx-server.py"
BOOT_WAIT=4

# --- Defaults ---
ROLE=""
ROLLBACK=false
AUTO_YES=false
INSTALL_DEPS=false

# --- Parse Arguments ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --role)   ROLE="$2"; shift 2 ;;
        --rollback) ROLLBACK=true; shift ;;
        --yes|-y) AUTO_YES=true; shift ;;
        --install-deps) INSTALL_DEPS=true; shift ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [[ -z "${ROLE}" ]]; then
    echo "Error: --role is required."
    echo "Usage: sudo bash deploy.sh --role <api|worker|graph-maintenance|control-data>"
    exit 1
fi

# --- Role Configuration ---
case "${ROLE}" in
    api)
        SRC="${SCRIPT_DIR}/service_factory/services/linkx-api/src"
        DEST="/opt/linkx-backend-api/src"
        UNITS="linkx-api"
        VENV="/opt/linkx-backend-api/.venv"
        ;;
    worker)
        SRC="${SCRIPT_DIR}/service_factory/services/linkx-worker/src"
        DEST="/opt/linkx-worker/src"
        UNITS="linkx-worker"
        VENV="/opt/linkx-worker/.venv"
        ;;
    graph-maintenance)
        SRC="${SCRIPT_DIR}/service_factory/services/Linkx_xmaintenance/src"
        DEST="/opt/Linkx_xmaintenance/src"
        UNITS="linkx-xcleanup-worker linkx-xcleanup-scheduler"
        VENV="/opt/Linkx_xmaintenance/.venv"
        ;;
    control-data)
        SRC="${SCRIPT_DIR}/service_factory/services/linkx-control-data"
        DEST="/opt/linkx-control-data"
        UNITS=""
        VENV=""
        ;;
    *)
        echo "Error: Unknown role '${ROLE}'."
        echo "Valid roles: api, worker, graph-maintenance, control-data"
        exit 1
        ;;
esac

BACKUP_DIR="${DEST}.bak.${TIMESTAMP}"
LOG_FILE="/var/log/linkx-deploy.log"

# --- Helper Functions ---
log_deploy() {
    local msg="[${TIMESTAMP}] host=${HOSTNAME_VAL} role=${ROLE} commit=${GIT_COMMIT} $1"
    echo "${msg}"
    echo "${msg}" >> "${LOG_FILE}" 2>/dev/null || true
}

record_audit() {
    local result="$1"
    local failures="${2:-0}"
    local warnings="${3:-0}"
    # Attempt to record in PostgreSQL (non-fatal if DB is unreachable)
    local pg_dsn=""
    for env_file in "${DEST}/.env" "${DEST}/../.env"; do
        if [[ -f "${env_file}" ]]; then
            pg_dsn=$(grep -E '^(DATABASE_URL|LINKX_POSTGRES_DSN)=' "${env_file}" 2>/dev/null | head -1 | cut -d= -f2- | tr -d "'" | tr -d '"')
            [[ -n "${pg_dsn}" ]] && break
        fi
    done
    if [[ -n "${pg_dsn}" ]] && command -v psql &>/dev/null; then
        psql "${pg_dsn}" -c "
            INSERT INTO deployment_audit_log
                (hostname, role, git_commit, git_branch, domain, verify_result, failures, warnings, operator)
            VALUES
                ('${HOSTNAME_VAL}', '${ROLE}', '${GIT_COMMIT}', '${GIT_BRANCH}',
                 '$(grep -s LINKX_DOMAIN ${DEST}/../.env ${DEST}/.env 2>/dev/null | head -1 | cut -d= -f2 || echo "")',
                 '${result}', ${failures}, ${warnings}, '$(whoami)')
        " 2>/dev/null || true
    fi
}

# --- Rollback Mode ---
if [[ "${ROLLBACK}" == true ]]; then
    # Find the most recent backup
    LATEST_BACKUP=$(ls -1d "${DEST}.bak."* 2>/dev/null | sort -r | head -1)
    if [[ -z "${LATEST_BACKUP}" ]]; then
        echo "Error: No backup found for ${DEST}. Cannot rollback."
        exit 1
    fi
    log_deploy "ROLLBACK from ${LATEST_BACKUP}"
    rm -rf "${DEST}"
    mv "${LATEST_BACKUP}" "${DEST}"

    if [[ -n "${UNITS}" ]]; then
        systemctl restart ${UNITS}
        echo "Waiting ${BOOT_WAIT}s for service startup..."
        sleep ${BOOT_WAIT}
    fi

    if [[ -f "${VERIFY_SCRIPT}" ]]; then
        python3 "${VERIFY_SCRIPT}" --role "${ROLE}"
    fi
    log_deploy "ROLLBACK complete"
    record_audit "ROLLBACK" 0 0
    exit 0
fi

# --- Pre-Deploy: Source Validation ---
if [[ ! -d "${SRC}" ]]; then
    echo "Error: Source directory not found: ${SRC}"
    exit 1
fi

# --- Pre-Deploy: Diff Preview ---
if [[ -d "${DEST}" ]]; then
    echo ""
    echo "=== Deployment Diff Preview (${ROLE}) ==="
    echo "Source:  ${SRC}"
    echo "Target:  ${DEST}"
    echo "Commit:  ${GIT_COMMIT} (${GIT_BRANCH})"
    echo ""

    DIFF_COUNT=$(diff -rq "${SRC}" "${DEST}" 2>/dev/null | grep -c "^" || true)
    if [[ "${DIFF_COUNT}" -eq 0 ]]; then
        echo "No file differences detected. Target is already up to date."
        echo "Skipping deployment. Use --yes to force."
        [[ "${AUTO_YES}" != true ]] && exit 0
    else
        diff -rq "${SRC}" "${DEST}" 2>/dev/null | head -30 || true
        if [[ "${DIFF_COUNT}" -gt 30 ]]; then
            echo "... and $((DIFF_COUNT - 30)) more differences"
        fi
        echo ""
        echo "Total: ${DIFF_COUNT} file(s) differ"
    fi

    if [[ "${AUTO_YES}" != true ]]; then
        echo ""
        read -p "Proceed with deployment? [y/N] " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Deployment cancelled."
            exit 0
        fi
    fi
else
    echo "Target ${DEST} does not exist. Fresh deployment."
fi

# --- Backup Current Live Source ---
if [[ -d "${DEST}" ]]; then
    log_deploy "Creating backup: ${BACKUP_DIR}"
    cp -a "${DEST}" "${BACKUP_DIR}"

    # Keep only the 3 most recent backups
    ls -1d "${DEST}.bak."* 2>/dev/null | sort -r | tail -n +4 | xargs rm -rf 2>/dev/null || true
fi

# --- Deploy: Copy Files ---
log_deploy "Deploying ${SRC} -> ${DEST}"
if [[ "${ROLE}" == "control-data" ]]; then
    # Control-data: copy compose + config files (not a src/ copy)
    cp -r "${SRC}/." "${DEST}/"
else
    mkdir -p "${DEST}"
    cp -r "${SRC}/." "${DEST}/"
fi

# --- Deploy: Install New Dependencies (only when --install-deps is passed) ---
if [[ "${INSTALL_DEPS}" == true && -n "${VENV}" && -f "${DEST}/requirements.txt" ]]; then
    log_deploy "Checking pip dependencies..."
    "${VENV}/bin/python" -m pip install -q -r "${DEST}/requirements.txt" 2>/dev/null || true
fi

# --- Deploy: Restart Services ---
if [[ -n "${UNITS}" ]]; then
    log_deploy "Restarting: ${UNITS}"
    systemctl restart ${UNITS}
    echo "Waiting ${BOOT_WAIT}s for service startup..."
    sleep ${BOOT_WAIT}
fi

# --- Deploy: Control-Data Docker ---
if [[ "${ROLE}" == "control-data" ]]; then
    log_deploy "Updating Docker containers..."
    docker compose -f "${DEST}/docker-compose.yml" up -d
fi

# --- Verify ---
VERIFY_RESULT="SKIP"
FAILURES=0
WARNINGS=0
if [[ -f "${VERIFY_SCRIPT}" ]]; then
    log_deploy "Running verification..."
    VERIFY_OUTPUT=$(python3 "${VERIFY_SCRIPT}" --role "${ROLE}" 2>&1) || true
    echo "${VERIFY_OUTPUT}"

    # Parse summary line
    FAILURES=$(echo "${VERIFY_OUTPUT}" | grep -oP 'failures=\K[0-9]+' || echo "0")
    WARNINGS=$(echo "${VERIFY_OUTPUT}" | grep -oP 'warnings=\K[0-9]+' || echo "0")
    if [[ "${FAILURES}" -eq 0 ]]; then
        VERIFY_RESULT="PASS"
    else
        VERIFY_RESULT="FAIL"
    fi
fi

# --- Record Audit ---
record_audit "${VERIFY_RESULT}" "${FAILURES}" "${WARNINGS}"

# --- Summary ---
echo ""
echo "=== Deployment Complete ==="
echo "  Role:     ${ROLE}"
echo "  Commit:   ${GIT_COMMIT} (${GIT_BRANCH})"
echo "  Host:     ${HOSTNAME_VAL}"
echo "  Backup:   ${BACKUP_DIR}"
echo "  Result:   ${VERIFY_RESULT} (failures=${FAILURES} warnings=${WARNINGS})"
echo ""

if [[ "${FAILURES}" -gt 0 ]]; then
    echo "WARNING: Verification reported ${FAILURES} failure(s)."
    echo "To rollback: sudo bash deploy.sh --role ${ROLE} --rollback"
    exit 1
fi
