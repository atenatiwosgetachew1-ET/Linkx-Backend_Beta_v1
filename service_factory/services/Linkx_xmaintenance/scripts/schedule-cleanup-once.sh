#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../src"
set -a
. ../.env
set +a
exec ../.venv/bin/python -m linkx_xcleanup.scheduler --once ""
