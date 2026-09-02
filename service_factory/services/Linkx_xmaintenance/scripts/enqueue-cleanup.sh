#!/usr/bin/env bash
set -euo pipefail
cd ""/bin/../src"
set -a
. ../.env
set +a
exec ../.venv/bin/python -m linkx_cleanup.enqueue_cleanup ""
