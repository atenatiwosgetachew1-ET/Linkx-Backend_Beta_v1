#!/usr/bin/env bash
set -euo pipefail
PORT="8100"
curl -fsS "http://127.0.0.1:/db/health"
