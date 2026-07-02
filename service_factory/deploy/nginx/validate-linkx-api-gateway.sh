#!/usr/bin/env bash
set -euo pipefail

TEMPLATE_PATH="${1:-$(dirname "$0")/linkx-api-gateway.conf}"
if ! command -v nginx >/dev/null 2>&1; then
  echo "nginx is required to validate ${TEMPLATE_PATH}" >&2
  exit 127
fi

TEMPLATE_ABS=$(readlink -f "$TEMPLATE_PATH")
WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

cat > "$WORK_DIR/nginx.conf" <<EOF
events {}
http {
    include $TEMPLATE_ABS;
}
EOF

nginx -t -c "$WORK_DIR/nginx.conf" -p "$WORK_DIR"
