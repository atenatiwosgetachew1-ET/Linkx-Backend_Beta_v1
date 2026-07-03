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

TEMPLATE_TEST="$WORK_DIR/linkx-api-gateway.conf"
sed -E 's/^[[:space:]]*listen[[:space:]]+80;/    listen 127.0.0.1:18080;/' "$TEMPLATE_ABS" > "$TEMPLATE_TEST"

cat > "$WORK_DIR/nginx.conf" <<EOF
pid $WORK_DIR/nginx.pid;
error_log $WORK_DIR/error.log;
events {}
http {
    access_log $WORK_DIR/access.log;
    include $TEMPLATE_TEST;
}
EOF

nginx -t -c "$WORK_DIR/nginx.conf" -p "$WORK_DIR"
