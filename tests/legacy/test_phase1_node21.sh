#!/bin/bash
echo "=== Phase 1 Rule Centralization Test ==="

# Source environment
if [ -f "/opt/linkx-backend-update/.env" ]; then
    export $(cat /opt/linkx-backend-update/.env | grep -v '#' | xargs)
    echo "Loaded environment from /opt/linkx-backend-update/.env"
elif [ -f "/var/www/linkx-backend/.env" ]; then
    export $(cat /var/www/linkx-backend/.env | grep -v '#' | xargs)
    echo "Loaded environment from /var/www/linkx-backend/.env"
else
    echo "Warning: Could not find .env file. DB connection might fail."
fi

# Run the python test
cd /var/www/linkx-backend/service_factory/services/linkx-worker/src
python3 test_phase1_rules.py
