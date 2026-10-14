import re

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'r') as f:
    text = f.read()

# We need to replace the Phase 1 generators with the v3 generators that support incremental_batch_id
# For the sake of safety, let's just edit SMURFING and CIRCULAR_FLOW to support incremental_batch_id for now.
