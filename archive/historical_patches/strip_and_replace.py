import re

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'r') as f:
    content = f.read()

# The block starts at "# --- RULE GENERATORS (PHASE 1) ---"
# and ends before "def _create_transaction_indexes(session, label):"
pattern = r"# --- RULE GENERATORS \(PHASE 1\) ---.*?def _create_transaction_indexes\(session, label\):"

with open('/var/www/linkx-backend/generator_funcs_fixed.py', 'r') as f:
    new_gen = f.read()

replacement = "# --- RULE GENERATORS (PHASE 1) ---\n" + new_gen + "\n\ndef _create_transaction_indexes(session, label):"

new_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

with open('/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py', 'w') as f:
    f.write(new_content)
