import re

RULES_FILE = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py"

with open(RULES_FILE, "r") as f:
    content = f.read()

# Replace the signature of get_circular_flow_query to absorb the consumer's arguments
old_signature = """def get_circular_flow_query(
    label,
    scope_clause_t,
    trusted_pair_clause,
    is_provisional=False,
    incremental_batch_id=None
):"""

new_signature = """def get_circular_flow_query(
    label,
    scope_clause_t,
    trusted_pair_clause,
    is_provisional=False,
    incremental_batch_id=None,
    scope_clause_a=None,
    scope_clause_b=None,
    boundary_clause=None,
    **kwargs
):"""

content = content.replace(old_signature, new_signature)

with open(RULES_FILE, "w") as f:
    f.write(content)

print("Signature updated successfully.")
