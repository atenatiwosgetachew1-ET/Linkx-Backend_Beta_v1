import sys

RULES_FILE = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py"

with open(RULES_FILE, "r") as f:
    content = f.read()

# Replace the exact line in get_circular_flow_query
old_block = """
    else:
        seed_block = \"\"\"
        MATCH (seed:{label})
        WHERE ({scope_clause_t})
"""

new_block = """
    else:
        scope_clause_seed = scope_clause_t.replace("t.", "seed.")
        seed_block = \"\"\"
        MATCH (seed:{label})
        WHERE ({scope_clause_seed})
"""

content = content.replace(old_block, new_block)

old_format = '""".format(label=label, scope_clause_t=scope_clause_t)'
new_format = '""".format(label=label, scope_clause_seed=scope_clause_seed)'

content = content.replace(old_format, new_format)

with open(RULES_FILE, "w") as f:
    f.write(content)

print("Seed block scope fixed.")
