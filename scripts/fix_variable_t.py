import sys

RULES_FILE = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py"

with open(RULES_FILE, "r") as f:
    content = f.read()

# The exact block to replace in both queries
old_block = """    CALL {{
        MATCH (x:{label})
        WHERE ({scope_clause_t})
          AND coalesce(x.IGNORE_LOGICAL, false) = false
          AND x.ACCOUNTNO IS NOT NULL AND x.ACCOUNTNO <> ''
        WITH x.ACCOUNTNO AS account, count(x) AS outgoing_count"""

new_block = """    CALL {{
        MATCH (t:{label})
        WHERE ({scope_clause_t})
          AND coalesce(t.IGNORE_LOGICAL, false) = false
          AND t.ACCOUNTNO IS NOT NULL AND t.ACCOUNTNO <> ''
        WITH t.ACCOUNTNO AS account, count(t) AS outgoing_count"""

# Replace occurrences
new_content = content.replace(old_block, new_block)

if old_block in content and old_block not in new_content:
    print("Successfully replaced x with t in subqueries.")
else:
    print("Error: Block not found or replacement failed.")

with open(RULES_FILE, "w") as f:
    f.write(new_content)
