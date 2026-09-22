import re

file_path = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py"
with open(file_path, "r") as f:
    content = f.read()

# 1. Inject the Logical Layer initialization block right after EFFECTIVE_FLOW
logical_layer_code = """
        # ---- 0.5. LOGICAL TRANSACTION LAYER ----
        try:
            with driver.session() as s:
                s.run(f'''
                MATCH (t:{label})
                WHERE ($session_id IS NULL OR t.session_id = $session_id)
                SET t.LOGICAL_ACCOUNTNO = coalesce(t.ACCOUNTNO, ''),
                    t.LOGICAL_BENACCOUNTNO = coalesce(t.BENACCOUNTNO, ''),
                    t.IGNORE_LOGICAL = false
                ''', session_id=sp)
                
                if pass_through_accounts:
                    s.run(f'''
                    MATCH (inbound:{label})-[r:EFFECTIVE_FLOW]->(outbound:{label})
                    WHERE ($session_id IS NULL OR inbound.session_id = $session_id)
                    SET inbound.LOGICAL_BENACCOUNTNO = coalesce(outbound.BENACCOUNTNO, ''),
                        outbound.IGNORE_LOGICAL = true
                    ''', session_id=sp)
            rules_completed.append("LOGICAL_LAYER")
            print("  [Rule] LOGICAL_LAYER ✓", flush=True)
        except Exception as e:
            rules_failed.append(("LOGICAL_LAYER", str(e)[:100]))
            print(f"  [Rule] LOGICAL_LAYER ✗ {str(e)[:100]}", flush=True)
"""

# Find where EFFECTIVE_FLOW ends
eff_flow_end = content.find("print(f\"  [Rule] EFFECTIVE_FLOW ✓ (skipped: no pass-through accounts configured)\", flush=True)")
if eff_flow_end != -1:
    insert_pos = content.find("\n", eff_flow_end) + 1
    content = content[:insert_pos] + logical_layer_code + content[insert_pos:]
else:
    print("Could not find EFFECTIVE_FLOW end")
    exit(1)

# Now, apply substitutions ONLY from the end of the Logical Layer block onwards
rules_start = content.find("# ---- 1. SMURFING ----")

header = content[:rules_start]
rules_section = content[rules_start:]

# Replace .ACCOUNTNO with .LOGICAL_ACCOUNTNO
rules_section = rules_section.replace("t.ACCOUNTNO", "t.LOGICAL_ACCOUNTNO")
rules_section = rules_section.replace("a.ACCOUNTNO", "a.LOGICAL_ACCOUNTNO")
rules_section = rules_section.replace("b.ACCOUNTNO", "b.LOGICAL_ACCOUNTNO")
rules_section = rules_section.replace("inbound.ACCOUNTNO", "inbound.LOGICAL_ACCOUNTNO")
rules_section = rules_section.replace("outbound.ACCOUNTNO", "outbound.LOGICAL_ACCOUNTNO")
rules_section = rules_section.replace("all_t.ACCOUNTNO", "all_t.LOGICAL_ACCOUNTNO")

# Replace .BENACCOUNTNO with .LOGICAL_BENACCOUNTNO
rules_section = rules_section.replace("t.BENACCOUNTNO", "t.LOGICAL_BENACCOUNTNO")
rules_section = rules_section.replace("a.BENACCOUNTNO", "a.LOGICAL_BENACCOUNTNO")
rules_section = rules_section.replace("b.BENACCOUNTNO", "b.LOGICAL_BENACCOUNTNO")
rules_section = rules_section.replace("inbound.BENACCOUNTNO", "inbound.LOGICAL_BENACCOUNTNO")
rules_section = rules_section.replace("outbound.BENACCOUNTNO", "outbound.LOGICAL_BENACCOUNTNO")
rules_section = rules_section.replace("all_t.BENACCOUNTNO", "all_t.LOGICAL_BENACCOUNTNO")

# Add ignore_logical filter to all rules.
# For each rule query, find "MATCH (t:{label})" and append the ignore filter
def inject_ignore(match):
    return match.group(0) + "\n                  AND coalesce(t.IGNORE_LOGICAL, false) = false"

rules_section = re.sub(r"MATCH \(t:\{label\}\)\n\s+WHERE \(\$session_id IS NULL OR t\.session_id = \$session_id\)", inject_ignore, rules_section)
rules_section = re.sub(r"MATCH \(all_t:\{label\}\)\n\s+WHERE all_t\.LOGICAL_ACCOUNTNO = acc AND coalesce\(toString\(all_t\.TRANSACTIONDATE\), ''\) <> ''", lambda m: m.group(0) + " AND coalesce(all_t.IGNORE_LOGICAL, false) = false", rules_section)
rules_section = re.sub(r"MATCH \(a:\{label\} \{LOGICAL_BENACCOUNTNO: acc\}\)\n\s+WHERE \(\$session_id IS NULL OR a\.session_id = \$session_id\)", lambda m: m.group(0) + " AND coalesce(a.IGNORE_LOGICAL, false) = false", rules_section)
rules_section = re.sub(r"MATCH \(b:\{label\} \{LOGICAL_ACCOUNTNO: acc\}\)\n\s+WHERE \(\$session_id IS NULL OR b\.session_id = \$session_id\)", lambda m: m.group(0) + " AND coalesce(b.IGNORE_LOGICAL, false) = false", rules_section)

# Also fix the _trusted_entry_match function to use LOGICAL_ACCOUNTNO
header = header.replace(
    "def _trusted_entry_match(alias):", 
    "def _trusted_entry_match(alias):\n    # Re-map alias to use logical fields for matching\n    logical_alias = f'{{alias}}'\n"
)
header = header.replace(
    "toString(coalesce({alias}[k], \"\"))",
    "toString(coalesce({alias}['LOGICAL_' + k], {alias}[k], \"\"))"
)

with open(file_path, "w") as f:
    f.write(header + rules_section)
print("Updated successfully")
