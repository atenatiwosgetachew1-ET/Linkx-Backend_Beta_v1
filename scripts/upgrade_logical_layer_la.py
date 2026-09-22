import re

file_path = "/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py"
with open(file_path, "r") as f:
    content = f.read()

# 1. Inject Logical Layer into batch_graph_analysis_transactions
logical_layer_code_batch = """
        # ----------------------------
        # 0.5 LOGICAL TRANSACTION LAYER INITIALIZATION
        # ----------------------------
        session.run(f'''
        MATCH (t:{label})
        WHERE ($session_id IS NULL OR {_session_scope_clause("t")})
        SET t.LOGICAL_ACCOUNTNO = coalesce(t.ACCOUNTNO, ''),
            t.LOGICAL_BENACCOUNTNO = coalesce(t.BENACCOUNTNO, ''),
            t.IGNORE_LOGICAL = false
        ''', session_id=session_param)
        
        if pass_through_accounts:
            session.run(f'''
            MATCH (inbound:{label})-[r:EFFECTIVE_FLOW]->(outbound:{label})
            WHERE ($session_id IS NULL OR {_session_scope_clause("inbound")})
            SET inbound.LOGICAL_BENACCOUNTNO = coalesce(outbound.BENACCOUNTNO, ''),
                outbound.IGNORE_LOGICAL = true
            ''', session_id=session_param)
        log_writer(log_file, f"[{datetime.now()}] [Info] Logical Layer initialized")
"""

eff_flow_end_batch = content.find("log_writer(log_file, f\"[{datetime.now()}] [Info] EFFECTIVE_FLOW rule completed\")")
if eff_flow_end_batch != -1:
    insert_pos = content.find("\n", eff_flow_end_batch) + 1
    content = content[:insert_pos] + logical_layer_code_batch + content[insert_pos:]

# 2. Inject Logical Layer into incremental_graph_analysis_transactions
logical_layer_code_inc = """
        # ----------------------------
        # 0.5 LOGICAL TRANSACTION LAYER INITIALIZATION
        # ----------------------------
        session.run(f'''
        MATCH (t:{label})
        WHERE t.batch_id = $batch_id
        SET t.LOGICAL_ACCOUNTNO = coalesce(t.ACCOUNTNO, ''),
            t.LOGICAL_BENACCOUNTNO = coalesce(t.BENACCOUNTNO, ''),
            t.IGNORE_LOGICAL = false
        ''', batch_id=batch_id)
        
        if pass_through_accounts:
            session.run(f'''
            MATCH (inbound:{label})-[r:EFFECTIVE_FLOW]->(outbound:{label})
            WHERE inbound.batch_id = $batch_id
            SET inbound.LOGICAL_BENACCOUNTNO = coalesce(outbound.BENACCOUNTNO, ''),
                outbound.IGNORE_LOGICAL = true
            ''', batch_id=batch_id)
        log_writer(log_file, f"[{datetime.now()}] [Info] Logical Layer initialized")
"""

eff_flow_end_inc = content.find("log_writer(log_file, f\"[{datetime.now()}] [Info] Running incremental transaction analysis for batch {batch_id}\")")
if eff_flow_end_inc != -1:
    # Wait, effective flow is after that in incremental!
    eff_flow_end_inc = content.find("r.directed_display = true\n            \"\"\", session_id=session_param, batch_id=batch_id, pass_through_accounts=pass_through_accounts)", eff_flow_end_inc)
    if eff_flow_end_inc != -1:
        insert_pos = content.find("\n", eff_flow_end_inc) + 1
        content = content[:insert_pos] + logical_layer_code_inc + content[insert_pos:]


# Replace ACCOUNTNO with LOGICAL_ACCOUNTNO only in the Smurfing and subsequent rules
# We know rules start at "1. SMURFING:"
rules_parts = content.split("# 1. SMURFING:")
if len(rules_parts) == 3: # 1 for batch, 1 for incremental
    for i in range(1, 3):
        rules_section = rules_parts[i]
        
        # Stop at the end of the transaction analysis function
        end_idx = rules_section.find("def ")
        if end_idx == -1: end_idx = len(rules_section)
        
        target = rules_section[:end_idx]
        remainder = rules_section[end_idx:]
        
        target = target.replace("t.ACCOUNTNO", "t.LOGICAL_ACCOUNTNO")
        target = target.replace("a.ACCOUNTNO", "a.LOGICAL_ACCOUNTNO")
        target = target.replace("b.ACCOUNTNO", "b.LOGICAL_ACCOUNTNO")
        target = target.replace("seed.ACCOUNTNO", "seed.LOGICAL_ACCOUNTNO")
        target = target.replace("all_t.ACCOUNTNO", "all_t.LOGICAL_ACCOUNTNO")
        
        target = target.replace("t.BENACCOUNTNO", "t.LOGICAL_BENACCOUNTNO")
        target = target.replace("a.BENACCOUNTNO", "a.LOGICAL_BENACCOUNTNO")
        target = target.replace("b.BENACCOUNTNO", "b.LOGICAL_BENACCOUNTNO")
        target = target.replace("seed.BENACCOUNTNO", "seed.LOGICAL_BENACCOUNTNO")
        target = target.replace("all_t.BENACCOUNTNO", "all_t.LOGICAL_BENACCOUNTNO")
        
        target = re.sub(r"MATCH \(t:\{label\}\)\n\s+WHERE \(\$session_id IS NULL OR \{_session_scope_clause\(\"t\"\)\}\)", lambda m: m.group(0) + "\n          AND coalesce(t.IGNORE_LOGICAL, false) = false", target)
        target = re.sub(r"MATCH \(a:\{label\}\), \(b:\{label\}\)\n\s+WHERE \(\$session_id IS NULL OR \(\{_session_scope_clause\(\"a\"\)\} AND \{_session_scope_clause\(\"b\"\)\}\)\)", lambda m: m.group(0) + "\n          AND coalesce(a.IGNORE_LOGICAL, false) = false AND coalesce(b.IGNORE_LOGICAL, false) = false", target)
        
        # Incremental specific matching:
        target = re.sub(r"MATCH \(t:\{label\}\)\n\s+WHERE \{_session_scope_clause\(\"t\"\)\}", lambda m: m.group(0) + "\n          AND coalesce(t.IGNORE_LOGICAL, false) = false", target)
        target = re.sub(r"MATCH \(a:\{label\}\), \(b:\{label\}\)\n\s+WHERE \{_session_scope_clause\(\"a\"\)\} AND \{_session_scope_clause\(\"b\"\)\}", lambda m: m.group(0) + "\n          AND coalesce(a.IGNORE_LOGICAL, false) = false AND coalesce(b.IGNORE_LOGICAL, false) = false", target)
        
        rules_parts[i] = target + remainder

    content = rules_parts[0] + "# 1. SMURFING:" + rules_parts[1] + "# 1. SMURFING:" + rules_parts[2]

# Fix trusted entry matching logic to fall back to raw fields if logical doesn't match
content = content.replace(
    "def _trusted_entry_match(alias):", 
    "def _trusted_entry_match(alias):\n    # Support matching against both raw and logical fields"
)
content = content.replace(
    "toString(coalesce({alias}[k], \"\"))",
    "toString(coalesce({alias}['LOGICAL_' + k], {alias}[k], \"\"))"
)

with open(file_path, "w") as f:
    f.write(content)

print("Updated successfully")
