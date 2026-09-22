import re

def fix_rules(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # 1. Fix RAPID_WITHDRAWAL logic and pass-through protection
    # Consumer (double braces)
    old_rapid_c = """                WHERE out_count < 1000

                MATCH (a:{label} {{BENACCOUNTNO: acc}})
                WHERE ($session_id IS NULL OR a.session_id = $session_id)
                CALL (a, acc) {{
                  MATCH (b:{label} {{ACCOUNTNO: acc}})"""
                  
    new_rapid_c = """                WHERE out_count < 1000 AND NOT acc IN $pt

                MATCH (a:{label} {{LOGICAL_BENACCOUNTNO: acc}})
                WHERE ($session_id IS NULL OR a.session_id = $session_id)
                CALL (a, acc) {{
                  MATCH (b:{label} {{LOGICAL_ACCOUNTNO: acc}})"""
    content = content.replace(old_rapid_c, new_rapid_c)
    
    # LA Rules (single braces)
    old_rapid_la = """                WHERE out_count < 1000

                MATCH (a:{label} {BENACCOUNTNO: acc})
                WHERE {_session_scope_clause('a')}
                CALL (a, acc) {
                  MATCH (b:{label} {ACCOUNTNO: acc})"""
                  
    new_rapid_la = """                WHERE out_count < 1000 AND NOT acc IN $pt

                MATCH (a:{label} {LOGICAL_BENACCOUNTNO: acc})
                WHERE {_session_scope_clause('a')}
                CALL (a, acc) {
                  MATCH (b:{label} {LOGICAL_ACCOUNTNO: acc})"""
    content = content.replace(old_rapid_la, new_rapid_la)

    # 2. Fix ACCOUNT_ACTIVITY_SPIKE pass-through protection
    old_spike = """                WITH t.LOGICAL_ACCOUNTNO AS acc, t.TRANSACTIONDATE AS tx_day, count(t) AS daily_count, collect(t) AS day_txns
                WHERE daily_count >= $activity_spike_min_daily_count"""
    
    new_spike = """                WITH t.LOGICAL_ACCOUNTNO AS acc, t.TRANSACTIONDATE AS tx_day, count(t) AS daily_count, collect(t) AS day_txns
                WHERE daily_count >= $activity_spike_min_daily_count AND NOT acc IN $pt"""
    content = content.replace(old_spike, new_spike)
    
    with open(file_path, "w") as f:
        f.write(content)

fix_rules("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
fix_rules("/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py")
print("Done")
