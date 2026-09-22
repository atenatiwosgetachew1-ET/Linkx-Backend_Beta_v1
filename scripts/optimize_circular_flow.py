import re

def optimize_circular_flow(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # In xvigilance_consumer.py, CIRCULAR_FLOW:
    #                 MATCH (a:{label} {{ACCOUNTNO: acc}})
    #                 WHERE ($session_id IS NULL OR a.session_id = $session_id)
    #                   AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> ''
    
    old_block_consumer = """
                MATCH (a:{label} {ACCOUNTNO: acc})
                WHERE ($session_id IS NULL OR a.session_id = $session_id)
                  AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> ''
"""
    new_block_consumer = """
                MATCH (a:{label} {ACCOUNTNO: acc})
                WHERE ($session_id IS NULL OR a.session_id = $session_id)
                  AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> ''
                  AND NOT a.LOGICAL_BENACCOUNTNO IN $pass_through_accounts
"""
    content = content.replace(old_block_consumer.strip(), new_block_consumer.strip())
    
    # Also in LA_rules_script:
    old_block_la = """
                MATCH (a:{label} {ACCOUNTNO: acc})
                WHERE {_session_scope_clause('a')}
                  AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> ''
"""
    new_block_la = """
                MATCH (a:{label} {ACCOUNTNO: acc})
                WHERE {_session_scope_clause('a')}
                  AND a.LOGICAL_BENACCOUNTNO IS NOT NULL AND a.LOGICAL_BENACCOUNTNO <> ''
                  AND NOT a.LOGICAL_BENACCOUNTNO IN $pt
"""
    content = content.replace(old_block_la.strip(), new_block_la.strip())
    
    with open(file_path, "w") as f:
        f.write(content)

optimize_circular_flow("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
optimize_circular_flow("/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py")
print("Done")
