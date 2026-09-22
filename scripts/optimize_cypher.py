import re

def optimize_cypher(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    # The block we want to replace
    old_block = """
                        MATCH (outbound:{label})
                        WHERE outbound.ACCOUNTNO = inbound.BENACCOUNTNO
                          AND ($session_id IS NULL OR outbound.session_id = $session_id)
                          AND outbound.BENACCOUNTNO IS NOT NULL
                          AND outbound.BENACCOUNTNO <> ''
                          AND outbound.BENACCOUNTNO <> inbound.ACCOUNTNO
                          AND coalesce(outbound.TRANSACTIONDATE, '') = coalesce(inbound.TRANSACTIONDATE, '')
                          AND coalesce(outbound.TRANSACTIONTIME, '') >= coalesce(inbound.TRANSACTIONTIME, '')
"""
    
    new_block = """
                        MATCH (outbound:{label} {ACCOUNTNO: inbound.BENACCOUNTNO, TRANSACTIONDATE: inbound.TRANSACTIONDATE})
                        WHERE ($session_id IS NULL OR outbound.session_id = $session_id)
                          AND outbound.BENACCOUNTNO IS NOT NULL
                          AND outbound.BENACCOUNTNO <> ''
                          AND outbound.BENACCOUNTNO <> inbound.ACCOUNTNO
                          AND outbound.TRANSACTIONTIME >= inbound.TRANSACTIONTIME
"""
    
    content = content.replace(old_block, new_block)
    
    # LA_rules_script has a slightly different block because of _session_scope_clause
    old_block_la = """
                MATCH (outbound:{label})
                WHERE outbound.ACCOUNTNO = inbound.BENACCOUNTNO
                  AND {_session_scope_clause("outbound")}
                  AND outbound.BENACCOUNTNO IS NOT NULL
                  AND outbound.BENACCOUNTNO <> ''
                  AND outbound.BENACCOUNTNO <> inbound.ACCOUNTNO
                  AND coalesce(outbound.TRANSACTIONDATE, '') = coalesce(inbound.TRANSACTIONDATE, '')
                  AND coalesce(outbound.TRANSACTIONTIME, '') >= coalesce(inbound.TRANSACTIONTIME, '')
"""
    new_block_la = """
                MATCH (outbound:{label} {ACCOUNTNO: inbound.BENACCOUNTNO, TRANSACTIONDATE: inbound.TRANSACTIONDATE})
                WHERE {_session_scope_clause("outbound")}
                  AND outbound.BENACCOUNTNO IS NOT NULL
                  AND outbound.BENACCOUNTNO <> ''
                  AND outbound.BENACCOUNTNO <> inbound.ACCOUNTNO
                  AND outbound.TRANSACTIONTIME >= inbound.TRANSACTIONTIME
"""
    content = content.replace(old_block_la, new_block_la)

    with open(file_path, "w") as f:
        f.write(content)

optimize_cypher("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
optimize_cypher("/var/www/linkx-backend/service_factory/services/linkx-worker/src/batch_manager/analyzing/LA_rules_script.py")
print("Done")
