import re

def optimize_file(file_path):
    with open(file_path, "r") as f:
        content = f.read()

    optimized_query = """
                    MATCH (inbound:{label})
                    WHERE ($session_id IS NULL OR inbound.session_id = $session_id)
                      AND inbound.BENACCOUNTNO IN $pass_through_accounts
                      AND inbound.ACCOUNTNO IS NOT NULL AND inbound.ACCOUNTNO <> ''

                    CALL {
                        WITH inbound
                        MATCH (outbound:{label})
                        WHERE outbound.ACCOUNTNO = inbound.BENACCOUNTNO
                          AND ($session_id IS NULL OR outbound.session_id = $session_id)
                          AND outbound.BENACCOUNTNO IS NOT NULL
                          AND outbound.BENACCOUNTNO <> ''
                          AND outbound.BENACCOUNTNO <> inbound.ACCOUNTNO
                          AND coalesce(outbound.TRANSACTIONDATE, '') = coalesce(inbound.TRANSACTIONDATE, '')
                          AND coalesce(outbound.TRANSACTIONTIME, '') >= coalesce(inbound.TRANSACTIONTIME, '')
                        WITH inbound, outbound,
                             coalesce(toFloat(inbound.AMOUNTINBIRR), toFloat(inbound.AMOUNT),
                                      toFloat(inbound.amount), toFloat(inbound.LOCAL_AMOUNT), 0.0) AS in_amt,
                             coalesce(toFloat(outbound.AMOUNTINBIRR), toFloat(outbound.AMOUNT),
                                      toFloat(outbound.amount), toFloat(outbound.LOCAL_AMOUNT), 0.0) AS out_amt
                        WHERE in_amt > 0 AND out_amt > 0
                          AND abs(out_amt - in_amt) <= (in_amt * 0.1)
                        RETURN outbound, in_amt, out_amt
                        ORDER BY outbound.TRANSACTIONTIME ASC
                        LIMIT 1
                    }

                    MERGE (inbound)-[r:EFFECTIVE_FLOW {{session_id:$session_id}}]->(outbound)
    """

    pattern = re.compile(r"MATCH \(inbound:\{label\}\).*?MERGE \(inbound\)-\[r:EFFECTIVE_FLOW \{\{session_id:\$session_id\}\}\]->\(outbound\)", re.DOTALL)
    
    new_content = pattern.sub(optimized_query.strip(), content)
    
    with open(file_path, "w") as f:
        f.write(new_content)

optimize_file("/var/www/linkx-backend/service_factory/services/linkx-worker/src/linkx_worker/xvigilance_consumer.py")
print("Done")
