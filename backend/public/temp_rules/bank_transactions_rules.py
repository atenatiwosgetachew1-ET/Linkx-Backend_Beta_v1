from datetime import datetime
from logger import log_writer

# ====================================================
# Bank transactions
# ====================================================

def main(driver, session_id, nodes_label, log_file, high_risk_accounts=None, threshold_multiplier=3):
    if high_risk_accounts is None:
        high_risk_accounts = [
            'ACC6970109', 'ACC4659300', 'ACC8482897',
            'ACC5960522', 'ACC2976147', 'ACC6802190'
        ]

    log_writer(log_file, f"[{datetime.now()}] [Info] Starting Transactions analysis")

    with driver.session() as session:

        # -------------------------------------------------
        # 1️⃣ SMURFING (consecutive transactions)
        # -------------------------------------------------
        session.run("""
            MATCH (t:Transactions)
            WHERE t.nodes_label = $nodes_label
            WITH t.ACCOUNTNO AS acc, t
            ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
            WITH acc, collect(t) AS txns
            UNWIND range(0, size(txns)-2) AS i
            WITH txns[i] AS a, txns[i+1] AS b
            WITH DISTINCT a, b
            MERGE (a)-[r:SMURFING {session_id: $session_id}]->(b)
            SET r.bgcolor = '#d5d276'
        """, nodes_label=nodes_label, session_id=session_id)

        # -------------------------------------------------
        # 2️⃣ CIRCULAR FLOW (2-step)
        # -------------------------------------------------
        session.run("""
            MATCH (a:Transactions)-[:SMURFING]->(b:Transactions)-[:SMURFING]->(a)
            WHERE a.nodes_label = $nodes_label
              AND b.nodes_label = $nodes_label
            WITH DISTINCT a, b
            MERGE (a)-[r1:CIRCULAR_FLOW {session_id: $session_id}]->(b)
            SET r1.bgcolor = '#e6e6e6'
            MERGE (b)-[r2:CIRCULAR_FLOW {session_id: $session_id}]->(a)
            SET r2.bgcolor = '#e6e6e6'
        """, nodes_label=nodes_label, session_id=session_id)

        # -------------------------------------------------
        # 3️⃣ FUND FLOW
        # -------------------------------------------------
        session.run("""
            MATCH (a:Transactions)-[:SMURFING]->(b:Transactions)
            WHERE a.nodes_label = $nodes_label
              AND b.nodes_label = $nodes_label
            WITH DISTINCT a, b
            MERGE (a)-[r:FUND_FLOW {session_id: $session_id}]->(b)
            SET r.bgcolor = '#d8a822'
        """, nodes_label=nodes_label, session_id=session_id)

        # -------------------------------------------------
        # 4️⃣ DORMANT → ACTIVE
        # -------------------------------------------------
        session.run("""
            MATCH (t:Transactions)
            WHERE t.nodes_label = $nodes_label
              AND t.ACCOUNTSTATE = 'dormant'
              AND t.BENACCOUNTSTATE = 'active'
            WITH DISTINCT t
            MERGE (t)-[r:DORMANT_TO_ACTIVE {session_id: $session_id}]->(t)
            SET r.bgcolor = '#c20f0f',r.textcolor = '#eeeeee'
        """, nodes_label=nodes_label, session_id=session_id)

        # -------------------------------------------------
        # 5️⃣ HIGH RISK LINK
        # -------------------------------------------------
        session.run("""
            UNWIND $accounts AS acc
            MATCH (t:Transactions {ACCOUNTNO: acc})
            WHERE t.nodes_label = $nodes_label
            WITH acc, t
            ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
            WITH acc, collect(t) AS txns
            UNWIND range(1, size(txns)-1) AS i
            WITH txns[i] AS current_txn, txns[i-1] AS prev_txn
            WITH DISTINCT current_txn, prev_txn
            MERGE (current_txn)-[r:HIGH_RISK_LINK {session_id: $session_id}]->(prev_txn)
            SET r.bgcolor = '#de7d07'
        """, nodes_label=nodes_label, accounts=high_risk_accounts, session_id=session_id)

        # -------------------------------------------------
        # 6️⃣ ABNORMAL BALANCE CHANGE
        # -------------------------------------------------
        session.run("""
            MATCH (t:Transactions)
            WHERE t.nodes_label = $nodes_label
            WITH t.ACCOUNTNO AS acc, t
            ORDER BY t.TRANSACTIONDATE DESC, t.TRANSACTIONTIME DESC
            WITH acc, collect(t) AS txns
            UNWIND range(0, size(txns)-1) AS i
            WITH txns[i] AS current,
                 txns[
                   CASE WHEN i - 10 < 0 THEN 0 ELSE i - 10 END
                   .. i
                 ] AS prev_txns
            WITH current,
                 [tx IN prev_txns 
                    WHERE tx.BALANCE IS NOT NULL AND toFloat(tx.BALANCE) IS NOT NULL |
                    abs(toFloat(current.BALANCE) - toFloat(tx.BALANCE))
                 ] AS changes,
                 prev_txns
            WITH current,
                 changes,
                 size(changes) AS change_count,
                 prev_txns
            WHERE change_count > 1
            WITH current,
                 reduce(s = 0, c IN changes | s + c) / change_count AS avg_change,
                 prev_txns
            WHERE avg_change >= $threshold

            UNWIND prev_txns AS prev_tx
            WITH DISTINCT prev_tx, current
            MERGE (prev_tx)-[r:ABNORMAL_BALANCE_CHANGE {session_id: $session_id}]->(current)
            SET r.bgcolor = '#196e08',r.textcolor = '#eeeeee',
                r.change = abs(current.BALANCE - prev_tx.BALANCE)
        """, nodes_label=nodes_label, threshold=threshold_multiplier, session_id=session_id)


    log_writer(log_file, f"[{datetime.now()}] [Success] Transactions analysis completed")
