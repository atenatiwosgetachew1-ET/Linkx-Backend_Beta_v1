from datetime import datetime, timedelta
from logger import log_writer
from textblob import TextBlob


def batch_graph_analysis_transactions(driver, log_file, high_risk_accounts=None, threshold_multiplier=3):
    if high_risk_accounts is None:
        high_risk_accounts = ['ACC6970109','ACC4659300','ACC8482897','ACC5960522','ACC2976147','ACC6802190']

    log_writer(log_file, f"[{datetime.now()}] [Info] Starting transactions analysis")

    with driver.session() as session:

        # ----------------------------
        # 1️⃣ SMURFING: consecutive transactions only
        # ----------------------------
        session.run("""
        MATCH (t:Transactions)
        WITH t.ACCOUNTNO AS acc, t
        ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
        WITH acc, collect(t) AS txns
        UNWIND range(0, size(txns)-2) AS i
        WITH txns[i] AS a, txns[i+1] AS b
        MERGE (a)-[:SMURFING {bgcolor:'#d5d276'}]->(b)
        """)

        # ----------------------------
        # 2️⃣ CIRCULAR_FLOW: 2-step cycles only
        # ----------------------------
        session.run("""
        MATCH (a:Transactions)-[:SMURFING]->(b:Transactions)-[:SMURFING]->(a)
        MERGE (a)-[:CIRCULAR_FLOW {bgcolor:'#e6e6e6'}]->(b)
        MERGE (b)-[:CIRCULAR_FLOW {bgcolor:'#e6e6e6'}]->(a)
        """)

        # ----------------------------
        # 3️⃣ FUND_FLOW: sequential next transactions
        # ----------------------------
        session.run("""
        MATCH (a:Transactions)-[:SMURFING]->(b:Transactions)
        MERGE (a)-[:FUND_FLOW {bgcolor:'#d8a822'}]->(b)
        """)

        # ----------------------------
        # 4️⃣ DORMANT_TO_ACTIVE
        # ----------------------------
        session.run("""
        MATCH (t:Transactions)
        WHERE t.ACCOUNTSTATE='dormant' AND t.BENACCOUNTSTATE='active'
        MERGE (t)-[:DORMANT_TO_ACTIVE {bgcolor:'#c20f0f'}]->(t)
        """)

        # ----------------------------
        # 5️⃣ HIGH_RISK_LINK: previous transaction only
        # ----------------------------
        session.run("""
        UNWIND $accounts AS acc
        MATCH (t:Transactions {ACCOUNTNO: acc})
        WITH acc, t
        ORDER BY t.TRANSACTIONDATE, t.TRANSACTIONTIME
        WITH acc, collect(t) AS txns
        UNWIND range(1, size(txns)-1) AS i
        WITH txns[i] AS current_txn, txns[i-1] AS prev_txn
        MERGE (current_txn)-[:HIGH_RISK_LINK {bgcolor:'#ba720d'}]->(prev_txn)
        """, accounts=high_risk_accounts)

        # ----------------------------
        # 6️⃣ ABNORMAL_BALANCE_CHANGE
        # ----------------------------
        session.run(f"""
        MATCH (t:Transactions)
        WITH t.ACCOUNTNO AS acc, t
        ORDER BY t.TRANSACTIONDATE DESC, t.TRANSACTIONTIME DESC
        WITH acc, collect(t) AS txns
        UNWIND range(0, size(txns)-1) AS i
        WITH txns[i] AS current, txns[max(0,i-10)..i] AS prev_txns
        WITH current, [tx IN prev_txns | abs(current.BALANCE - tx.BALANCE)] AS changes
        WITH current, reduce(s=0, c IN changes | s+c)/size(changes) AS avg_change
        WHERE size(changes) > 1 AND avg_change >= $threshold
        UNWIND prev_txns AS prev_tx
        MERGE (prev_tx)-[:ABNORMAL_BALANCE_CHANGE {bgcolor:'#196e08', change: abs(current.BALANCE - prev_tx.BALANCE)}]->(current)
        """, threshold=threshold_multiplier)

    log_writer(log_file, f"[{datetime.now()}] [Success] Transactions analysis completed")

# ====================================================
# Social Media Posts
# ====================================================

def batch_graph_analysis_posts(driver, log_file):
    log_writer(log_file, f"[{datetime.now()}] [Info] Starting social media analysis")

    with driver.session() as session:

        # ----------------------------
        # 1️⃣ Link users to their posts (one-to-many)
        # ----------------------------
        session.run("""
        MATCH (t:Tweet)
        MERGE (u:User {Username: t.USERNAME})
        MERGE (u)-[:CREATED {bgcolor:'#e6e6e6'}]->(t)
        """)

        # ----------------------------
        # 2️⃣ Low engagement cluster (threshold only)
        # ----------------------------
        session.run("""
        MATCH (t:Tweet)
        WHERE toInteger(t.LIKES) < 10 AND toInteger(t.RETWEETS) < 5
        MERGE (c:LowEngagementCluster {flag:'LOW_ENG'})
        MERGE (t)-[:LOW_ENGAGEMENT {bgcolor:'#e6e6e6'}]->(c)
        """)

        # ----------------------------
        # 3️⃣ Influencer network (already tagged, avoid duplicate links)
        # ----------------------------
        session.run("""
        MATCH (u:User)-[:IS_INFLUENCER]->(t:Tweet)
        MERGE (t)-[:INFLUENCER_POST {bgcolor:'#363636'}]->(u)
        """)

        # ----------------------------
        # 4️⃣ Negative sentiment cluster
        # ----------------------------
        session.run("""
        MATCH (t:Tweet)-[:HAS_SENTIMENT]->(s:Sentiment)
        WHERE s.Polarity < 0
        MERGE (c:NegativeSentiment {flag:'NEG_SENTIMENT'})
        MERGE (t)-[:NEGATIVE_CONTENT {bgcolor:'#dba124'}]->(c)
        """)

        # ----------------------------
        # 5️⃣ Suspicious cluster: low engagement + negative sentiment
        # ----------------------------
        session.run("""
        MATCH (t:Tweet)-[:LOW_ENGAGEMENT]->(:LowEngagementCluster),
              (t)-[:NEGATIVE_CONTENT]->(:NegativeSentiment)
        MERGE (sc:SuspiciousCluster {type:'LOW_ENG_NEG_SENT'})
        MERGE (t)-[:SUSPICIOUS_PATTERN {bgcolor:'#d5d276'}]->(sc)
        """)

        # ----------------------------
        # 6️⃣ Community: link users sharing negative posts (avoid full cartesian)
        # ----------------------------
        session.run("""
        MATCH (u1:User)-[:CREATED]->(t1:Tweet)-[:NEGATIVE_CONTENT]->()
        MATCH (u2:User)-[:CREATED]->(t2:Tweet)-[:NEGATIVE_CONTENT]->()
        WHERE u1.Username < u2.Username
        MERGE (u1)-[:SHARED_NEG_NET {bgcolor:'#d5d276'}]->(u2)
        """)
    log_writer(log_file, f"[{datetime.now()}] [Success] Social media analysis completed")
