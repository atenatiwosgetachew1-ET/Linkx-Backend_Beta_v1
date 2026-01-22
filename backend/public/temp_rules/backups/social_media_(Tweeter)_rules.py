from datetime import datetime
from logger import log_writer

# ====================================================
# Social Media Posts (Twitter)
# ====================================================

def main(driver, nodes_label, log_file):
    """
    nodes_label: The label of the nodes to analyze (e.g., 'Tweet')
    log_file: Path to the log file
    """

    log_writer(log_file, f"[{datetime.now()}] [Info] Starting social media analysis")

    with driver.session() as session:

        # -------------------------------------------------
        # 1️⃣ Link users to their posts (one-to-many)
        # -------------------------------------------------
        session.run("""
            MATCH (t:$nodes_label)
            MERGE (u:User {Username: t.USERNAME})
            MERGE (u)-[r:CREATED]->(t)
            SET r.bgcolor = '#e6e6e6'
        """, nodes_label=nodes_label)

        # -------------------------------------------------
        # 2️⃣ Low engagement cluster (threshold only)
        # -------------------------------------------------
        session.run("""
            MATCH (t:$nodes_label)
            WHERE toInteger(t.LIKES) < 10 AND toInteger(t.RETWEETS) < 5
            MERGE (c:LowEngagementCluster {flag:'LOW_ENG'})
            MERGE (t)-[r:LOW_ENGAGEMENT]->(c)
            SET r.bgcolor = '#e6e6e6'
        """, nodes_label=nodes_label)

        # -------------------------------------------------
        # 3️⃣ Influencer network (already tagged, avoid duplicate links)
        # -------------------------------------------------
        session.run("""
            MATCH (u:User)-[:IS_INFLUENCER]->(t:$nodes_label)
            MERGE (t)-[r:INFLUENCER_POST]->(u)
            SET r.bgcolor = '#363636'
        """, nodes_label=nodes_label)

        # -------------------------------------------------
        # 4️⃣ Negative sentiment cluster
        # -------------------------------------------------
        session.run("""
            MATCH (t:$nodes_label)-[:HAS_SENTIMENT]->(s:Sentiment)
            WHERE s.Polarity < 0
            MERGE (c:NegativeSentiment {flag:'NEG_SENTIMENT'})
            MERGE (t)-[r:NEGATIVE_CONTENT]->(c)
            SET r.bgcolor = '#dba124'
        """, nodes_label=nodes_label)

        # -------------------------------------------------
        # 5️⃣ Suspicious cluster: low engagement + negative sentiment
        # -------------------------------------------------
        session.run("""
            MATCH (t:$nodes_label)-[:LOW_ENGAGEMENT]->(:LowEngagementCluster),
                  (t)-[:NEGATIVE_CONTENT]->(:NegativeSentiment)
            MERGE (sc:SuspiciousCluster {type:'LOW_ENG_NEG_SENT'})
            MERGE (t)-[r:SUSPICIOUS_PATTERN]->(sc)
            SET r.bgcolor = '#d5d276'
        """, nodes_label=nodes_label)

        # -------------------------------------------------
        # 6️⃣ Community: link users sharing negative posts (avoid full cartesian)
        # -------------------------------------------------
        session.run("""
            MATCH (u1:User)-[:CREATED]->(t1:$nodes_label)-[:NEGATIVE_CONTENT]->()
            MATCH (u2:User)-[:CREATED]->(t2:$nodes_label)-[:NEGATIVE_CONTENT]->()
            WHERE u1.Username < u2.Username
            MERGE (u1)-[r:SHARED_NEG_NET]->(u2)
            SET r.bgcolor = '#d5d276'
        """, nodes_label=nodes_label)

    log_writer(log_file, f"[{datetime.now()}] [Success] Social media analysis completed")
